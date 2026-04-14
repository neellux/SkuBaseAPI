import asyncio
import base64
import logging
import os
from io import BytesIO
from typing import Any, Dict, List

import httpx
import litellm
import orjson
from config import config
from google import genai
from google.genai import types
from models.api_models import FieldDefinition
from PIL import Image
from services.sellercloud_service import sellercloud_service

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"
logger = logging.getLogger(__name__)


try:
    AI_CONFIG = config.get("ai", {})
    ASPECTS_MODEL = AI_CONFIG.get("aspects_model")
    ASPECTS_API_KEY = AI_CONFIG.get("aspects_api_key")
    CAPTION_MODEL = AI_CONFIG.get("caption_model")
    CAPTION_API_KEY = AI_CONFIG.get("caption_api_key")
except Exception as e:
    logger.error(f"Error loading config.toml: {e}")
    ASPECTS_MODEL = None
    ASPECTS_API_KEY = None
    CAPTION_MODEL = None
    CAPTION_API_KEY = None


def _load_prompt(file_path: str) -> str:
    try:
        with open(file_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {file_path}")
        return ""


SYSTEM_PROMPT = _load_prompt("./utils/prompts/aspects_system_prompt.txt")
USER_PROMPT_TEMPLATE = _load_prompt("./utils/prompts/aspects_prompt.txt")
CAPTION_SYSTEM_PROMPT = _load_prompt("./utils/prompts/caption_system_prompt.txt")
CAPTION_USER_PROMPT_TEMPLATE = _load_prompt("./utils/prompts/caption_prompt.txt")

MAX_IMAGE_SIDE = 1024
MAX_IMAGE_SIZE_MB = 5
MAX_IMAGES_TO_SEND = 8


def _convert_hyphens_to_html_list(text: str) -> str:
    if not text:
        return text

    lines = text.split("\n")
    result = []
    in_list = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("- "):
            if not in_list:
                result.append("<ul>")
                in_list = True

            content = stripped[2:].strip().capitalize()
            result.append(f"<li><p>{content}</p></li>")
        else:
            if in_list:
                result.append("</ul>")
                in_list = False

            if stripped:
                result.append(stripped.capitalize())

    if in_list:
        result.append("</ul>")

    return "".join(result)


async def _process_image_url(url: str, client: httpx.AsyncClient) -> str | None:
    try:
        response = await client.get(url, timeout=30.0)
        response.raise_for_status()
        image_data = response.content

        with Image.open(BytesIO(image_data)) as img:
            img.thumbnail((MAX_IMAGE_SIDE, MAX_IMAGE_SIDE))

            output_buffer = BytesIO()
            quality = 95
            img.save(output_buffer, format="JPEG", quality=quality)
            while output_buffer.tell() > MAX_IMAGE_SIZE_MB * 1024 * 1024 and quality > 10:
                output_buffer = BytesIO()
                quality -= 5
                img.save(output_buffer, format="JPEG", quality=quality)

            if output_buffer.tell() > MAX_IMAGE_SIZE_MB * 1024 * 1024:
                logger.warning(f"Image {url} is still too large after compression, skipping.")
                return None

            encoded_string = base64.b64encode(output_buffer.getvalue()).decode("utf-8")
            return f"data:image/jpeg;base64,{encoded_string}"

    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching image {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing image {url}: {e}")
        return None


async def _call_google_genai(
    system_prompt: str,
    user_prompt: str,
    model_name: str,
    api_key: str,
    image_data: List[str] = None,
    image_urls: List[str] = None,
    temperature: float = 1,
    max_output_tokens: int = 8192,
) -> str:
    try:
        num_images = len(image_data or []) + len(image_urls or [])
        logger.debug(f"Making API call to Google GenAI with {num_images} images")

        parts = [types.Part.from_text(text=user_prompt)]

        if image_data:
            for base64_image in image_data:
                parts.append(
                    types.Part.from_bytes(
                        data=base64.b64decode(base64_image), mime_type="image/jpeg"
                    )
                )

        if image_urls:
            for url in image_urls:
                try:
                    parts.append(types.Part.from_uri(file_uri=url, mime_type="image/jpeg"))
                except Exception as e:
                    logger.warning(f"Failed to process image {url}: {e}")

        contents = [
            types.Content(role="user", parts=parts),
        ]

        generate_content_config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=temperature,
            top_p=0.95,
            max_output_tokens=max_output_tokens,
            safety_settings=[
                types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF"),
            ],
            thinking_config=types.ThinkingConfig(
                thinking_budget=0,
            ),
        )

        async with genai.Client(
            vertexai=True,
            project="433271307736",
            location="us-central1",
        ).aio as aclient:
            response = await aclient.models.generate_content(
                model=model_name.split("vertex_ai/")[-1],
                contents=contents,
                config=generate_content_config,
            )

            response_text = response.text

            if response_text:
                logger.debug(f"Successfully generated response, length: {len(response_text)}")
                return response_text
            else:
                raise Exception("No response generated from Google GenAI.")

    except Exception as e:
        logger.error(f"Error in Google GenAI API call: {e}")
        raise


class AIService:
    @staticmethod
    async def _generate_ai_description(
        product_data: Dict[str, Any],
        image_urls: List[str],
        product_name: str,
        product_type: str,
        use_raw_image_urls: bool = True,
    ) -> str | None:
        if not CAPTION_MODEL or not CAPTION_API_KEY:
            logger.warning("AI description generation is not fully configured. Skipping.")
            return None

        caption_user_prompt = CAPTION_USER_PROMPT_TEMPLATE

        user_content = [{"type": "text", "text": caption_user_prompt}]
        if image_urls:
            if use_raw_image_urls:
                for url in image_urls:
                    payload = (
                        {
                            "type": "image_url",
                            "image_url": {"url": url},
                        }
                        if "openai" not in CAPTION_MODEL
                        else {
                            "type": "image_url",
                            "image_url": {"url": url, "detail": "high"},
                        }
                    )
                    user_content.append(payload)
            else:
                async with httpx.AsyncClient() as client:
                    tasks = [_process_image_url(url, client) for url in image_urls]
                    processed_images = await asyncio.gather(*tasks)

                for base64_image in processed_images:
                    if base64_image:
                        payload = (
                            {
                                "type": "image_url",
                                "image_url": {"url": base64_image},
                            }
                            if "openai" not in CAPTION_MODEL
                            else {
                                "type": "image_url",
                                "image_url": {"url": base64_image, "detail": "high"},
                            }
                        )
                        user_content.append(payload)

        messages = [
            {"role": "system", "content": CAPTION_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

        try:
            logger.info(f"Calling Caption AI model {CAPTION_MODEL} for product {product_name}")

            if CAPTION_MODEL and CAPTION_MODEL.startswith("vertex"):
                direct_image_urls = None
                base64_images = None

                if image_urls:
                    if use_raw_image_urls:
                        direct_image_urls = image_urls
                    else:
                        base64_images = []
                        for item in user_content:
                            if isinstance(item, dict) and item.get("type") == "image_url":
                                image_url = item.get("image_url", {}).get("url", "")
                                if image_url.startswith("data:image/jpeg;base64,"):
                                    base64_data = image_url.replace("data:image/jpeg;base64,", "")
                                    base64_images.append(base64_data)

                description = await _call_google_genai(
                    system_prompt=CAPTION_SYSTEM_PROMPT,
                    user_prompt=caption_user_prompt,
                    model_name=CAPTION_MODEL,
                    api_key=CAPTION_API_KEY,
                    image_urls=direct_image_urls,
                    image_data=base64_images,
                    temperature=0,
                    max_output_tokens=3000,
                )
            else:
                response = await litellm.acompletion(
                    model=CAPTION_MODEL,
                    messages=messages,
                    api_key=CAPTION_API_KEY,
                    reasoning_effort="low",
                    max_tokens=3000,
                )
                description = response.choices[0].message.content

            logger.debug(f"Raw Caption AI response: {description}")
            logger.info(f"Successfully received AI description for {product_name}")

            description = _convert_hyphens_to_html_list(description)
            return description

        except Exception as e:
            logger.error(f"Error calling Caption AI model: {e}")
            return None

    @staticmethod
    async def _get_ai_aspects_task(
        product_data: Dict[str, Any],
        fields_to_fill: List[FieldDefinition],
        image_urls: List[str],
        mapped_options: Dict[str, List[Any]] = None,
        use_raw_image_urls: bool = True,
    ) -> Dict[str, Any]:
        if not ASPECTS_MODEL or not ASPECTS_API_KEY:
            logger.warning("AI aspects service is not configured. Skipping.")
            return {}

        if not fields_to_fill:
            logger.info("No fields to fill with AI aspects. Skipping.")
            return {}

        if mapped_options is None:
            mapped_options = {}

        product_name = product_data.get("ProductName", product_data.get("ID", "Unknown Product"))

        category_aspect_data = [
            {
                "aspectName": field.name,
                "aspectType": field.type,
                "aspectOptions": (
                    mapped_options.get(field.name, field.options)
                    if mapped_options.get(field.name, field.options)
                    else None
                ),
                "itemToAspectCardinality": "MULTI" if field.multiselect else "SINGLE",
            }
            for field in fields_to_fill
        ]

        product_type = product_data.get("ProductType", "Unknown Type")
        user_prompt = USER_PROMPT_TEMPLATE.replace("{{product_name}}", str(product_name))
        user_prompt = user_prompt.replace("{{product_type}}", str(product_type))
        user_prompt = user_prompt.replace(
            "{{category_aspect_data}}",
            orjson.dumps(category_aspect_data, option=orjson.OPT_INDENT_2).decode(),
        )

        user_content = [{"type": "text", "text": user_prompt}]
        if image_urls:
            if use_raw_image_urls:
                for url in image_urls:
                    payload = (
                        {
                            "type": "image_url",
                            "image_url": {"url": url},
                        }
                        if "openai" not in ASPECTS_MODEL
                        else {
                            "type": "image_url",
                            "image_url": {"url": url, "detail": "high"},
                        }
                    )
                    user_content.append(payload)
            else:
                async with httpx.AsyncClient() as client:
                    tasks = [_process_image_url(url, client) for url in image_urls]
                    processed_images = await asyncio.gather(*tasks)

                for base64_image in processed_images:
                    if base64_image:
                        payload = (
                            {
                                "type": "image_url",
                                "image_url": {"url": base64_image},
                            }
                            if "openai" not in ASPECTS_MODEL
                            else {
                                "type": "image_url",
                                "image_url": {"url": base64_image, "detail": "high"},
                            }
                        )
                        user_content.append(payload)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

        try:
            logger.info(f"Calling Aspects AI model {ASPECTS_MODEL} for product {product_name}")

            if ASPECTS_MODEL and ASPECTS_MODEL.startswith("vertex"):
                direct_image_urls = None
                base64_images = None

                if image_urls:
                    if use_raw_image_urls:
                        direct_image_urls = image_urls
                    else:
                        base64_images = []
                        for item in user_content:
                            if isinstance(item, dict) and item.get("type") == "image_url":
                                image_url = item.get("image_url", {}).get("url", "")
                                if image_url.startswith("data:image/jpeg;base64,"):
                                    base64_data = image_url.replace("data:image/jpeg;base64,", "")
                                    base64_images.append(base64_data)

                ai_response_content = await _call_google_genai(
                    system_prompt=SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    model_name=ASPECTS_MODEL,
                    api_key=ASPECTS_API_KEY,
                    image_urls=direct_image_urls,
                    image_data=base64_images,
                    temperature=0,
                    max_output_tokens=8192,
                )
            else:
                response = await litellm.acompletion(
                    model=ASPECTS_MODEL,
                    messages=messages,
                    api_key=ASPECTS_API_KEY,
                    reasoning_effort="low",
                )

                ai_response_content = response.choices[0].message.content

            if ai_response_content.strip().startswith("```json"):
                ai_response_content = ai_response_content.strip()[7:-3].strip()

            ai_data = orjson.loads(ai_response_content)
            logger.info(f"Successfully received and parsed AI aspects for product {product_name}")
            return ai_data
        except Exception as e:
            logger.error(f"Error in AI aspects generation: {e}")
            return {}

    @staticmethod
    async def generate_ai_content(
        product_data: Dict[str, Any],
        fields_to_fill: List[FieldDefinition],
        mapped_options: Dict[str, List[Any]] = None,
        use_raw_image_urls: bool = True,
    ) -> Dict[str, Any]:
        if mapped_options is None:
            mapped_options = {}

        product_name = product_data.get("ProductName", product_data.get("ID", "Unknown Product"))
        product_type = product_data.get("ProductType", "Unknown Type")

        description_field = next(
            (f for f in fields_to_fill if f.name == "description" and f.ai_tagging),
            None,
        )

        tasks = []

        product_id = product_data.get("ID")
        image_urls = []
        if product_id:
            try:
                all_images = await sellercloud_service.get_product_images(product_id)
                image_urls = [url for url in all_images if "washtag_" not in url][
                    :MAX_IMAGES_TO_SEND
                ]
            except Exception as e:
                logger.error(f"Error fetching images from SellerCloud for {product_id}: {e}")

        aspect_fields_to_fill = [f for f in fields_to_fill if f.name != "description"]

        if aspect_fields_to_fill:
            tasks.append(
                AIService._get_ai_aspects_task(
                    product_data,
                    aspect_fields_to_fill,
                    image_urls,
                    mapped_options,
                    use_raw_image_urls,
                )
            )
        else:
            tasks.append(asyncio.sleep(0, result={}))

        if description_field:
            logger.info(f"Calling Description AI model {CAPTION_MODEL} for product {product_name}")
            tasks.append(
                AIService._generate_ai_description(
                    product_data,
                    image_urls,
                    str(product_name),
                    str(product_type),
                    use_raw_image_urls,
                )
            )
        else:
            tasks.append(asyncio.sleep(0, result=None))

        results = await asyncio.gather(*tasks)

        ai_aspects = results[0] if len(results) > 0 else {}
        ai_description = results[1] if len(results) > 1 else None

        return {"aspects": ai_aspects, "description": ai_description}
