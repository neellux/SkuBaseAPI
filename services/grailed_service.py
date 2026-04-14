import logging
import re
import traceback
from typing import Any, Dict, List

import httpx
from config import config
from fastapi import HTTPException
from tortoise import Tortoise
from models.db_models import AppSettings, Listing, ListingSubmission
from services.listing_options_service import listing_options_service

logger = logging.getLogger(__name__)

COUNTRY_CODE_MAP = country_codes = {
    "Albania": {"iso2": "AL", "iso3": "ALB"},
    "Algeria": {"iso2": "DZ", "iso3": "DZA"},
    "Andorra": {"iso2": "AD", "iso3": "AND"},
    "Angola": {"iso2": "AO", "iso3": "AGO"},
    "Anguilla": {"iso2": "AI", "iso3": "AIA"},
    "Antigua & Barbuda": {"iso2": "AG", "iso3": "ATG"},
    "Argentina": {"iso2": "AR", "iso3": "ARG"},
    "Armenia": {"iso2": "AM", "iso3": "ARM"},
    "Aruba": {"iso2": "AW", "iso3": "ABW"},
    "Australia": {"iso2": "AU", "iso3": "AUS"},
    "Austria": {"iso2": "AT", "iso3": "AUT"},
    "Azerbaijan": {"iso2": "AZ", "iso3": "AZE"},
    "Bahamas": {"iso2": "BS", "iso3": "BHS"},
    "Bahrain": {"iso2": "BH", "iso3": "BHR"},
    "Bangladesh": {"iso2": "BD", "iso3": "BGD"},
    "Barbados": {"iso2": "BB", "iso3": "BRB"},
    "Belarus": {"iso2": "BY", "iso3": "BLR"},
    "Belgium": {"iso2": "BE", "iso3": "BEL"},
    "Belize": {"iso2": "BZ", "iso3": "BLZ"},
    "Benin": {"iso2": "BJ", "iso3": "BEN"},
    "Bermuda": {"iso2": "BM", "iso3": "BMU"},
    "Bhutan": {"iso2": "BT", "iso3": "BTN"},
    "Bolivia": {"iso2": "BO", "iso3": "BOL"},
    "Bosnia & Herzegovina": {"iso2": "BA", "iso3": "BIH"},
    "Botswana": {"iso2": "BW", "iso3": "BWA"},
    "Brazil": {"iso2": "BR", "iso3": "BRA"},
    "British Virgin Islands": {"iso2": "VG", "iso3": "VGB"},
    "Brunei": {"iso2": "BN", "iso3": "BRN"},
    "Bulgaria": {"iso2": "BG", "iso3": "BGR"},
    "Burkina Faso": {"iso2": "BF", "iso3": "BFA"},
    "Burundi": {"iso2": "BI", "iso3": "BDI"},
    "Cambodia": {"iso2": "KH", "iso3": "KHM"},
    "Cameroon": {"iso2": "CM", "iso3": "CMR"},
    "Canada": {"iso2": "CA", "iso3": "CAN"},
    "Cape Verde": {"iso2": "CV", "iso3": "CPV"},
    "Cayman Islands": {"iso2": "KY", "iso3": "CYM"},
    "Central African Republic": {"iso2": "CF", "iso3": "CAF"},
    "Chad": {"iso2": "TD", "iso3": "TCD"},
    "Chile": {"iso2": "CL", "iso3": "CHL"},
    "China": {"iso2": "CN", "iso3": "CHN"},
    "Colombia": {"iso2": "CO", "iso3": "COL"},
    "Comoros": {"iso2": "KM", "iso3": "COM"},
    "Congo - Brazzaville": {"iso2": "CG", "iso3": "COG"},
    "Congo - Kinshasa": {"iso2": "CD", "iso3": "COD"},
    "Cook Islands": {"iso2": "CK", "iso3": "COK"},
    "Costa Rica": {"iso2": "CR", "iso3": "CRI"},
    "Côte d'Ivoire": {"iso2": "CI", "iso3": "CIV"},
    "Croatia": {"iso2": "HR", "iso3": "HRV"},
    "Cyprus": {"iso2": "CY", "iso3": "CYP"},
    "Czech Republic": {"iso2": "CZ", "iso3": "CZE"},
    "Denmark": {"iso2": "DK", "iso3": "DNK"},
    "Djibouti": {"iso2": "DJ", "iso3": "DJI"},
    "Dominica": {"iso2": "DM", "iso3": "DMA"},
    "Dominican Republic": {"iso2": "DO", "iso3": "DOM"},
    "Ecuador": {"iso2": "EC", "iso3": "ECU"},
    "Egypt": {"iso2": "EG", "iso3": "EGY"},
    "El Salvador": {"iso2": "SV", "iso3": "SLV"},
    "Eritrea": {"iso2": "ER", "iso3": "ERI"},
    "Estonia": {"iso2": "EE", "iso3": "EST"},
    "Ethiopia": {"iso2": "ET", "iso3": "ETH"},
    "Falkland Islands": {"iso2": "FK", "iso3": "FLK"},
    "Faroe Islands": {"iso2": "FO", "iso3": "FRO"},
    "Fiji": {"iso2": "FJ", "iso3": "FJI"},
    "Finland": {"iso2": "FI", "iso3": "FIN"},
    "France": {"iso2": "FR", "iso3": "FRA"},
    "French Guiana": {"iso2": "GF", "iso3": "GUF"},
    "French Polynesia": {"iso2": "PF", "iso3": "PYF"},
    "Gabon": {"iso2": "GA", "iso3": "GAB"},
    "Gambia": {"iso2": "GM", "iso3": "GMB"},
    "Georgia": {"iso2": "GE", "iso3": "GEO"},
    "Germany": {"iso2": "DE", "iso3": "DEU"},
    "Gibraltar": {"iso2": "GI", "iso3": "GIB"},
    "Greece": {"iso2": "GR", "iso3": "GRC"},
    "Greenland": {"iso2": "GL", "iso3": "GRL"},
    "Grenada": {"iso2": "GD", "iso3": "GRD"},
    "Guadeloupe": {"iso2": "GP", "iso3": "GLP"},
    "Guatemala": {"iso2": "GT", "iso3": "GTM"},
    "Guinea": {"iso2": "GN", "iso3": "GIN"},
    "Guinea-Bissau": {"iso2": "GW", "iso3": "GNB"},
    "Guyana": {"iso2": "GY", "iso3": "GUY"},
    "Haiti": {"iso2": "HT", "iso3": "HTI"},
    "Honduras": {"iso2": "HN", "iso3": "HND"},
    "Hong Kong": {"iso2": "HK", "iso3": "HKG"},
    "Hungary": {"iso2": "HU", "iso3": "HUN"},
    "Iceland": {"iso2": "IS", "iso3": "ISL"},
    "India": {"iso2": "IN", "iso3": "IND"},
    "Indonesia": {"iso2": "ID", "iso3": "IDN"},
    "Ireland": {"iso2": "IE", "iso3": "IRL"},
    "Israel": {"iso2": "IL", "iso3": "ISR"},
    "Italy": {"iso2": "IT", "iso3": "ITA"},
    "Jamaica": {"iso2": "JM", "iso3": "JAM"},
    "Japan": {"iso2": "JP", "iso3": "JPN"},
    "Jordan": {"iso2": "JO", "iso3": "JOR"},
    "Kazakhstan": {"iso2": "KZ", "iso3": "KAZ"},
    "Kenya": {"iso2": "KE", "iso3": "KEN"},
    "Kiribati": {"iso2": "KI", "iso3": "KIR"},
    "Kuwait": {"iso2": "KW", "iso3": "KWT"},
    "Kyrgyzstan": {"iso2": "KG", "iso3": "KGZ"},
    "Laos": {"iso2": "LA", "iso3": "LAO"},
    "Latvia": {"iso2": "LV", "iso3": "LVA"},
    "Lebanon": {"iso2": "LB", "iso3": "LBN"},
    "Lesotho": {"iso2": "LS", "iso3": "LSO"},
    "Liberia": {"iso2": "LR", "iso3": "LBR"},
    "Libya": {"iso2": "LY", "iso3": "LBY"},
    "Liechtenstein": {"iso2": "LI", "iso3": "LIE"},
    "Lithuania": {"iso2": "LT", "iso3": "LTU"},
    "Luxembourg": {"iso2": "LU", "iso3": "LUX"},
    "Macau": {"iso2": "MO", "iso3": "MAC"},
    "Madagascar": {"iso2": "MG", "iso3": "MDG"},
    "Malawi": {"iso2": "MW", "iso3": "MWI"},
    "Malaysia": {"iso2": "MY", "iso3": "MYS"},
    "Maldives": {"iso2": "MV", "iso3": "MDV"},
    "Mali": {"iso2": "ML", "iso3": "MLI"},
    "Malta": {"iso2": "MT", "iso3": "MLT"},
    "Marshall Islands": {"iso2": "MH", "iso3": "MHL"},
    "Martinique": {"iso2": "MQ", "iso3": "MTQ"},
    "Mauritania": {"iso2": "MR", "iso3": "MRT"},
    "Mauritius": {"iso2": "MU", "iso3": "MUS"},
    "Mayotte": {"iso2": "YT", "iso3": "MYT"},
    "Mexico": {"iso2": "MX", "iso3": "MEX"},
    "Micronesia": {"iso2": "FM", "iso3": "FSM"},
    "Moldova": {"iso2": "MD", "iso3": "MDA"},
    "Monaco": {"iso2": "MC", "iso3": "MCO"},
    "Mongolia": {"iso2": "MN", "iso3": "MNG"},
    "Montenegro": {"iso2": "ME", "iso3": "MNE"},
    "Montserrat": {"iso2": "MS", "iso3": "MSR"},
    "Morocco": {"iso2": "MA", "iso3": "MAR"},
    "Mozambique": {"iso2": "MZ", "iso3": "MOZ"},
    "Namibia": {"iso2": "NA", "iso3": "NAM"},
    "Nauru": {"iso2": "NR", "iso3": "NRU"},
    "Nepal": {"iso2": "NP", "iso3": "NPL"},
    "Netherlands": {"iso2": "NL", "iso3": "NLD"},
    "Netherlands Antilles": {"iso2": "AN", "iso3": "ANT"},
    "New Caledonia": {"iso2": "NC", "iso3": "NCL"},
    "New Zealand": {"iso2": "NZ", "iso3": "NZL"},
    "Nicaragua": {"iso2": "NI", "iso3": "NIC"},
    "Niger": {"iso2": "NE", "iso3": "NER"},
    "Nigeria": {"iso2": "NG", "iso3": "NGA"},
    "Niue": {"iso2": "NU", "iso3": "NIU"},
    "Norfolk Island": {"iso2": "NF", "iso3": "NFK"},
    "North Macedonia": {"iso2": "MK", "iso3": "MKD"},
    "Norway": {"iso2": "NO", "iso3": "NOR"},
    "Oman": {"iso2": "OM", "iso3": "OMN"},
    "Pakistan": {"iso2": "PK", "iso3": "PAK"},
    "Palau": {"iso2": "PW", "iso3": "PLW"},
    "Panama": {"iso2": "PA", "iso3": "PAN"},
    "Papua New Guinea": {"iso2": "PG", "iso3": "PNG"},
    "Paraguay": {"iso2": "PY", "iso3": "PRY"},
    "Peru": {"iso2": "PE", "iso3": "PER"},
    "Philippines": {"iso2": "PH", "iso3": "PHL"},
    "Pitcairn Islands": {"iso2": "PN", "iso3": "PCN"},
    "Poland": {"iso2": "PL", "iso3": "POL"},
    "Portugal": {"iso2": "PT", "iso3": "PRT"},
    "Qatar": {"iso2": "QA", "iso3": "QAT"},
    "Réunion": {"iso2": "RE", "iso3": "REU"},
    "Romania": {"iso2": "RO", "iso3": "ROU"},
    "Russia": {"iso2": "RU", "iso3": "RUS"},
    "Rwanda": {"iso2": "RW", "iso3": "RWA"},
    "Samoa": {"iso2": "WS", "iso3": "WSM"},
    "San Marino": {"iso2": "SM", "iso3": "SMR"},
    "São Tomé & Príncipe": {"iso2": "ST", "iso3": "STP"},
    "Saudi Arabia": {"iso2": "SA", "iso3": "SAU"},
    "Senegal": {"iso2": "SN", "iso3": "SEN"},
    "Serbia": {"iso2": "RS", "iso3": "SRB"},
    "Seychelles": {"iso2": "SC", "iso3": "SYC"},
    "Sierra Leone": {"iso2": "SL", "iso3": "SLE"},
    "Singapore": {"iso2": "SG", "iso3": "SGP"},
    "Slovakia": {"iso2": "SK", "iso3": "SVK"},
    "Slovenia": {"iso2": "SI", "iso3": "SVN"},
    "Solomon Islands": {"iso2": "SB", "iso3": "SLB"},
    "Somalia": {"iso2": "SO", "iso3": "SOM"},
    "South Africa": {"iso2": "ZA", "iso3": "ZAF"},
    "South Korea": {"iso2": "KR", "iso3": "KOR"},
    "Spain": {"iso2": "ES", "iso3": "ESP"},
    "Sri Lanka": {"iso2": "LK", "iso3": "LKA"},
    "St. Helena": {"iso2": "SH", "iso3": "SHN"},
    "St. Kitts & Nevis": {"iso2": "KN", "iso3": "KNA"},
    "St. Lucia": {"iso2": "LC", "iso3": "LCA"},
    "St. Pierre & Miquelon": {"iso2": "PM", "iso3": "SPM"},
    "St. Vincent & Grenadines": {"iso2": "VC", "iso3": "VCT"},
    "Suriname": {"iso2": "SR", "iso3": "SUR"},
    "Svalbard & Jan Mayen": {"iso2": "SJ", "iso3": "SJM"},
    "Swaziland": {"iso2": "SZ", "iso3": "SWZ"},
    "Sweden": {"iso2": "SE", "iso3": "SWE"},
    "Switzerland": {"iso2": "CH", "iso3": "CHE"},
    "Taiwan": {"iso2": "TW", "iso3": "TWN"},
    "Tajikistan": {"iso2": "TJ", "iso3": "TJK"},
    "Tanzania": {"iso2": "TZ", "iso3": "TZA"},
    "Thailand": {"iso2": "TH", "iso3": "THA"},
    "Togo": {"iso2": "TG", "iso3": "TGO"},
    "Tonga": {"iso2": "TO", "iso3": "TON"},
    "Trinidad & Tobago": {"iso2": "TT", "iso3": "TTO"},
    "Tunisia": {"iso2": "TN", "iso3": "TUN"},
    "Turkey": {"iso2": "TR", "iso3": "TUR"},
    "Turkmenistan": {"iso2": "TM", "iso3": "TKM"},
    "Turks & Caicos Islands": {"iso2": "TC", "iso3": "TCA"},
    "Tuvalu": {"iso2": "TV", "iso3": "TUV"},
    "Uganda": {"iso2": "UG", "iso3": "UGA"},
    "Ukraine": {"iso2": "UA", "iso3": "UKR"},
    "United Arab Emirates": {"iso2": "AE", "iso3": "ARE"},
    "United Kingdom": {"iso2": "GB", "iso3": "GBR"},
    "United States": {"iso2": "US", "iso3": "USA"},
    "Uruguay": {"iso2": "UY", "iso3": "URY"},
    "Vanuatu": {"iso2": "VU", "iso3": "VUT"},
    "Vatican City": {"iso2": "VA", "iso3": "VAT"},
    "Venezuela": {"iso2": "VE", "iso3": "VEN"},
    "Vietnam": {"iso2": "VN", "iso3": "VNM"},
    "Wallis & Futuna": {"iso2": "WF", "iso3": "WLF"},
    "Yemen": {"iso2": "YE", "iso3": "YEM"},
    "Zambia": {"iso2": "ZM", "iso3": "ZMB"},
    "Zimbabwe": {"iso2": "ZW", "iso3": "ZWE"},
}


class GrailedService:

    PLATFORM_ID = "grailed"

    FIELD_DEFAULTS = {
        "condition": "new",
        "color": "multi",
    }

    @staticmethod
    def _format_description(
        form_data: Dict[str, Any],
        stripped_description: str,
        country_full: str,
        seller_sku: str,
        size: str = "",
    ) -> str:
        stripped_description = re.sub(r"\n{2,}", "\n", stripped_description).strip()

        brand_name = form_data.get("brand_name", "")
        style_name = form_data.get("style_name", "")
        materials = form_data.get("material", "")
        manufacturer_sku = form_data.get("manufacturer_sku", "")

        parts = [
            brand_name,
            style_name,
            f"Size: {size}",
            "",
            "Highlights: ",
            stripped_description,
            "",
            "Material Composition:",
            materials,
            "",
            f"Manufacturer SKU: {manufacturer_sku}",
            "",
            f"Seller SKU: {seller_sku}",
            "",
            f"Made in {country_full}",
        ]

        return "\n".join(parts)

    def __init__(self):
        grailed_config = config.get("grailed", {})
        self.api_endpoint = grailed_config.get("api_endpoint", "")
        self.api_key = grailed_config.get("api_key", "")

    async def get_platform_settings(self) -> Dict[str, Any]:
        settings = await AppSettings.first()
        if not settings or not settings.platform_settings:
            return {}
        return settings.platform_settings.get(self.PLATFORM_ID, {})

    async def submit_listing(
        self,
        listing: Listing,
        form_data: Dict[str, Any],
        field_definitions: List[Dict[str, Any]],
        submission: ListingSubmission,
    ) -> ListingSubmission:
        try:
            products = await self.build_csv_rows(listing, form_data, field_definitions)

            if not products:
                raise ValueError("No children found to submit to Grailed")

            if not self.api_endpoint:
                raise ValueError(
                    "Grailed API endpoint not configured in config.toml [grailed] section"
                )

            async with httpx.AsyncClient(timeout=300.0) as client:
                response = await client.post(
                    self.api_endpoint,
                    follow_redirects=True,
                    json={
                        "key": self.api_key,
                        "action": "addListings",
                        "data": products,
                    },
                )
            print(response.content)
            response_data = response.json()

            if response_data.get("success"):
                submission.status = "success"
                added_refs = response_data.get("added_references", [])
                if added_refs:
                    submission.external_id = added_refs
                await submission.save()
                logger.info(
                    f"Grailed submission succeeded for listing {listing.id}: "
                    f"added={response_data.get('added', 0)}, "
                    f"skipped={response_data.get('skipped', 0)}"
                )
            else:
                error_msg = response_data.get("error", "Unknown error from Grailed AppScript")
                submission.status = "failed"
                submission.error = error_msg
                await submission.save()
                logger.error(
                    f"Grailed AppScript returned error for listing {listing.id}: {error_msg}"
                )
                raise ValueError(f"Grailed submission failed: {error_msg}")

        except Exception as e:
            logger.error(
                f"Grailed submission failed for listing {listing.id}: {traceback.format_exc()}"
            )
            submission.status = "failed"
            submission.error = traceback.format_exc()
            await submission.save()
            raise HTTPException(
                status_code=400,
                detail=f"Internal server error: {e}",
            )

        return submission

    async def build_csv_rows(
        self,
        listing: Listing,
        form_data: Dict[str, Any],
        field_definitions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        settings = await self.get_platform_settings()
        price_multiplier = settings.get("price_multiplier", None)
        if price_multiplier is None:
            raise ValueError("Grailed platform_settings missing required field: price_multiplier")
        shipping_us = settings.get("shipping_us", None)
        if shipping_us is None:
            raise ValueError("Grailed platform_settings missing required field: shipping_us")
        international_shipping = settings.get("international_shipping", None)
        if international_shipping is None:
            raise ValueError(
                "Grailed platform_settings missing required field: international_shipping"
            )

        grailed_field_map = {}
        for field_def in field_definitions:
            local_name = field_def.get("name")
            if not local_name:
                continue
            platforms = field_def.get("platforms") or []
            for platform in platforms:
                if platform.get("platform_id") == self.PLATFORM_ID:
                    grailed_field_map[local_name] = {
                        "field_id": platform.get("field_id"),
                        "platform_tags": platform.get("platform_tags") or [],
                    }
                    break

        row_data = {}
        for local_name, mapping in grailed_field_map.items():
            grailed_field = mapping["field_id"]
            tags = mapping["platform_tags"]
            value = form_data.get(local_name, "")

            if "strip_html" in tags and value:
                value = str(value)
                value = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
                value = re.sub(r"</?(p|div|h[1-6]|li|tr)\b[^>]*>", "\n", value, flags=re.IGNORECASE)
                value = re.sub(r"<[^>]+>", "", value)
                value = re.sub(r"\n{3,}", "\n\n", value).strip()

            if "apply_multiplier" in tags and value:
                try:
                    value = round(float(value) * price_multiplier)
                except (ValueError, TypeError):
                    pass

            row_data[grailed_field] = value

        stripped_description = row_data.pop("description", "")
        country_full = row_data.get("country_of_origin", "")

        category = row_data.get("category")
        if category:
            grailed_type = await listing_options_service.get_platform_type(
                category, self.PLATFORM_ID
            )
            if grailed_type:
                row_data["category"] = grailed_type

        country_of_origin = row_data.get("country_of_origin")
        if country_of_origin:
            row_data["country_of_origin"] = COUNTRY_CODE_MAP.get(country_of_origin, {}).get(
                "iso2", ""
            )

        base_product = {
            "condition": self.FIELD_DEFAULTS["condition"],
            "color": self.FIELD_DEFAULTS["color"],
            "shipping_us": shipping_us,
            "shipping_ca": international_shipping,
            "shipping_uk": international_shipping,
            "shipping_eu": international_shipping,
            "shipping_asia": international_shipping,
            "shipping_au": international_shipping,
            "shipping_other": international_shipping,
        }
        base_product.update(row_data)

        child_size_overrides = form_data.get("child_size_overrides", {})
        if not child_size_overrides:
            raise ValueError("No children found in listing data (child_size_overrides is empty)")

        sizing_scheme = form_data.get("SIZING_SCHEME", "")
        unique_sizes = list(set(v for v in child_size_overrides.values() if v and v.strip()))
        size_map = {}
        sizing_type = None
        product_type = form_data.get("product_type")
        if product_type:
            conn = Tortoise.get_connection("default")
            type_result = await conn.execute_query_dict(
                "SELECT sizing_types FROM listingoptions_types WHERE type = $1 LIMIT 1",
                [product_type],
            )
            if type_result:
                sizing_type = type_result[0]["sizing_types"]
        if sizing_scheme and unique_sizes:
            size_map = await listing_options_service.get_mapped_platform_sizes(
                sizing_scheme, unique_sizes, self.PLATFORM_ID, sizing_type
            )

        products = []
        for child_sku, size in child_size_overrides.items():
            product = {**base_product}
            product["sku"] = child_sku
            if size:
                grailed_size = size_map.get(size, size)
                if not grailed_size:
                    raise Exception("Internal Server Error")
                grailed_size = grailed_size.split(" ")[-1] if grailed_size else ""
                print("grailed size", grailed_size)
                if form_data.get("GENDER") == "Womens":
                    product["exact_size"] = grailed_size
                    product["size"] = ""
                else:
                    product["size"] = grailed_size
                    product["exact_size"] = ""
                product["title"] = f"{form_data.get('style_name')} SIZE {size}"
            product["description"] = self._format_description(
                form_data,
                stripped_description,
                country_full,
                listing.product_id,
                size=size,
            )
            products.append(product)

        return products


grailed_service = GrailedService()
