"""Shared per-platform field-template rendering.

One place for the `{placeholder}` substitution logic that used to be duplicated in
`listing_service._generate_product_name` and `sellercloud_service._populate_description_template`.

Template shape stored in ``app_settings.field_templates`` is per-platform and per logical field::

    {
        "sellercloud": {"title": "...", "description": "<html>..."},
        "grailed":     {"title": "...", "description": "..."},
        "spo":         {}
    }

Placeholder syntax:
- ``{field}``      -> replaced with ``context["field"]`` (empty removed).
- ``{a/b}``        -> first of ``a``, ``b`` with a non-empty value ("use a, else b").

A resolver falls back to the legacy flat ``field_templates[field]`` shape so behaviour is
preserved during the window between deploying this code and running the one-time backfill.
"""

import re
from typing import Any, Callable, Dict, List, Optional, Tuple

_PLACEHOLDER_RE = re.compile(r"\{([^}]+)\}")


def extract_placeholders(template: str) -> List[str]:
    """Return every field name referenced by a template, expanding ``{a/b}`` into ``a`` and ``b``.

    Used by save-time strict validation to check tokens against the valid field list.
    """
    names: List[str] = []
    if not template:
        return names
    for match in _PLACEHOLDER_RE.finditer(template):
        for option in match.group(1).split("/"):
            option = option.strip()
            if option:
                names.append(option)
    return names


def render_template(
    template: str,
    context: Dict[str, Any],
    *,
    value_transforms: Optional[Dict[str, Callable[[Any], Any]]] = None,
    collapse_whitespace: bool = False,
    field_labels: Optional[Dict[str, str]] = None,
) -> Tuple[str, List[str]]:
    """Substitute ``{placeholder}`` tokens in ``template`` using ``context``.

    Returns ``(rendered, missing)`` where ``missing`` is the list of placeholders that resolved
    to an empty value (labelled via ``field_labels`` when provided). Empty placeholders are
    removed from the output; callers that need strict behaviour raise on a non-empty ``missing``.

    - ``value_transforms``: optional ``{field_name: fn}`` applied to the chosen value (e.g. the
      SellerCloud GENDER/MATERIAL transforms). Keyed on the *resolved* option name.
    - ``collapse_whitespace``: when True, collapse runs of whitespace to single spaces and trim
      (matches the old ProductName generation).
    """
    if not template:
        return "", []

    value_transforms = value_transforms or {}
    field_labels = field_labels or {}
    missing: List[str] = []
    result = template

    for match in _PLACEHOLDER_RE.finditer(template):
        token = match.group(0)
        options = [opt.strip() for opt in match.group(1).split("/") if opt.strip()]

        resolved: Optional[str] = None
        for option in options:
            raw = context.get(option)
            if raw is not None and str(raw).strip() != "":
                value: Any = raw
                if option in value_transforms:
                    value = value_transforms[option](value)
                resolved = str(value)
                break

        if resolved is None:
            label = field_labels.get(match.group(1), match.group(1))
            missing.append(label)
            result = result.replace(token, "")
        else:
            result = result.replace(token, resolved)

    if collapse_whitespace:
        result = " ".join(result.split())

    return result, missing


def resolve_field_template(
    field_templates: Optional[Dict[str, Any]],
    platform_id: str,
    field_key: str,
) -> Optional[str]:
    """Return the template string for ``(platform_id, field_key)`` or None.

    Prefers the per-platform nested value; falls back to the legacy flat ``field_templates[field_key]``
    so consumers keep working until the backfill restructures the singleton row.
    """
    if not isinstance(field_templates, dict):
        return None

    platform_templates = field_templates.get(platform_id)
    if isinstance(platform_templates, dict):
        value = platform_templates.get(field_key)
        if isinstance(value, str) and value.strip():
            return value

    legacy = field_templates.get(field_key)
    if isinstance(legacy, str) and legacy.strip():
        return legacy

    return None


def build_field_value(
    field_templates: Optional[Dict[str, Any]],
    platform_id: str,
    field_key: str,
    field_def: Optional[Dict[str, Any]],
    context: Dict[str, Any],
    *,
    value_transforms: Optional[Dict[str, Callable[[Any], Any]]] = None,
    collapse_whitespace: bool = False,
) -> Any:
    """Resolve a single platform field value: template if configured, else raw mapped value.

    Fallback order (matches the product decision):
    1. Per-platform template configured -> render it (returns a string).
    2. Field's ``use_raw_fallback`` flag on (default) -> the field's raw value from ``context``,
       returned unchanged (today's SPO pass-through behaviour; preserves non-string types).
    3. Otherwise -> empty string (field omitted from the payload).
    """
    template = resolve_field_template(field_templates, platform_id, field_key)
    if template is not None:
        rendered, _missing = render_template(
            template,
            context,
            value_transforms=value_transforms,
            collapse_whitespace=collapse_whitespace,
        )
        return rendered

    use_raw = True
    if isinstance(field_def, dict):
        use_raw = field_def.get("use_raw_fallback", True)

    if use_raw:
        return context.get(field_key, "")

    return ""
