"""Outbound email: resolve a named recipient list, then send through the AppScript.

Two hops, because the two halves are owned by different systems.

  1. The auth service owns the address book. `GET /api/email_lists?name=...` with
     an `X-API-Key` header returns {"to": [...], "cc": [...], "bcc": [...]}, so a
     recipient change is a change over there and never a deploy here.
  2. The AppScript at `[email] endpoint` actually sends. It runs as its owner and
     therefore sends from a real mailbox.

WHY NOT THE GOOGLE SERVICE ACCOUNT: a GCP service account has no mailbox. Sending
as one requires domain-wide delegation so it can impersonate a real user, and DWD
would let that key act as ANY user in the Workspace - which is exactly why it was
ruled out for the Drive work. The AppScript gets the same result with none of
that blast radius.

Nothing here raises into a caller's critical path. A batch must not fail because
a notification did not go out.
"""

import logging
from typing import Any, Mapping, Sequence

import httpx

from config import config

logger = logging.getLogger(__name__)

_cfg = config.get("email", {})
ENDPOINT: str = _cfg.get("endpoint", "")
API_KEY: str = _cfg.get("api_key", "")
LIST_ENDPOINT: str = _cfg.get("list_endpoint", "")
LIST_API_KEY: str = _cfg.get("list_api_key", "")

TIMEOUT = httpx.Timeout(90.0)


async def resolve_list(name: str) -> dict[str, list[str]] | None:
    """{"to": [...], "cc": [...], "bcc": [...]} for a named list, or None.

    A missing list is a 404 with {"detail": "Email list not found"}, which is a
    configuration problem rather than an outage: log it and return None so the
    caller skips the send rather than retrying forever.
    """
    if not (LIST_ENDPOINT and LIST_API_KEY and name):
        logger.debug("email: list lookup not configured, skipping")
        return None
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(
                LIST_ENDPOINT, params={"name": name},
                headers={"X-API-Key": LIST_API_KEY},
            )
    except httpx.HTTPError as exc:
        logger.warning("email: could not reach the email-list service: %s", exc)
        return None

    if response.status_code == 404:
        logger.warning("email: no email list named %r", name)
        return None
    if response.status_code >= 400:
        logger.warning("email: list lookup for %r returned %s", name, response.status_code)
        return None

    emails = (response.json() or {}).get("emails") or {}
    return {
        "to": [e for e in emails.get("to") or [] if e],
        "cc": [e for e in emails.get("cc") or [] if e],
        "bcc": [e for e in emails.get("bcc") or [] if e],
    }


async def send(
    to: Sequence[str],
    subject: str,
    body: str,
    *,
    html_body: str | None = None,
    cc: Sequence[str] = (),
    bcc: Sequence[str] = (),
) -> bool:
    """Send one email. Returns whether it went out; never raises.

    The AppScript wants comma-separated recipient strings, not arrays, and
    answers 200 with {"success": false, "error": ...} on a bad payload rather
    than an HTTP error - so the body has to be checked, not just the status.

    ALWAYS SEND BOTH BODIES. The AppScript documents only (to, subject, body) and
    hands `body` to MailApp.sendEmail, which renders it as plain text - an HTML
    string there arrives as visible tags. Whether it honours `htmlBody` could not
    be determined from the API (it accepts and ignores unknown params, answering
    success either way). Sending both means the message is correct under either
    behaviour: the rich version if it is supported, a readable plain-text version
    with the URL spelled out if it is not.
    """
    if not (ENDPOINT and API_KEY):
        logger.debug("email: [email] endpoint/api_key not configured, skipping")
        return False
    if not to:
        logger.warning("email: refusing to send %r with no recipients", subject)
        return False

    payload: dict[str, Any] = {
        "key": API_KEY,
        "to": ", ".join(to),
        "subject": subject,
        "body": body,
    }
    if html_body:
        payload["htmlBody"] = html_body
    if cc:
        payload["cc"] = ", ".join(cc)
    if bcc:
        payload["bcc"] = ", ".join(bcc)

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            # follow_redirects: the AppScript exec URL 302s to its runtime host.
            response = await client.post(ENDPOINT, json=payload, follow_redirects=True)
    except httpx.HTTPError as exc:
        logger.warning("email: send failed for %r: %s", subject, exc)
        return False

    try:
        data = response.json()
    except ValueError:
        logger.warning("email: non-JSON reply for %r (HTTP %s)", subject, response.status_code)
        return False

    if not data.get("success"):
        # Never log `input` back: the AppScript echoes it, api key included.
        logger.warning("email: rejected %r: %s", subject, str(data.get("error"))[:200])
        return False
    logger.info("email: sent %r to %d recipient(s)", subject, len(to))
    return True


async def send_to_list(
    name: str, subject: str, body: str, html_body: str | None = None
) -> bool:
    """Resolve a named list and send to it. Never raises."""
    try:
        recipients = await resolve_list(name)
        if not recipients or not recipients["to"]:
            return False
        return await send(
            recipients["to"], subject, body, html_body=html_body,
            cc=recipients["cc"], bcc=recipients["bcc"],
        )
    except Exception:  # noqa: BLE001 - a notification must never break a caller
        logger.exception("email: unexpected failure sending %r", subject)
        return False
