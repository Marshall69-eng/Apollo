"""Notification channels: Telegram, SMS (Vonage) and a generic webhook.

This module only knows how to COMPOSE and SEND. Deciding when to alert, retries and
delivery bookkeeping live in engine.py (outbox pattern), so a slow or failing channel
can never stall the sensor loop.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

try:  # optional convenience
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
VONAGE_API_KEY = os.getenv("VONAGE_API_KEY", "").strip()
VONAGE_API_SECRET = os.getenv("VONAGE_API_SECRET", "").strip()
VONAGE_FROM_NUMBER = os.getenv("VONAGE_FROM_NUMBER", "").strip()
GENERIC_WEBHOOK_URL = os.getenv("GENERIC_WEBHOOK_URL", "").strip()  # env only: never taken from the UI (SSRF)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()  # optional: HMAC-signs webhook bodies

TIMEOUT = httpx.Timeout(10.0)


def channel_status() -> Dict[str, bool]:
    """Which channels have real credentials wired up."""
    return {
        "telegram": bool(TELEGRAM_BOT_TOKEN),
        "sms": bool(VONAGE_API_KEY and VONAGE_API_SECRET and VONAGE_FROM_NUMBER),
        "webhook": bool(GENERIC_WEBHOOK_URL),
    }


# ---------------------------------------------------------------
# Message composition
# ---------------------------------------------------------------
_HEADLINE = {
    "Clear": "ALL CLEAR",
    "Offline": "SENSOR OFFLINE",
}
_ACTION = {
    "Clear": "Readings are back below alert thresholds. Stand down per your DRRMO protocol.",
    "Offline": "Check buoy power and connectivity. Do not treat this site as safe until it reports again.",
}


def build_message(
    site_name: str,
    severity: str,
    flow: float,
    flood: float,
    reasons: List[str],
    lat: float,
    lon: float,
    barangay: str,
    drill: bool = False,
) -> str:
    stamp = datetime.now(timezone.utc).astimezone().strftime("%d %b %Y, %I:%M %p")
    maps = f"https://maps.google.com/?q={lat:.5f},{lon:.5f}"
    if drill:
        head = "DRILL - TEST ALERT (no action required)"
        action = "This is a scheduled test of the alert chain. No action required."
    else:
        head = _HEADLINE.get(severity, f"{severity.upper()} FLOOD ALERT")
        action = _ACTION.get(severity, "Advise DRRMO to pre-position responders and warn riverside households.")
    return (
        f"[NetZero Wave] {head}\n"
        f"Site: {site_name}\n"
        f"Barangay/LGU: {barangay}\n"
        f"{'Last reading - ' if severity == 'Offline' else ''}Flow velocity: {flow:.2f} m/s\n"
        f"Flood proximity index: {flood:.0f}/100\n"
        f"Trigger: {'; '.join(reasons)}\n"
        f"Location: {maps}\n"
        f"Time: {stamp}\n"
        f"Action: {action}"
    )


def build_sms(site_name: str, severity: str, flow: float, flood: float, lat: float, lon: float, drill: bool = False) -> str:
    """Short single-purpose text: keeps SMS to 1-2 GSM segments."""
    tag = "DRILL (test, no action)" if drill else _HEADLINE.get(severity, severity.upper())
    return (
        f"NetZero Wave {tag}: {site_name}. Flow {flow:.1f} m/s, flood idx {flood:.0f}/100. "
        f"https://maps.google.com/?q={lat:.4f},{lon:.4f}"
    )


def _ascii(text: str) -> str:
    """SMS-safe: swap typographic characters, then drop anything non-ASCII."""
    text = text.replace("\u2014", "-").replace("\u2013", "-").replace("\u2265", ">=")
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()


def _scrub(msg: str) -> str:
    for secret in (TELEGRAM_BOT_TOKEN, VONAGE_API_SECRET):
        if secret:
            msg = msg.replace(secret, "***")
    return msg[:180]


# ---------------------------------------------------------------
# Channel senders. `target` is ALWAYS the recipient's own chat id / number.
# ---------------------------------------------------------------
async def _send_telegram(client: httpx.AsyncClient, chat_id: str, text: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        return {"status": "simulated", "detail": "TELEGRAM_BOT_TOKEN not set"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = await client.post(url, json={"chat_id": chat_id, "text": text})
    if r.status_code == 200 and r.json().get("ok"):
        return {"status": "sent", "detail": "telegram ok"}
    return {"status": "failed", "detail": _scrub(f"telegram {r.status_code}: {r.text}")}


async def _send_sms(client: httpx.AsyncClient, to_number: str, text: str) -> Dict[str, Any]:
    if not (VONAGE_API_KEY and VONAGE_API_SECRET and VONAGE_FROM_NUMBER):
        return {"status": "simulated", "detail": "Vonage credentials not set"}
    payload = {
        "api_key": VONAGE_API_KEY,
        "api_secret": VONAGE_API_SECRET,
        "to": re.sub(r"\D", "", to_number),  # Vonage wants digits only (E.164 without '+')
        "from": VONAGE_FROM_NUMBER,
        "text": _ascii(text)[:300],
    }
    r = await client.post("https://rest.nexmo.com/sms/json", json=payload)
    if r.status_code == 200:
        messages = r.json().get("messages", [])
        if messages and messages[0].get("status") == "0":
            return {"status": "sent", "detail": f"vonage msg id {messages[0].get('message-id', '')}"}
        err = messages[0].get("error-text", "unknown error") if messages else "no message block"
        return {"status": "failed", "detail": _scrub(f"vonage error: {err}")}
    return {"status": "failed", "detail": _scrub(f"vonage {r.status_code}: {r.text}")}


async def _send_webhook(client: httpx.AsyncClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not GENERIC_WEBHOOK_URL:
        return {"status": "simulated", "detail": "GENERIC_WEBHOOK_URL not set"}
    body = json.dumps(payload, separators=(",", ":")).encode()
    headers = {"Content-Type": "application/json"}
    if WEBHOOK_SECRET:
        headers["X-NetZero-Signature"] = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    r = await client.post(GENERIC_WEBHOOK_URL, content=body, headers=headers)
    ok = 200 <= r.status_code < 300
    return {"status": "sent" if ok else "failed", "detail": f"webhook {r.status_code}"}


async def send_one(
    client: httpx.AsyncClient, channel: str, target: str, text: str, payload: Dict[str, Any], dry_run: bool = False
) -> Dict[str, Any]:
    if dry_run:
        return {"status": "simulated", "detail": "simulation mode: nothing was sent"}
    if channel == "telegram":
        return await _send_telegram(client, target, text)
    if channel == "sms":
        return await _send_sms(client, target, text)
    if channel == "webhook":
        return await _send_webhook(client, payload)
    return {"status": "failed", "detail": f"unknown channel {channel}"}


async def deliver(items: List[Dict[str, Any]], dry_run: bool = False) -> List[Dict[str, Any]]:
    """Attempt each item once; results are returned in the same order.
    Never raises: one bad recipient cannot block the rest."""
    results: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for it in items:
            try:
                res = await send_one(client, it["channel"], it["target"], it["text"], it.get("payload") or {}, dry_run)
            except Exception as exc:  # network blip, DNS, timeout...
                res = {"status": "failed", "detail": _scrub(f"{type(exc).__name__}: {exc}")}
            results.append(res)
    return results
