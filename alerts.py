"""
Alert dispatch service — Telegram + Twilio SMS webhooks
=======================================================

Sends emergency notifications to Barangay / LGU DRRMO personnel when a
site crosses a critical flood-proximity or flow-velocity threshold.

Credentials are read from environment variables (a `.env` file is loaded
automatically if `python-dotenv` is installed):

    TELEGRAM_BOT_TOKEN=8827096340:AAG0_PSFr27scqu5B9YV72ZAcAT3xQYPwx8
    TWILIO_ACCOUNT_SID=ACxxxxxxxx
    TWILIO_AUTH_TOKEN=xxxxxxxx
    TWILIO_FROM_NUMBER=+15551234567
    GENERIC_WEBHOOK_URL=https://hooks.example.org/drrmo   (optional)
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
VONAGE_API_KEY = os.getenv("VONAGE_API_KEY", "").strip()
VONAGE_API_SECRET = os.getenv("VONAGE_API_SECRET", "").strip()
VONAGE_FROM_NUMBER = os.getenv("VONAGE_FROM_NUMBER", "").strip()
GENERIC_WEBHOOK_URL = os.getenv("GENERIC_WEBHOOK_URL", "").strip()

If a channel has no credentials configured the dispatcher runs in
DRY-RUN mode: the alert is still recorded and shown in the dashboard,
with delivery status "simulated" instead of "sent". That keeps the demo
fully functional without a paid Twilio account or a live bot token.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
def channel_status() -> Dict[str, bool]:
    """Which channels have real credentials wired up."""
    return {
        "telegram": bool(TELEGRAM_BOT_TOKEN),
        "sms": bool(VONAGE_API_KEY and VONAGE_API_SECRET and VONAGE_FROM_NUMBER),
        "webhook": bool(GENERIC_WEBHOOK_URL),
    }
import httpx


try:  # optional convenience
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "").strip()
GENERIC_WEBHOOK_URL = os.getenv("GENERIC_WEBHOOK_URL", "").strip()

TIMEOUT = httpx.Timeout(10.0)


def channel_status() -> Dict[str, bool]:
    """Which channels have real credentials wired up."""
    return {
        "telegram": bool(TELEGRAM_BOT_TOKEN),
        "sms": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER),
        "webhook": bool(GENERIC_WEBHOOK_URL),
    }


# ---------------------------------------------------------------
# Message composition
# ---------------------------------------------------------------
def build_message(
    site_name: str,
    severity: str,
    flow: float,
    flood: float,
    reasons: List[str],
    lat: float,
    lon: float,
    barangay: str,
) -> str:
    stamp = datetime.now(timezone.utc).astimezone().strftime("%d %b %Y, %I:%M %p")
    maps = f"https://maps.google.com/?q={lat:.5f},{lon:.5f}"
    return (
        f"[NetZero Wave] {severity.upper()} FLOOD ALERT\n"
        f"Site: {site_name}\n"
        f"Barangay/LGU: {barangay}\n"
        f"Flow velocity: {flow:.2f} m/s\n"
        f"Flood proximity index: {flood:.0f}/100\n"
        f"Trigger: {'; '.join(reasons)}\n"
        f"Location: {maps}\n"
        f"Time: {stamp}\n"
        f"Action: advise DRRMO to pre-position responders and warn riverside households."
    )


# ---------------------------------------------------------------
# Channel senders
# ---------------------------------------------------------------

async def _send_sms(client: httpx.AsyncClient, to_number: str, text: str) -> Dict[str, Any]:
    if not (VONAGE_API_KEY and VONAGE_API_SECRET and VONAGE_FROM_NUMBER):
        return {"status": "simulated", "detail": "Vonage credentials not set"}
    
    url = "https://rest.nexmo.com/sms/json"
    payload = {
        "api_key": VONAGE_API_KEY,
        "api_secret": VONAGE_API_SECRET,
        "to": to_number,
        "from": VONAGE_FROM_NUMBER,
        "text": text[:1500],
    }
    
    r = await client.post(url, json=payload)
    if r.status_code == 200:
        data = r.json()
        messages = data.get("messages", [])
        if messages and messages[0].get("status") == "0":
            return {"status": "sent", "detail": f"vonage msg id {messages[0].get('message-id', '')}"}
        else:
            err = messages[0].get("error-text", "unknown error") if messages else "no message block"
            return {"status": "failed", "detail": f"vonage error: {err}"}
            
    return {"status": "failed", "detail": f"vonage {r.status_code}: {r.text[:180]}"}
async def _send_telegram(client: httpx.AsyncClient, chat_id: str, text: str) -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        return {"status": "simulated", "detail": "TELEGRAM_BOT_TOKEN not set"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = await client.post(url, json={"chat_id": chat_id, "text": text})
    if r.status_code == 200 and r.json().get("ok"):
        return {"status": "sent", "detail": "telegram ok"}
    return {"status": "failed", "detail": f"telegram {r.status_code}: {r.text[:180]}"}


async def _send_sms(client: httpx.AsyncClient, to_number: str, text: str) -> Dict[str, Any]:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER):
        return {"status": "simulated", "detail": "Twilio credentials not set"}
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    r = await client.post(
        url,
        data={"From": TWILIO_FROM_NUMBER, "To": to_number, "Body": text[:1500]},
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
    )
    if r.status_code in (200, 201):
        return {"status": "sent", "detail": f"twilio sid {r.json().get('sid', '')}"}
    return {"status": "failed", "detail": f"twilio {r.status_code}: {r.text[:180]}"}


async def _send_webhook(client: httpx.AsyncClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not GENERIC_WEBHOOK_URL:
        return {"status": "simulated", "detail": "GENERIC_WEBHOOK_URL not set"}
    r = await client.post(GENERIC_WEBHOOK_URL, json=payload)
    ok = 200 <= r.status_code < 300
    return {
        "status": "sent" if ok else "failed",
        "detail": f"webhook {r.status_code}",
    }


# ---------------------------------------------------------------
# Fan-out
# ---------------------------------------------------------------
async def dispatch(
    recipients: List[Dict[str, Any]],
    text: str,
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Send `text` to every active recipient. Never raises — each
    recipient gets its own delivery record so one bad number can't
    block the rest of the fan-out."""
    results: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for rcpt in recipients:
            ch = rcpt["channel"]
            try:
                if ch == "telegram":
                    res = await _send_telegram(client, rcpt["target"], text)
                elif ch == "sms":
                    res = await _send_sms(client, rcpt["target"], text)
                elif ch == "webhook":
                    res = await _send_webhook(client, payload)
                else:
                    res = {"status": "failed", "detail": f"unknown channel {ch}"}
            except Exception as exc:  # network blip, DNS, timeout…
                res = {"status": "failed", "detail": f"{type(exc).__name__}: {exc}"[:180]}

            results.append(
                {
                    "recipient_id": rcpt["id"],
                    "name": rcpt["name"],
                    "org": rcpt.get("org", ""),
                    "channel": ch,
                    "target": rcpt["target"],
                    **res,
                }
            )

        if GENERIC_WEBHOOK_URL and not any(r["channel"] == "webhook" for r in results):
            try:
                res = await _send_webhook(client, payload)
                results.append(
                    {
                        "recipient_id": "webhook",
                        "name": "DRRMO webhook",
                        "org": "Integration",
                        "channel": "webhook",
                        "target": GENERIC_WEBHOOK_URL,
                        **res,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "recipient_id": "webhook",
                        "name": "DRRMO webhook",
                        "org": "Integration",
                        "channel": "webhook",
                        "target": GENERIC_WEBHOOK_URL,
                        "status": "failed",
                        "detail": str(exc)[:180],
                    }
                )
    return results
