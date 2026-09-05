"""
NetZero Wave backend (FastAPI) — Self-Contained Monolith
======================================================

Serves the site-selection scoring model, live unit-monitoring data,
an interactive GIS layer, and an automated Telegram / SMS alert
dispatcher — all backed by a local SQLite file and contained in a 
single file.

Run:
    pip install fastapi uvicorn httpx python-dotenv pydantic
    uvicorn main:app --reload

Interactive API docs:
    http://127.0.0.1:8000/docs
"""

import asyncio
import json
import os
import random
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# ---------------------------------------------------------------
# Alert service configuration & credentials
# ---------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "").strip()
GENERIC_WEBHOOK_URL = os.getenv("GENERIC_WEBHOOK_URL", "").strip()

TIMEOUT = httpx.Timeout(10.0)
DB_PATH = "storm_baler.db"


def channel_status() -> Dict[str, bool]:
    return {
        "telegram": bool(TELEGRAM_BOT_TOKEN),
        "sms": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER),
        "webhook": bool(GENERIC_WEBHOOK_URL),
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


async def dispatch(
    recipients: List[Dict[str, Any]],
    text: str,
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
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
            except Exception as exc:
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
                        "status": "failed",
                        "detail": str(exc)[:180],
                    }
                )
    return results


# ---------------------------------------------------------------
# Scoring model & weights
# ---------------------------------------------------------------
WEIGHTS = {"flow": 0.30, "debris": 0.30, "flood": 0.25, "solar": 0.15}


def flow_score(v: float) -> float:
    if v < 0.5:
        return (v / 0.5) * 60
    if v <= 3.0:
        return 60 + ((v - 0.5) / 2.5) * 40
    return max(40, min(100, 100 - (v - 3.0) * 15))


def composite_score(flow: float, debris: float, flood: float, solar: float) -> int:
    fs = flow_score(flow)
    total = (
        fs * WEIGHTS["flow"]
        + debris * WEIGHTS["debris"]
        + flood * WEIGHTS["flood"]
        + solar * WEIGHTS["solar"]
    )
    return round(total)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def alert_level(flow: float, flood: float) -> str:
    risk = flow * 20 + flood * 0.5
    if risk > 95:
        return "High"
    if risk > 60:
        return "Moderate"
    return "Low"


# ---------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------
SEED_SITES = [
    ("S1", "Marikina River — Tumana Bridge", 1.9, 58, 72, 80, 14.64650, 121.08630, "Brgy. Tumana, Marikina City"),
    ("S2", "Pasig River — Guadalupe", 1.2, 74, 65, 60, 14.56847, 121.04600, "Brgy. Guadalupe Nuevo, Makati / Mandaluyong"),
    ("S3", "San Juan River — Ortigas Ave", 0.7, 41, 48, 55, 14.60166, 121.02030, "Brgy. Corazon de Jesus, San Juan City"),
    ("S4", "Marikina River — Nangka", 2.4, 66, 83, 88, 14.67548, 121.10685, "Brgy. Nangka, Marikina City"),
    ("S5", "Pasig River — Lambingan", 1.5, 52, 59, 70, 14.58639, 121.01972, "Brgy. 866 Sta. Ana, Manila"),
]

RIVER_PATHS = {
    "Pasig River": [
        [14.59560, 120.97290], [14.58990, 120.98600], [14.58750, 120.99850],
        [14.58639, 121.01972], [14.58200, 121.03100], [14.56847, 121.04600],
        [14.56600, 121.05800], [14.57450, 121.06950], [14.58600, 121.07600],
    ],
    "Marikina River": [
        [14.58600, 121.07600], [14.59900, 121.08100], [14.61500, 121.08000],
        [14.63200, 121.08450], [14.64650, 121.08630], [14.65900, 121.09400],
        [14.67548, 121.10685], [14.69100, 121.11500],
    ],
    "San Juan River": [
        [14.58750, 120.99850], [14.59300, 121.00700], [14.60166, 121.02030],
        [14.60900, 121.03100], [14.61700, 121.04200],
    ],
}

SEED_RECIPIENTS = [
    ("R1", "Marikina CDRRMO Operations", "Marikina City DRRMO", "telegram", "-1001234567890"),
    ("R2", "Brgy. Tumana Emergency Desk", "Barangay Tumana", "sms", "+639171234567"),
    ("R3", "MMDA Flood Control", "MMDA", "telegram", "-1009876543210"),
]

DEFAULT_CONFIG = {
    "enabled": "1",
    "flood_critical": "85",
    "flood_warning": "72",
    "flow_critical": "3.0",
    "flow_warning": "2.6",
    "cooldown_minutes": "10",
}


# ---------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
    cols = {r["name"] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sites (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            flow REAL, debris REAL, flood REAL, solar REAL,
            lat REAL, lon REAL, barangay TEXT
        )
        """
    )
    _ensure_column(cur, "sites", "lat", "REAL")
    _ensure_column(cur, "sites", "lon", "REAL")
    _ensure_column(cur, "sites", "barangay", "TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            flow REAL, debris REAL, flood REAL, solar REAL, score INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS units (
            id TEXT PRIMARY KEY,
            site_id TEXT NOT NULL,
            deployed_at TEXT NOT NULL,
            plastic_load REAL,
            bales INTEGER,
            active INTEGER DEFAULT 1
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS recipients (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            org TEXT,
            channel TEXT NOT NULL,
            target TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT NOT NULL,
            site_name TEXT NOT NULL,
            ts TEXT NOT NULL,
            severity TEXT NOT NULL,
            flow REAL, flood REAL,
            reasons TEXT,
            message TEXT,
            deliveries TEXT,
            acknowledged INTEGER DEFAULT 0
        )
        """
    )
    cur.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)")

    if cur.execute("SELECT COUNT(*) AS c FROM sites").fetchone()["c"] == 0:
        cur.executemany(
            "INSERT INTO sites (id, name, flow, debris, flood, solar, lat, lon, barangay) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            SEED_SITES,
        )
    else:
        for s in SEED_SITES:
            cur.execute(
                "UPDATE sites SET lat=COALESCE(lat,?), lon=COALESCE(lon,?), "
                "barangay=COALESCE(barangay,?) WHERE id=?",
                (s[6], s[7], s[8], s[0]),
            )

    if cur.execute("SELECT COUNT(*) AS c FROM recipients").fetchone()["c"] == 0:
        cur.executemany(
            "INSERT INTO recipients (id, name, org, channel, target, active) "
            "VALUES (?,?,?,?,?,1)",
            SEED_RECIPIENTS,
        )

    for k, v in DEFAULT_CONFIG.items():
        cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?,?)", (k, v))

    conn.commit()
    conn.close()


def read_config(conn: sqlite3.Connection) -> Dict[str, float]:
    rows = conn.execute("SELECT key, value FROM config").fetchall()
    raw = {r["key"]: r["value"] for r in rows}
    return {
        "enabled": raw.get("enabled", "1") == "1",
        "flood_critical": float(raw.get("flood_critical", 85)),
        "flood_warning": float(raw.get("flood_warning", 72)),
        "flow_critical": float(raw.get("flow_critical", 3.0)),
        "flow_warning": float(raw.get("flow_warning", 2.6)),
        "cooldown_minutes": int(float(raw.get("cooldown_minutes", 10))),
    }


# ---------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------
class Site(BaseModel):
    id: str
    name: str
    flow: float
    debris: float
    flood: float
    solar: float
    score: int
    lat: Optional[float] = None
    lon: Optional[float] = None
    barangay: Optional[str] = None
    status: str = "Low"
    deployed: bool = False


class Unit(BaseModel):
    id: str
    site_id: str
    site_name: str
    deployed_at: str
    flow: float
    flood_alert: str
    plastic_load: float
    bales: int
    lat: Optional[float] = None
    lon: Optional[float] = None


class Recipient(BaseModel):
    id: str
    name: str
    org: str = ""
    channel: str
    target: str
    active: bool = True


class RecipientIn(BaseModel):
    name: str
    org: str = ""
    channel: str = Field(description="telegram | sms | webhook")
    target: str


class AlertConfig(BaseModel):
    enabled: bool = True
    flood_critical: float = 85
    flood_warning: float = 72
    flow_critical: float = 3.0
    flow_warning: float = 2.6
    cooldown_minutes: int = 10


class Alert(BaseModel):
    id: int
    site_id: str
    site_name: str
    ts: str
    severity: str
    flow: float
    flood: float
    reasons: List[str]
    message: str
    deliveries: List[Dict[str, Any]]
    acknowledged: bool


# ---------------------------------------------------------------
# Alert evaluation & background engine
# ---------------------------------------------------------------
def _classify(site: sqlite3.Row, cfg: Dict[str, Any]):
    reasons: List[str] = []
    severity = None

    if site["flood"] >= cfg["flood_critical"]:
        severity = "Critical"
        reasons.append(f"flood proximity {site['flood']:.0f} ≥ critical {cfg['flood_critical']:.0f}")
    if site["flow"] >= cfg["flow_critical"]:
        severity = "Critical"
        reasons.append(f"flow velocity {site['flow']:.2f} m/s ≥ critical {cfg['flow_critical']:.2f}")

    if severity is None:
        if site["flood"] >= cfg["flood_warning"]:
            severity = "Warning"
            reasons.append(f"flood proximity {site['flood']:.0f} ≥ warning {cfg['flood_warning']:.0f}")
        if site["flow"] >= cfg["flow_warning"]:
            severity = "Warning"
            reasons.append(f"flow velocity {site['flow']:.2f} m/s ≥ warning {cfg['flow_warning']:.2f}")

    return severity, reasons


def _in_cooldown(conn: sqlite3.Connection, site_id: str, severity: str, minutes: int) -> bool:
    row = conn.execute(
        "SELECT ts FROM alerts WHERE site_id=? AND severity=? ORDER BY id DESC LIMIT 1",
        (site_id, severity),
    ).fetchone()
    if not row:
        return False
    try:
        last = datetime.fromisoformat(row["ts"])
    except ValueError:
        return False
    return datetime.now(timezone.utc) - last < timedelta(minutes=minutes)


async def evaluate_alerts() -> List[int]:
    conn = get_db()
    cfg = read_config(conn)
    if not cfg["enabled"]:
        conn.close()
        return []

    sites = conn.execute("SELECT * FROM sites").fetchall()
    recipients = [dict(r) for r in conn.execute("SELECT * FROM recipients WHERE active=1").fetchall()]

    pending = []
    for s in sites:
        severity, reasons = _classify(s, cfg)
        if not severity:
            continue
        if _in_cooldown(conn, s["id"], severity, cfg["cooldown_minutes"]):
            continue
        pending.append((s, severity, reasons))

    created: List[int] = []
    for s, severity, reasons in pending:
        created.append(await _raise_alert(conn, s, severity, reasons, recipients))

    conn.commit()
    conn.close()
    return created


async def _raise_alert(
    conn: sqlite3.Connection,
    s: sqlite3.Row,
    severity: str,
    reasons: List[str],
    recipients: List[Dict[str, Any]],
) -> int:
    text = build_message(
        site_name=s["name"],
        severity=severity,
        flow=s["flow"],
        flood=s["flood"],
        reasons=reasons,
        lat=s["lat"] or 0.0,
        lon=s["lon"] or 0.0,
        barangay=s["barangay"] or "—",
    )
    ts = datetime.now(timezone.utc).isoformat()
    payload = {
        "site_id": s["id"],
        "site_name": s["name"],
        "barangay": s["barangay"],
        "severity": severity,
        "flow_mps": round(s["flow"], 3),
        "flood_index": round(s["flood"], 1),
        "lat": s["lat"],
        "lon": s["lon"],
        "reasons": reasons,
        "ts": ts,
        "message": text,
    }
    deliveries = await dispatch(recipients, text, payload)

    cur = conn.execute(
        "INSERT INTO alerts (site_id, site_name, ts, severity, flow, flood, reasons, "
        "message, deliveries, acknowledged) VALUES (?,?,?,?,?,?,?,?,?,0)",
        (
            s["id"], s["name"], ts, severity, s["flow"], s["flood"],
            json.dumps(reasons), text, json.dumps(deliveries),
        ),
    )
    conn.commit()
    return cur.lastrowid


def _alert_row(r: sqlite3.Row) -> Alert:
    return Alert(
        id=r["id"],
        site_id=r["site_id"],
        site_name=r["site_name"],
        ts=r["ts"],
        severity=r["severity"],
        flow=r["flow"],
        flood=r["flood"],
        reasons=json.loads(r["reasons"] or "[]"),
        message=r["message"] or "",
        deliveries=json.loads(r["deliveries"] or "[]"),
        acknowledged=bool(r["acknowledged"]),
    )


async def simulate_tick() -> None:
    while True:
        await asyncio.sleep(3)
        conn = get_db()
        cur = conn.cursor()

        rows = cur.execute("SELECT * FROM sites").fetchall()
        for r in rows:
            flow = clamp(r["flow"] + random.uniform(-0.09, 0.09), 0.2, 3.6)
            debris = clamp(r["debris"] + random.uniform(-2, 2), 5, 98)
            flood = clamp(r["flood"] + random.uniform(-1, 1), 10, 98)
            solar = clamp(r["solar"] + random.uniform(-1, 1), 20, 99)
            score = composite_score(flow, debris, flood, solar)

            cur.execute(
                "UPDATE sites SET flow=?, debris=?, flood=?, solar=? WHERE id=?",
                (flow, debris, flood, solar, r["id"]),
            )
            cur.execute(
                "INSERT INTO readings (site_id, ts, flow, debris, flood, solar, score) "
                "VALUES (?,?,?,?,?,?,?)",
                (r["id"], datetime.now(timezone.utc).isoformat(), flow, debris, flood, solar, score),
            )

        units = cur.execute("SELECT * FROM units WHERE active=1").fetchall()
        for u in units:
            plastic_load = clamp(u["plastic_load"] + random.uniform(-3, 3), 10, 98)
            bales = u["bales"] + (1 if random.random() < 0.15 else 0)
            cur.execute(
                "UPDATE units SET plastic_load=?, bales=? WHERE id=?",
                (plastic_load, bales, u["id"]),
            )

        conn.commit()
        conn.close()

        try:
            await evaluate_alerts()
        except Exception as exc:
            print(f"[alerts] evaluation failed: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(simulate_tick())
    yield
    task.cancel()


# ---------------------------------------------------------------
# FastAPI application setup
# ---------------------------------------------------------------
app = FastAPI(title="NetZero Wave API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def read_root():
    return FileResponse("index.html")


def _unit_response(u: sqlite3.Row, site: sqlite3.Row) -> Unit:
    return Unit(
        id=u["id"],
        site_id=u["site_id"],
        site_name=site["name"],
        deployed_at=u["deployed_at"],
        flow=site["flow"],
        flood_alert=alert_level(site["flow"], site["flood"]),
        plastic_load=u["plastic_load"],
        bales=u["bales"],
        lat=site["lat"],
        lon=site["lon"],
    )


@app.get("/api/sites", response_model=List[Site])
def list_sites():
    conn = get_db()
    rows = conn.execute("SELECT * FROM sites").fetchall()
    deployed = {r["site_id"] for r in conn.execute("SELECT site_id FROM units WHERE active=1").fetchall()}
    cfg = read_config(conn)
    conn.close()

    sites = []
    for r in rows:
        severity, _ = _classify(r, cfg)
        sites.append(
            Site(
                id=r["id"],
                name=r["name"],
                flow=r["flow"],
                debris=r["debris"],
                flood=r["flood"],
                solar=r["solar"],
                score=composite_score(r["flow"], r["debris"], r["flood"], r["solar"]),
                lat=r["lat"],
                lon=r["lon"],
                barangay=r["barangay"],
                status={"Critical": "High", "Warning": "Moderate"}.get(severity, "Low"),
                deployed=r["id"] in deployed,
            )
        )
    return sorted(sites, key=lambda s: s.score, reverse=True)


@app.get("/api/geo/rivers")
def river_geometry():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": name},
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[lon, lat] for lat, lon in path],
                },
            }
            for name, path in RIVER_PATHS.items()
        ],
    }


@app.get("/api/sites/{site_id}/history")
def site_history(site_id: str, limit: int = 50):
    conn = get_db()
    rows = conn.execute(
        "SELECT ts, flow, debris, flood, solar, score FROM readings "
        "WHERE site_id=? ORDER BY id DESC LIMIT ?",
        (site_id, limit),
    ).fetchall()
    conn.close()
    if not rows:
        raise HTTPException(404, "No readings yet for this site")
    return list(reversed([dict(r) for r in rows]))


@app.post("/api/deploy/{site_id}", response_model=Unit)
def deploy_site(site_id: str):
    conn = get_db()
    site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    if not site:
        conn.close()
        raise HTTPException(404, "Unknown site")

    existing = conn.execute("SELECT * FROM units WHERE site_id=? AND active=1", (site_id,)).fetchone()
    if existing:
        conn.close()
        return _unit_response(existing, site)

    count = conn.execute("SELECT COUNT(*) AS c FROM units").fetchone()["c"]
    unit_id = f"PB-{count + 1:02d}"
    conn.execute(
        "INSERT INTO units (id, site_id, deployed_at, plastic_load, bales, active) "
        "VALUES (?,?,?,?,?,1)",
        (unit_id, site_id, datetime.now(timezone.utc).isoformat(), site["debris"], 0),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM units WHERE id=?", (unit_id,)).fetchone()
    conn.close()
    return _unit_response(row, site)


@app.get("/api/units", response_model=List[Unit])
def list_units():
    conn = get_db()
    rows = conn.execute("SELECT * FROM units WHERE active=1").fetchall()
    result = []
    for u in rows:
        site = conn.execute("SELECT * FROM sites WHERE id=?", (u["site_id"],)).fetchone()
        result.append(_unit_response(u, site))
    conn.close()
    return result


@app.get("/api/units/{unit_id}", response_model=Unit)
def get_unit(unit_id: str):
    conn = get_db()
    u = conn.execute("SELECT * FROM units WHERE id=? AND active=1", (unit_id,)).fetchone()
    if not u:
        conn.close()
        raise HTTPException(404, "Unit not found or retired")
    site = conn.execute("SELECT * FROM sites WHERE id=?", (u["site_id"],)).fetchone()
    conn.close()
    return _unit_response(u, site)


@app.delete("/api/units/{unit_id}")
def retire_unit(unit_id: str):
    conn = get_db()
    conn.execute("UPDATE units SET active=0 WHERE id=?", (unit_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/alert-config")
def get_alert_config():
    conn = get_db()
    cfg = read_config(conn)
    conn.close()
    return {**cfg, "channels": channel_status()}


@app.put("/api/alert-config")
def update_alert_config(cfg: AlertConfig):
    conn = get_db()
    values = {
        "enabled": "1" if cfg.enabled else "0",
        "flood_critical": str(cfg.flood_critical),
        "flood_warning": str(cfg.flood_warning),
        "flow_critical": str(cfg.flow_critical),
        "flow_warning": str(cfg.flow_warning),
        "cooldown_minutes": str(cfg.cooldown_minutes),
    }
    for k, v in values.items():
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (k, v),
        )
    conn.commit()
    result = read_config(conn)
    conn.close()
    return {**result, "channels": channel_status()}


@app.get("/api/recipients", response_model=List[Recipient])
def list_recipients():
    conn = get_db()
    rows = conn.execute("SELECT * FROM recipients ORDER BY rowid").fetchall()
    conn.close()
    return [
        Recipient(
            id=r["id"], name=r["name"], org=r["org"] or "",
            channel=r["channel"], target=r["target"], active=bool(r["active"]),
        )
        for r in rows
    ]


@app.post("/api/recipients", response_model=Recipient)
def add_recipient(body: RecipientIn):
    if body.channel not in ("telegram", "sms", "webhook"):
        raise HTTPException(400, "channel must be telegram, sms or webhook")
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) AS c FROM recipients").fetchone()["c"]
    rid = f"R{count + 1}"
    while conn.execute("SELECT 1 FROM recipients WHERE id=?", (rid,)).fetchone():
        count += 1
        rid = f"R{count + 1}"
    conn.execute(
        "INSERT INTO recipients (id, name, org, channel, target, active) VALUES (?,?,?,?,?,1)",
        (rid, body.name, body.org, body.channel, body.target),
    )
    conn.commit()
    conn.close()
    return Recipient(id=rid, name=body.name, org=body.org, channel=body.channel, target=body.target, active=True)


@app.post("/api/recipients/{rid}/toggle", response_model=Recipient)
def toggle_recipient(rid: str):
    conn = get_db()
    r = conn.execute("SELECT * FROM recipients WHERE id=?", (rid,)).fetchone()
    if not r:
        conn.close()
        raise HTTPException(404, "Recipient not found")
    new_state = 0 if r["active"] else 1
    conn.execute("UPDATE recipients SET active=? WHERE id=?", (new_state, rid))
    conn.commit()
    conn.close()
    return Recipient(id=r["id"], name=r["name"], org=r["org"] or "",
                     channel=r["channel"], target=r["target"], active=bool(new_state))


@app.delete("/api/recipients/{rid}")
def delete_recipient(rid: str):
    conn = get_db()
    conn.execute("DELETE FROM recipients WHERE id=?", (rid,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/alerts", response_model=List[Alert])
def list_alerts(limit: int = 30, site_id: Optional[str] = None):
    conn = get_db()
    if site_id:
        rows = conn.execute(
            "SELECT * FROM alerts WHERE site_id=? ORDER BY id DESC LIMIT ?", (site_id, limit)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [_alert_row(r) for r in rows]


@app.post("/api/alerts/{alert_id}/ack", response_model=Alert)
def acknowledge_alert(alert_id: int):
    conn = get_db()
    conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))
    conn.commit()
    row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Alert not found")
    return _alert_row(row)


@app.post("/api/alerts/test/{site_id}", response_model=Alert)
async def test_alert(site_id: str):
    conn = get_db()
    site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    if not site:
        conn.close()
        raise HTTPException(404, "Unknown site")
    recipients = [dict(r) for r in conn.execute("SELECT * FROM recipients WHERE active=1").fetchall()]
    alert_id = await _raise_alert(conn, site, "Critical", ["manual drill / test dispatch"], recipients)
    row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    conn.close()
    return _alert_row(row)


@app.post("/api/alerts/evaluate")
async def force_evaluate():
    created = await evaluate_alerts()
    return {"created": created, "count": len(created)}


@app.post("/api/ingest/{site_id}")
async def ingest_reading(site_id: str, flow: float, debris: float, flood: float, solar: float):
    conn = get_db()
    site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    if not site:
        conn.close()
        raise HTTPException(404, "Unknown site")
    score = composite_score(flow, debris, flood, solar)
    conn.execute(
        "UPDATE sites SET flow=?, debris=?, flood=?, solar=? WHERE id=?",
        (flow, debris, flood, solar, site_id),
    )
    conn.execute(
        "INSERT INTO readings (site_id, ts, flow, debris, flood, solar, score) VALUES (?,?,?,?,?,?,?)",
        (site_id, datetime.now(timezone.utc).isoformat(), flow, debris, flood, solar, score),
    )
    conn.commit()
    conn.close()
    created = await evaluate_alerts()
    return {"ok": True, "score": score, "alerts_created": created}