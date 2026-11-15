"""
NetZero Wave backend (FastAPI)
==============================
Site-selection scoring, live unit monitoring, GIS layer and automated Telegram / SMS /
webhook alerting for the Calumpang River watershed (Batangas).

Structure
  settings.py  environment configuration (SIMULATION_MODE is the master safety switch)
  auth.py      users, roles, tokens, device keys, audit log, rate limiting
  engine.py    scoring, SQLite schema, alert engine, outbox, background jobs
  alerts.py    Telegram / SMS (Vonage) / webhook senders
  main.py      HTTP API (this file)

Run (demo):   pip install -r requirements.txt && uvicorn main:app --reload
Run (live):   see README.md  ->  SIMULATION_MODE=false plus SECRET_KEY and ADMIN_PASSWORD
"""

import asyncio
import json
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

import alerts as alert_service
import auth
import engine
import settings
from engine import RIVER_PATHS, get_db, init_db, read_config, startup_report, warn_if_legacy_db


# ---------------------------------------------------------------
# App, middleware, lifecycle
# ---------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    warn_if_legacy_db()
    startup_report()
    task = asyncio.create_task(engine.background_loop()) if settings.RUN_BACKGROUND else None
    yield
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


_docs = settings.SIMULATION_MODE  # interactive API docs only in demo mode
app = FastAPI(
    title="NetZero Wave API",
    lifespan=lifespan,
    docs_url="/docs" if _docs else None,
    redoc_url=None,
    openapi_url="/openapi.json" if _docs else None,
)

if settings.CORS_ORIGINS:  # same-origin needs no CORS; list origins explicitly if you split front and back end
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Authorization", "Content-Type", "X-Device-Key"],
    )


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "same-origin")
    if request.url.path.startswith("/api/"):
        resp.headers.setdefault("Cache-Control", "no-store")
    return resp


@app.get("/", include_in_schema=False)
def read_root():
    return FileResponse(settings.BASE_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/healthz", include_in_schema=False)
def healthz():
    try:
        conn = get_db()
        conn.execute("SELECT 1")
        conn.close()
    except Exception:
        raise HTTPException(503, "database unavailable")
    return {"ok": True, "simulation": settings.SIMULATION_MODE}


@app.get("/api/meta")
def meta():
    """Public: lets the page show the DEMO DATA banner before anyone logs in."""
    return {"name": "NetZero Wave", "simulation": settings.SIMULATION_MODE}


# ---------------------------------------------------------------
# Models
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
    status: str = "Low"          # Low | Moderate | High  (the alert engine's confirmed state)
    deployed: bool = False
    offline: bool = False        # deployed buoy that stopped reporting
    last_seen: Optional[str] = None


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
    tier: int = 1                # 1 = first responders, 2 = escalation if unacknowledged


class RecipientIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    org: str = Field(default="", max_length=80)
    channel: str = Field(description="telegram | sms | webhook")
    target: str = Field(min_length=1, max_length=200)
    tier: int = Field(default=1, ge=1, le=2)


class Alert(BaseModel):
    id: int
    site_id: str
    site_name: str
    ts: str
    severity: str                # Warning | Critical | Clear | Offline
    flow: float
    flood: float
    reasons: List[str]
    message: str
    deliveries: List[Dict[str, Any]]
    acknowledged: bool
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None
    escalated: bool = False
    drill: bool = False


class ReadingIn(BaseModel):
    """One buoy reading. Out-of-range values are rejected, never stored."""
    flow: float = Field(ge=0, le=15, allow_inf_nan=False, description="surface velocity, m/s")
    debris: float = Field(ge=0, le=100, allow_inf_nan=False)
    flood: float = Field(ge=0, le=100, allow_inf_nan=False, description="flood-proximity index 0-100")
    solar: float = Field(ge=0, le=100, allow_inf_nan=False)


class LoginIn(BaseModel):
    username: str = Field(max_length=64)
    password: str = Field(max_length=256)


class PasswordIn(BaseModel):
    current: str = Field(max_length=256)
    new: str = Field(min_length=auth.MIN_PASSWORD, max_length=256)


class UserIn(BaseModel):
    username: str
    password: str = Field(min_length=auth.MIN_PASSWORD, max_length=256)
    role: str


# ---------------------------------------------------------------
# Authentication / authorisation
# ---------------------------------------------------------------
_bearer = HTTPBearer(auto_error=False)
_login_limiter = auth.RateLimiter(5, 300)   # 5 failures / 5 min per (ip, username)  [per worker]
_drill_limiter = auth.RateLimiter(3, 60)    # 3 drills / minute per user             [per worker]


def current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer)) -> Dict[str, str]:
    unauth = {"WWW-Authenticate": "Bearer"}
    if not creds:
        raise HTTPException(401, "Not authenticated", headers=unauth)
    claims = auth.read_token(creds.credentials)
    if not claims:
        raise HTTPException(401, "Session expired or invalid. Sign in again.", headers=unauth)
    conn = get_db()
    row = conn.execute("SELECT username, role, active FROM users WHERE username=?", (claims["sub"],)).fetchone()
    conn.close()
    if not row or not row["active"]:  # disabled users lose access immediately; the DB role is authoritative
        raise HTTPException(401, "Account disabled", headers=unauth)
    return {"username": row["username"], "role": row["role"]}


def require(role: str):
    need = auth.ROLES[role]

    def dep(user: Dict[str, str] = Depends(current_user)) -> Dict[str, str]:
        if auth.ROLES.get(user["role"], 0) < need:
            raise HTTPException(403, f"This action needs the {role} role")
        return user

    return dep


@app.post("/api/auth/login")
def login(body: LoginIn, request: Request):
    ip = request.client.host if request.client else "?"
    key = (ip, body.username.strip().lower())
    if _login_limiter.blocked(key):
        raise HTTPException(429, "Too many failed attempts. Try again in a few minutes.")
    conn = get_db()
    try:
        who = auth.authenticate(conn, body.username, body.password)
        if not who:
            _login_limiter.record(key)
            auth.audit(conn, (body.username.strip().lower() or "?")[:32], "auth.login_failed", ip)
            conn.commit()
            raise HTTPException(401, "Incorrect username or password")
        _login_limiter.reset(key)
        auth.audit(conn, who["username"], "auth.login", ip)
        conn.commit()
    finally:
        conn.close()
    return {
        "token": auth.make_token(who["username"], who["role"]),
        "username": who["username"],
        "role": who["role"],
        "expires_in": settings.TOKEN_TTL_HOURS * 3600,
    }


@app.get("/api/auth/me")
def me(user: Dict[str, str] = Depends(current_user)):
    return user


@app.post("/api/auth/password")
def change_password(body: PasswordIn, user: Dict[str, str] = Depends(current_user)):
    conn = get_db()
    try:
        if not auth.authenticate(conn, user["username"], body.current):
            raise HTTPException(403, "Current password is incorrect")
        conn.execute("UPDATE users SET pw_hash=? WHERE username=?", (auth.hash_password(body.new), user["username"]))
        auth.audit(conn, user["username"], "auth.password_changed")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.get("/api/users")
def list_users(user: Dict[str, str] = Depends(require("admin"))):
    conn = get_db()
    rows = conn.execute("SELECT username, role, active, created_at FROM users ORDER BY created_at").fetchall()
    conn.close()
    return [{**dict(r), "active": bool(r["active"])} for r in rows]


@app.post("/api/users")
def create_user(body: UserIn, user: Dict[str, str] = Depends(require("admin"))):
    name = body.username.strip().lower()
    if not auth.USERNAME_RE.match(name):
        raise HTTPException(400, "Username: 3-32 characters, a-z 0-9 . _ -")
    if body.role not in auth.ROLES:
        raise HTTPException(400, f"Role must be one of: {', '.join(auth.ROLES)}")
    conn = get_db()
    try:
        if conn.execute("SELECT 1 FROM users WHERE username=?", (name,)).fetchone():
            raise HTTPException(409, "That username already exists")
        conn.execute(
            "INSERT INTO users (username, pw_hash, role, active, created_at) VALUES (?,?,?,1,?)",
            (name, auth.hash_password(body.password), body.role, datetime.now(timezone.utc).isoformat()),
        )
        auth.audit(conn, user["username"], "user.create", f"{name} ({body.role})")
        conn.commit()
    finally:
        conn.close()
    return {"username": name, "role": body.role, "active": True}


@app.delete("/api/users/{username}")
def disable_user(username: str, user: Dict[str, str] = Depends(require("admin"))):
    username = username.strip().lower()
    if username == user["username"]:
        raise HTTPException(400, "You can't disable your own account")
    conn = get_db()
    try:
        n = conn.execute("UPDATE users SET active=0 WHERE username=?", (username,)).rowcount
        if not n:
            raise HTTPException(404, "No such user")
        auth.audit(conn, user["username"], "user.disable", username)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/sites/{site_id}/device-key")
def create_device_key(site_id: str, user: Dict[str, str] = Depends(require("admin"))):
    """Issue (or rotate) the key one buoy uses to post readings. Shown once."""
    conn = get_db()
    try:
        if not conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone():
            raise HTTPException(404, "Unknown site")
        key = auth.set_device_key(conn, site_id)
        auth.audit(conn, user["username"], "device.key_issued", site_id)
        conn.commit()
    finally:
        conn.close()
    return {"site_id": site_id, "device_key": key, "note": "Shown once. Send it as the X-Device-Key header."}


@app.get("/api/audit")
def list_audit(limit: int = Query(100, ge=1, le=500), user: Dict[str, str] = Depends(require("admin"))):
    conn = get_db()
    rows = conn.execute("SELECT ts, actor, action, detail FROM audit ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _unit_response(u, site) -> Unit:
    return Unit(
        id=u["id"],
        site_id=u["site_id"],
        site_name=site["name"],
        deployed_at=u["deployed_at"],
        flow=site["flow"],
        flood_alert=engine.site_risk(site["id"]),  # same confirmed state the alert engine uses
        plastic_load=u["plastic_load"],
        bales=u["bales"],
        lat=site["lat"],
        lon=site["lon"],
    )


# ---------------------------------------------------------------
# Site & unit routes
# ---------------------------------------------------------------
@app.get("/api/sites", response_model=List[Site])
def list_sites(user: Dict[str, str] = Depends(require("viewer"))):
    """All candidate sites with their latest reading, ranked by score."""
    conn = get_db()
    rows = conn.execute(
        "SELECT s.*, st.severity AS sev, COALESCE(st.offline, 0) AS offline "
        "FROM sites s LEFT JOIN site_state st ON st.site_id = s.id"
    ).fetchall()
    deployed = {r["site_id"] for r in conn.execute("SELECT site_id FROM units WHERE active=1").fetchall()}
    conn.close()
    sites = [
        Site(
            id=r["id"], name=r["name"], flow=r["flow"], debris=r["debris"], flood=r["flood"], solar=r["solar"],
            score=engine.composite_score(r["flow"], r["debris"], r["flood"], r["solar"]),
            lat=r["lat"], lon=r["lon"], barangay=r["barangay"],
            status=engine.RISK[r["sev"]], deployed=r["id"] in deployed,
            offline=bool(r["offline"]), last_seen=r["last_seen"],
        )
        for r in rows
    ]
    return sorted(sites, key=lambda s: s.score, reverse=True)


@app.get("/api/geo/rivers")
def river_geometry(user: Dict[str, str] = Depends(require("viewer"))):
    """GeoJSON river centreline(s) of the Calumpang watershed for the map.

    Serves RIVER_GEOJSON when that file exists (real geometry); otherwise
    falls back to the schematic line in RIVER_PATHS, flagged `approximate`.
    """
    if settings.RIVER_GEOJSON.exists():
        return json.loads(settings.RIVER_GEOJSON.read_text(encoding="utf-8"))
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": name, "approximate": True},
                "geometry": {
                    "type": "LineString",
                    # GeoJSON is [lon, lat]
                    "coordinates": [[lon, lat] for lat, lon in path],
                },
            }
            for name, path in RIVER_PATHS.items()
        ],
    }


@app.get("/api/sites/{site_id}/history")
def site_history(site_id: str, limit: int = 50, user: Dict[str, str] = Depends(require("viewer"))):
    """Recent raw readings for one site — handy for charting a trend."""
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
def deploy_site(site_id: str, user: Dict[str, str] = Depends(require("operator"))):
    """Deploy a storm baler unit to a candidate site (idempotent if already deployed)."""
    conn = get_db()
    site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
    if not site:
        conn.close()
        raise HTTPException(404, "Unknown site")

    existing = conn.execute(
        "SELECT * FROM units WHERE site_id=? AND active=1", (site_id,)
    ).fetchone()
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
    auth.audit(conn, user["username"], "unit.deploy", f"{unit_id} at {site_id}")
    conn.commit()
    row = conn.execute("SELECT * FROM units WHERE id=?", (unit_id,)).fetchone()
    conn.close()
    return _unit_response(row, site)


@app.get("/api/units", response_model=List[Unit])
def list_units(user: Dict[str, str] = Depends(require("viewer"))):
    """All currently deployed units with their latest live metrics."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM units WHERE active=1").fetchall()
    result = []
    for u in rows:
        site = conn.execute("SELECT * FROM sites WHERE id=?", (u["site_id"],)).fetchone()
        result.append(_unit_response(u, site))
    conn.close()
    return result


@app.get("/api/units/{unit_id}", response_model=Unit)
def get_unit(unit_id: str, user: Dict[str, str] = Depends(require("viewer"))):
    conn = get_db()
    u = conn.execute("SELECT * FROM units WHERE id=? AND active=1", (unit_id,)).fetchone()
    if not u:
        conn.close()
        raise HTTPException(404, "Unit not found or retired")
    site = conn.execute("SELECT * FROM sites WHERE id=?", (u["site_id"],)).fetchone()
    conn.close()
    return _unit_response(u, site)


@app.delete("/api/units/{unit_id}")
def retire_unit(unit_id: str, user: Dict[str, str] = Depends(require("operator"))):
    """Retire a deployed unit (e.g. for redeployment elsewhere)."""
    conn = get_db()
    conn.execute("UPDATE units SET active=0 WHERE id=?", (unit_id,))
    auth.audit(conn, user["username"], "unit.retire", unit_id)
    conn.commit()
    conn.close()
    return {"ok": True}


# ---------------------------------------------------------------
# Alerting routes
# ---------------------------------------------------------------
@app.get("/api/alert-config")
def get_alert_config(user: Dict[str, str] = Depends(require("viewer"))):
    """Read-only. Thresholds are safety constants in engine.py: no write verb exists."""
    return {
        **read_config(),
        "channels": alert_service.channel_status(),
        "simulation": settings.SIMULATION_MODE,
        "stale_after_seconds": settings.STALE_AFTER_SECONDS,
    }


@app.get("/api/recipients", response_model=List[Recipient])
def list_recipients(user: Dict[str, str] = Depends(require("operator"))):
    conn = get_db()
    rows = conn.execute("SELECT * FROM recipients ORDER BY tier, id").fetchall()
    conn.close()
    return [Recipient(**{**dict(r), "active": bool(r["active"]), "tier": r["tier"] or 1}) for r in rows]


@app.post("/api/recipients", response_model=Recipient)
def add_recipient(body: RecipientIn, user: Dict[str, str] = Depends(require("operator"))):
    channel, target, name = body.channel.strip().lower(), body.target.strip(), body.name.strip()
    if channel not in ("telegram", "sms", "webhook"):
        raise HTTPException(400, "Channel must be telegram, sms or webhook")
    if not name:
        raise HTTPException(400, "Name is required")
    problem = engine.validate_target(channel, target)
    if problem:
        raise HTTPException(400, problem)
    conn = get_db()
    try:
        n = conn.execute("SELECT COUNT(*) AS c FROM recipients").fetchone()["c"] + 1
        rid = f"R{n}"
        while conn.execute("SELECT 1 FROM recipients WHERE id=?", (rid,)).fetchone():
            n += 1
            rid = f"R{n}"
        conn.execute(
            "INSERT INTO recipients (id, name, org, channel, target, active, tier) VALUES (?,?,?,?,?,1,?)",
            (rid, name, body.org.strip(), channel, target, body.tier),
        )
        auth.audit(conn, user["username"], "recipient.add", f"{rid} {channel} tier {body.tier}")
        conn.commit()
    finally:
        conn.close()
    return Recipient(id=rid, name=name, org=body.org.strip(), channel=channel, target=target, active=True, tier=body.tier)


@app.post("/api/recipients/{rid}/toggle", response_model=Recipient)
def toggle_recipient(rid: str, user: Dict[str, str] = Depends(require("operator"))):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM recipients WHERE id=?", (rid,)).fetchone()
        if not row:
            raise HTTPException(404, "Recipient not found")
        new = 0 if row["active"] else 1
        conn.execute("UPDATE recipients SET active=? WHERE id=?", (new, rid))
        auth.audit(conn, user["username"], "recipient.toggle", f"{rid} -> {'on' if new else 'muted'}")
        conn.commit()
    finally:
        conn.close()
    return Recipient(**{**dict(row), "active": bool(new), "tier": row["tier"] or 1})


@app.delete("/api/recipients/{rid}")
def delete_recipient(rid: str, user: Dict[str, str] = Depends(require("admin"))):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM recipients WHERE id=?", (rid,)).fetchone()
        if not row:
            raise HTTPException(404, "Recipient not found")
        conn.execute("DELETE FROM recipients WHERE id=?", (rid,))
        auth.audit(conn, user["username"], "recipient.delete", f"{rid} {row['channel']} ({row['name']})")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


def _alert_model(conn, r, reveal: bool) -> Alert:
    return Alert(
        id=r["id"], site_id=r["site_id"], site_name=r["site_name"], ts=r["ts"], severity=r["severity"],
        flow=r["flow"], flood=r["flood"], reasons=json.loads(r["reasons"] or "[]"), message=r["message"] or "",
        deliveries=engine.deliveries_for(conn, r["id"], r["deliveries"], reveal),
        acknowledged=bool(r["acknowledged"]), acknowledged_by=r["acknowledged_by"],
        acknowledged_at=r["acknowledged_at"], escalated=bool(r["escalated"]), drill=bool(r["drill"]),
    )


@app.get("/api/alerts", response_model=List[Alert])
def list_alerts(
    limit: int = Query(30, ge=1, le=200),
    site_id: Optional[str] = None,
    user: Dict[str, str] = Depends(require("viewer")),
):
    """Alert log with per-recipient delivery status. Phone numbers / chat ids are masked for viewers."""
    reveal = auth.ROLES[user["role"]] >= auth.ROLES["operator"]
    conn = get_db()
    try:
        if site_id:
            rows = conn.execute("SELECT * FROM alerts WHERE site_id=? ORDER BY id DESC LIMIT ?", (site_id, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [_alert_model(conn, r, reveal) for r in rows]
    finally:
        conn.close()


@app.post("/api/alerts/{alert_id}/ack", response_model=Alert)
def acknowledge_alert(alert_id: int, user: Dict[str, str] = Depends(require("operator"))):
    conn = get_db()
    try:
        row = engine.acknowledge(conn, alert_id, user["username"])
        if not row:
            raise HTTPException(404, "Alert not found")
        return _alert_model(conn, row, True)
    finally:
        conn.close()


@app.post("/api/alerts/test/{site_id}", response_model=Alert)
def test_alert(site_id: str, user: Dict[str, str] = Depends(require("operator"))):
    """Labelled DRILL dispatch to every active recipient (rate-limited: 3 per minute per user)."""
    if not _drill_limiter.allow(user["username"]):
        raise HTTPException(429, "Drill limit reached (3 per minute). Wait a moment.")
    aid = engine.raise_drill(site_id, user["username"])
    if aid is None:
        raise HTTPException(404, "Unknown site")
    conn = get_db()
    try:
        return _alert_model(conn, conn.execute("SELECT * FROM alerts WHERE id=?", (aid,)).fetchone(), True)
    finally:
        conn.close()


@app.post("/api/alerts/evaluate")
def force_evaluate(user: Dict[str, str] = Depends(require("operator"))):
    """Run the threshold check now (it also runs every few seconds in the background)."""
    created = engine.evaluate_alerts()
    return {"created": len(created), "alert_ids": created}


# ---------------------------------------------------------------
# Real hardware ingestion
# ---------------------------------------------------------------
@app.post("/api/ingest/{site_id}")
def ingest_reading(site_id: str, body: ReadingIn, x_device_key: Optional[str] = Header(default=None)):
    """Buoy -> server. JSON body {flow, debris, flood, solar} plus header X-Device-Key.
    Stores the reading, then runs the alert engine (nothing else changes when you swap
    the simulator for real hardware)."""
    conn = get_db()
    try:
        if not auth.check_device_key(conn, site_id, x_device_key):
            raise HTTPException(401, "Invalid or missing device key")  # same answer for unknown site / bad key
        score = engine.record_reading(conn, site_id, body.flow, body.debris, body.flood, body.solar)
        conn.commit()
    finally:
        conn.close()
    created = engine.evaluate_alerts()
    return {"ok": True, "score": score, "alerts_created": created}
