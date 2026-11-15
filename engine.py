"""
NetZero Wave core logic (no web-framework imports, so it is easy to unit-test).

Scoring model, SQLite schema, the alert engine and the notification outbox.

Alert pipeline (all of it runs inside one write transaction, so two workers can
never double-alert):

  reading -> confirm (N of last M readings) -> hysteresis (Schmitt trigger)
          -> state change? -> alert row + outbox rows (one per recipient)
  outbox  -> sent by the background leader with retry/backoff, never inline
  extras  -> reminders while unacknowledged, ALL CLEAR on recovery, sensor
             OFFLINE detection, tier-2 escalation if nobody acknowledges
"""

import asyncio
import json
import os
import random
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import alerts as alert_service
import auth
import settings

DB_PATH = settings.DB_PATH
MAX_ATTEMPTS = 5


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------
# Scoring model (unchanged)
# ---------------------------------------------------------------
# ---------------------------------------------------------------
# Scoring model — mirrors the frontend prototype's weights/curve.
# ---------------------------------------------------------------
WEIGHTS = {"flow": 0.30, "debris": 0.30, "flood": 0.25, "solar": 0.15}


def flow_score(v: float) -> float:
    """Rewards flow velocity inside the 0.5-3.0 m/s ideal band."""
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


# ---------------------------------------------------------------
# Seed data — Calumpang River watershed, Batangas
# ---------------------------------------------------------------
# NOTE: coordinates are municipal / poblacion centres (PhilAtlas), NOT
# surveyed buoy positions, and the flow / debris / flood / solar values
# are illustrative starting points for the simulator, not measurements.
# Once real deployment spots on the river are chosen, replace the lat/lon
# here (or UPDATE the sites table) and feed real readings via /api/ingest.
# id, name, flow, debris, flood, solar, lat, lon, barangay/LGU
SEED_SITES = [
    ("S1", "Calumpang Watershed — Batangas City", 0.9, 80, 78, 74,
     13.75650, 121.05830, "Poblacion area, Batangas City"),
    ("S2", "Calumpang Watershed — Ibaan", 1.4, 62, 64, 78,
     13.81860, 121.13470, "Poblacion area, Ibaan"),
    ("S3", "Calumpang Watershed — Taysan", 1.8, 55, 58, 82,
     13.79680, 121.18850, "Poblacion area, Taysan"),
    ("S4", "Calumpang Watershed — Rosario", 2.1, 60, 52, 85,
     13.84600, 121.20600, "Poblacion area, Rosario"),
    ("S5", "Calumpang Watershed — Padre Garcia", 1.6, 48, 40, 83,
     13.87750, 121.21500, "Poblacion area, Padre Garcia"),
    ("S6", "Calumpang Watershed — San Jose", 1.3, 52, 45, 80,
     13.88020, 121.10190, "Poblacion area, San Jose"),
    ("S7", "Calumpang Watershed — Cuenca", 1.1, 40, 38, 76,
     13.90150, 121.04920, "Poblacion area, Cuenca"),
    ("S8", "Calumpang Watershed — Lipa City", 1.5, 68, 50, 81,
     13.94110, 121.16310, "Poblacion area, Lipa City"),
]

# SCHEMATIC main stem of the Calumpang River, source (Rosario) → mouth
# (Batangas Bay). It only links known anchor points in the order the river
# is described (Rosario → Taysan → Ibaan → Batangas City → bay); it is NOT
# the surveyed channel. Drop a real GeoJSON at RIVER_GEOJSON to replace it.
RIVER_PATHS = {
    "Calumpang River (schematic)": [
        [13.84600, 121.20600],   # Rosario — listed source area
        [13.79680, 121.18850],   # Taysan
        [13.81860, 121.13470],   # Ibaan
        [13.75650, 121.05830],   # Batangas City
        [13.73694, 121.05820],   # mouth at Batangas Bay
    ],
}

# PLACEHOLDER targets. They are seeded ONLY in SIMULATION_MODE (so the demo has
# someone to "notify"). In live mode nothing is seeded, and on startup any
# recipient still using one of these placeholder targets is deactivated so a
# real SMS can never go to a stranger's phone number.
# id, name, org, channel, target, tier  (tier 1 = first responders, 2 = escalation)
SEED_RECIPIENTS = [
    ("R1", "Batangas City CDRRMO Operations", "Batangas City DRRMO", "telegram", "-1001234567890", 1),
    ("R2", "Lipa City CDRRMO Operations", "Lipa City DRRMO", "telegram", "-1009876543210", 1),
    ("R3", "Batangas PDRRMO Operations", "Provincial DRRMO Batangas", "sms", "+639171234567", 2),
]
PLACEHOLDER_TARGETS = {r[4] for r in SEED_RECIPIENTS}


# ---------------------------------------------------------------
# Alert thresholds — LOCKED.
#
# These are engineering safety constants, not user preferences. They
# are deliberately defined in code and exposed read-only over the API
# so that no dashboard operator can raise a threshold (and silence a
# real flood warning) during an event. Changing them is a code change
# that goes through review and redeployment.
#
# Calibration basis: inherited from the Pasig–Marikina prototype (MMDA/EFCOS
# alarm practice and the 0.5-3.0 m/s working band used by the siting model
# above). NOT yet re-calibrated for the Calumpang watershed — confirm with the
# LGU DRRMOs / PAGASA before live use.
# ---------------------------------------------------------------
THRESHOLDS = {
    "enabled": True,
    "flood_critical": 85.0,      # flood-proximity index 0-100
    "flood_warning": 72.0,
    "flow_critical": 3.0,        # m/s
    "flow_warning": 2.6,
    "cooldown_minutes": 10,      # reminder interval while an alert stays unacknowledged
    "locked": True,
    # Confirmation, hysteresis and escalation: safety constants like the limits above.
    "confirm_n": 2,              # a level must show in N of the last M readings...
    "confirm_m": 3,              # ...before it alerts (one noisy spike pages nobody)
    "clear_margin_flood": 3.0,   # a level is released only once readings fall this far below it
    "clear_margin_flow": 0.15,   # m/s, same idea for flow
    "escalate_after_minutes": 15,  # unacknowledged Warning/Critical goes to tier-2 recipients
}


# ---------------------------------------------------------------
# Database
# ---------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)  # waits up to 10 s on a locked DB instead of failing
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
    cols = {r["name"] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        except sqlite3.OperationalError:  # another worker added it a moment ago
            pass


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")  # readers no longer block the writer
    cur.execute(
        """CREATE TABLE IF NOT EXISTS sites (
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            flow REAL, debris REAL, flood REAL, solar REAL,
            lat REAL, lon REAL, barangay TEXT, last_seen TEXT)"""
    )
    for col, ddl in (("lat", "REAL"), ("lon", "REAL"), ("barangay", "TEXT"), ("last_seen", "TEXT")):
        _ensure_column(cur, "sites", col, ddl)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, site_id TEXT NOT NULL, ts TEXT NOT NULL,
            flow REAL, debris REAL, flood REAL, solar REAL, score INTEGER)"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS units (
            id TEXT PRIMARY KEY, site_id TEXT NOT NULL, deployed_at TEXT NOT NULL,
            plastic_load REAL, bales INTEGER, active INTEGER DEFAULT 1)"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS recipients (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, org TEXT,
            channel TEXT NOT NULL,      -- telegram | sms | webhook
            target TEXT NOT NULL,       -- chat id | phone number | label (webhook URL comes from env)
            active INTEGER DEFAULT 1, tier INTEGER DEFAULT 1)"""
    )
    _ensure_column(cur, "recipients", "tier", "INTEGER DEFAULT 1")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, site_id TEXT NOT NULL, site_name TEXT NOT NULL,
            ts TEXT NOT NULL,
            severity TEXT NOT NULL,     -- Warning | Critical | Clear | Offline
            flow REAL, flood REAL, reasons TEXT, message TEXT,
            deliveries TEXT,            -- legacy JSON; deliveries now live in `outbox`
            acknowledged INTEGER DEFAULT 0, acknowledged_by TEXT, acknowledged_at TEXT,
            escalated INTEGER DEFAULT 0, drill INTEGER DEFAULT 0)"""
    )
    for col, ddl in (
        ("acknowledged_by", "TEXT"), ("acknowledged_at", "TEXT"),
        ("escalated", "INTEGER DEFAULT 0"), ("drill", "INTEGER DEFAULT 0"),
    ):
        _ensure_column(cur, "alerts", col, ddl)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT, alert_id INTEGER NOT NULL, recipient_id TEXT,
            name TEXT, org TEXT, channel TEXT NOT NULL, target TEXT NOT NULL,
            text TEXT, payload TEXT,
            status TEXT NOT NULL DEFAULT 'pending',   -- pending | sending | sent | simulated | failed
            detail TEXT DEFAULT '', attempts INTEGER DEFAULT 0, next_try TEXT NOT NULL)"""
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS site_state (site_id TEXT PRIMARY KEY, severity TEXT, "
        "last_alert_ts TEXT, offline INTEGER DEFAULT 0)"
    )
    cur.execute("CREATE TABLE IF NOT EXISTS leader (name TEXT PRIMARY KEY, holder TEXT NOT NULL, expires REAL NOT NULL)")
    for ddl in (
        "CREATE INDEX IF NOT EXISTS idx_readings_site ON readings(site_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings(ts)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_site ON alerts(site_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_try)",
        "CREATE INDEX IF NOT EXISTS idx_outbox_alert ON outbox(alert_id)",
    ):
        cur.execute(ddl)
    auth.init_tables(cur)

    if cur.execute("SELECT COUNT(*) AS c FROM sites").fetchone()["c"] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO sites (id, name, flow, debris, flood, solar, lat, lon, barangay) VALUES (?,?,?,?,?,?,?,?,?)",
            SEED_SITES,
        )
    else:  # backfill coordinates for pre-existing rows
        for s in SEED_SITES:
            cur.execute(
                "UPDATE sites SET lat=COALESCE(lat,?), lon=COALESCE(lon,?), barangay=COALESCE(barangay,?) WHERE id=?",
                (s[6], s[7], s[8], s[0]),
            )

    if settings.SIMULATION_MODE and cur.execute("SELECT COUNT(*) AS c FROM recipients").fetchone()["c"] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO recipients (id, name, org, channel, target, active, tier) VALUES (?,?,?,?,?,1,?)",
            [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in SEED_RECIPIENTS],
        )
    if not settings.SIMULATION_MODE:
        marks = ",".join("?" * len(PLACEHOLDER_TARGETS))
        n = cur.execute(
            f"UPDATE recipients SET active=0 WHERE active=1 AND target IN ({marks})", tuple(PLACEHOLDER_TARGETS)
        ).rowcount
        if n:
            print(f"[startup] Deactivated {n} recipient(s) that still use placeholder targets. Add real ones in the Alerts tab.")

    cur.execute("DROP TABLE IF EXISTS config")  # thresholds are locked in code now
    conn.commit()
    auth.bootstrap_admin(conn)
    conn.commit()
    conn.close()


def startup_report() -> None:
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) AS c FROM recipients WHERE active=1").fetchone()["c"]
    conn.close()
    mode = "SIMULATION (demo data, all notifications dry-run)" if settings.SIMULATION_MODE else "LIVE"
    print(f"[startup] mode: {mode}; channels: {alert_service.channel_status()}; active recipients: {n}")
    if not settings.SIMULATION_MODE and n == 0:
        print("[startup] WARNING: no active recipients. Alerts will be recorded but nobody will be notified.")


def warn_if_legacy_db() -> None:
    """init_db only seeds EMPTY tables, so a database created for the old
    Pasig–Marikina prototype keeps its old sites and recipients. Say so loudly
    instead of quietly mixing two geographies (and alerting the wrong DRRMO)."""
    conn = get_db()
    known = {(s[0], s[1]) for s in SEED_SITES}
    existing = {(r["id"], r["name"]) for r in conn.execute("SELECT id, name FROM sites").fetchall()}
    conn.close()
    if existing - known:
        print(
            "[startup] WARNING: storm_baler.db contains sites that are not in the "
            "Calumpang seed list (probably the old Pasig–Marikina data). Stop the "
            f"server and delete {DB_PATH} to reseed sites AND recipients."
        )


def read_config() -> Dict[str, Any]:
    """Return the locked thresholds. Read-only by design — there is no
    setter anywhere in the API surface."""
    return dict(THRESHOLDS)


# ---------------------------------------------------------------
# Classification: ONE definition of risk, used by alerts, the sites list and units
# ---------------------------------------------------------------
RANK = {None: 0, "Warning": 1, "Critical": 2}
RISK = {"Critical": "High", "Warning": "Moderate", None: "Low"}  # severity -> dashboard label


def classify(flow: float, flood: float, cfg: Dict[str, Any], prev: Optional[str] = None) -> Tuple[Optional[str], List[str]]:
    """Severity for one reading, with hysteresis.

    `prev` is the severity already active at the site. While a level is active, a
    reading must drop `clear_margin_*` BELOW that level's threshold before it is
    released, so a value hovering on the line cannot flap alert/clear/alert."""
    mf, mv = cfg["clear_margin_flood"], cfg["clear_margin_flow"]
    held_warn = prev in ("Warning", "Critical")
    crit_flood = cfg["flood_critical"] - (mf if prev == "Critical" else 0.0)
    crit_flow = cfg["flow_critical"] - (mv if prev == "Critical" else 0.0)
    warn_flood = cfg["flood_warning"] - (mf if held_warn else 0.0)
    warn_flow = cfg["flow_warning"] - (mv if held_warn else 0.0)

    reasons: List[str] = []
    severity = None
    if flood >= crit_flood:
        severity = "Critical"
        reasons.append(f"flood proximity {flood:.0f} >= critical {cfg['flood_critical']:.0f}")
    if flow >= crit_flow:
        severity = "Critical"
        reasons.append(f"flow velocity {flow:.2f} m/s >= critical {cfg['flow_critical']:.2f}")
    if severity is None:
        if flood >= warn_flood:
            severity = "Warning"
            reasons.append(f"flood proximity {flood:.0f} >= warning {cfg['flood_warning']:.0f}")
        if flow >= warn_flow:
            severity = "Warning"
            reasons.append(f"flow velocity {flow:.2f} m/s >= warning {cfg['flow_warning']:.2f}")
    return severity, reasons


def confirmed(window, cfg: Dict[str, Any], prev: Optional[str]) -> Tuple[Optional[str], List[str]]:
    """Highest severity seen in at least `confirm_n` of the last `confirm_m` readings
    (window is newest-first). One noisy spike therefore cannot page anybody."""
    n, m = cfg["confirm_n"], cfg["confirm_m"]
    window = list(window)[:m]
    if len(window) < n:
        return None, []
    results = [classify(r["flow"], r["flood"], cfg, prev) for r in window]
    for target in ("Critical", "Warning"):
        hits = [res for res in results if RANK[res[0]] >= RANK[target]]
        if len(hits) >= n:
            return target, hits[0][1]  # reasons from the newest qualifying reading
    return None, []


def site_risk(site_id: str) -> str:
    """Dashboard label (Low/Moderate/High) = the alert engine's confirmed state."""
    conn = get_db()
    row = conn.execute("SELECT severity FROM site_state WHERE site_id=?", (site_id,)).fetchone()
    conn.close()
    return RISK[row["severity"] if row else None]


def validate_target(channel: str, target: str) -> Optional[str]:
    """Return an error message, or None if the target looks right for the channel."""
    if channel == "telegram" and not re.fullmatch(r"-?\d{5,20}|@[A-Za-z0-9_]{5,32}", target):
        return "Telegram target must be a numeric chat id (e.g. -1001234567890) or @channelname"
    if channel == "sms" and not re.fullmatch(r"\+\d{8,15}", target):
        return "SMS target must be an international number like +639171234567"
    if channel == "webhook" and not (1 <= len(target) <= 200):
        return "Webhook target is just a label; the URL is set in GENERIC_WEBHOOK_URL"
    return None


def mask_target(channel: str, target: str) -> str:
    if channel == "webhook":
        return "webhook"
    return "\u2022\u2022\u2022" + target[-4 if channel == "telegram" else -3:]


# ---------------------------------------------------------------
# Alert engine
# ---------------------------------------------------------------
def _enqueue(conn, alert_id: int, recipients: List[Dict[str, Any]], text: str, sms_text: str,
             payload: Dict[str, Any], now: datetime) -> None:
    targets = list(recipients)
    if alert_service.GENERIC_WEBHOOK_URL and not any(r["channel"] == "webhook" for r in targets):
        targets.append({"id": "webhook", "name": "DRRMO webhook", "org": "Integration",
                        "channel": "webhook", "target": "GENERIC_WEBHOOK_URL"})
    for r in targets:
        conn.execute(
            "INSERT INTO outbox (alert_id, recipient_id, name, org, channel, target, text, payload, status, next_try) "
            "VALUES (?,?,?,?,?,?,?,?, 'pending', ?)",
            (alert_id, r["id"], r["name"], r.get("org") or "", r["channel"], r["target"],
             sms_text if r["channel"] == "sms" else text, json.dumps(payload), now.isoformat()),
        )


def _raise_alert(conn, s, severity: str, reasons: List[str], recipients: List[Dict[str, Any]],
                 now: datetime, drill: bool = False) -> int:
    lat, lon = s["lat"] or 0.0, s["lon"] or 0.0
    text = alert_service.build_message(
        site_name=s["name"], severity=severity, flow=s["flow"], flood=s["flood"], reasons=reasons,
        lat=lat, lon=lon, barangay=s["barangay"] or "-", drill=drill,
    )
    sms = alert_service.build_sms(s["name"], severity, s["flow"], s["flood"], lat, lon, drill=drill)
    ts = now.isoformat()
    payload = {
        "site_id": s["id"], "site_name": s["name"], "barangay": s["barangay"], "severity": severity,
        "flow_mps": round(s["flow"], 3), "flood_index": round(s["flood"], 1), "lat": s["lat"], "lon": s["lon"],
        "reasons": reasons, "ts": ts, "drill": drill, "message": text,
    }
    auto_ack = drill or severity == "Clear"  # informational: nothing for a human to acknowledge
    cur = conn.execute(
        "INSERT INTO alerts (site_id, site_name, ts, severity, flow, flood, reasons, message, deliveries, "
        "acknowledged, acknowledged_by, acknowledged_at, escalated, drill) VALUES (?,?,?,?,?,?,?,?, '[]', ?,?,?, 0, ?)",
        (s["id"], s["name"], ts, severity, s["flow"], s["flood"], json.dumps(reasons), text,
         1 if auto_ack else 0, ("drill" if drill else "system") if auto_ack else None, ts if auto_ack else None,
         1 if drill else 0),
    )
    alert_id = cur.lastrowid
    _enqueue(conn, alert_id, recipients, text, sms, payload, now)
    return alert_id


def _set_state(conn, site_id: str, severity: Optional[str], alert_ts: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO site_state (site_id, severity, last_alert_ts) VALUES (?,?,?) "
        "ON CONFLICT(site_id) DO UPDATE SET severity=excluded.severity, "
        "last_alert_ts=COALESCE(excluded.last_alert_ts, site_state.last_alert_ts)",
        (site_id, severity, alert_ts),
    )


def _clear_audience(conn, site_id: str, everyone: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ALL CLEAR goes to tier 1, plus tier 2 if this episode was escalated to them."""
    last_clear = conn.execute(
        "SELECT COALESCE(MAX(id),0) AS i FROM alerts WHERE site_id=? AND severity='Clear'", (site_id,)
    ).fetchone()["i"]
    esc = conn.execute(
        "SELECT 1 FROM alerts WHERE site_id=? AND escalated=1 AND drill=0 AND id>?", (site_id, last_clear)
    ).fetchone()
    return everyone if esc else [r for r in everyone if (r["tier"] or 1) <= 1]


def _evaluate_site(conn, s, cfg, everyone, now: datetime) -> List[int]:
    sid = s["id"]
    first = [r for r in everyone if (r["tier"] or 1) <= 1]
    st = conn.execute("SELECT severity, last_alert_ts FROM site_state WHERE site_id=?", (sid,)).fetchone()
    prev = st["severity"] if st else None
    window = conn.execute(
        "SELECT flow, flood FROM readings WHERE site_id=? ORDER BY id DESC LIMIT ?", (sid, cfg["confirm_m"])
    ).fetchall()
    if not window:
        return []
    cur, reasons = confirmed(window, cfg, prev)
    out: List[int] = []
    if RANK[cur] > RANK[prev]:  # new or worse
        out.append(_raise_alert(conn, s, cur, reasons, first, now))
        _set_state(conn, sid, cur, now.isoformat())
    elif cur and cur == prev:  # sustained: remind only while nobody has acknowledged
        last = conn.execute(
            "SELECT acknowledged FROM alerts WHERE site_id=? AND drill=0 AND severity IN ('Warning','Critical') "
            "ORDER BY id DESC LIMIT 1", (sid,),
        ).fetchone()
        last_ts = parse_ts(st["last_alert_ts"]) if st else None
        due = last_ts is None or now - last_ts >= timedelta(minutes=cfg["cooldown_minutes"])
        if last and not last["acknowledged"] and due:
            out.append(_raise_alert(conn, s, cur, ["reminder: still ongoing"] + reasons, first, now))
            _set_state(conn, sid, cur, now.isoformat())
    elif RANK[cur] < RANK[prev]:  # improved
        _set_state(conn, sid, cur)
        if cur is None:
            out.append(_raise_alert(conn, s, "Clear", ["readings back below thresholds"],
                                    _clear_audience(conn, sid, everyone), now))
    return out


def _check_offline(conn, first: List[Dict[str, Any]], now: datetime) -> List[int]:
    """A deployed buoy that goes quiet is NOT safe: absence of data must raise its own alert."""
    out: List[int] = []
    rows = conn.execute(
        "SELECT s.*, u.deployed_at AS deployed_at FROM sites s JOIN units u ON u.site_id=s.id AND u.active=1"
    ).fetchall()
    for r in rows:
        ref = max(t for t in (parse_ts(r["last_seen"]), parse_ts(r["deployed_at"])) if t)
        age = (now - ref).total_seconds()
        conn.execute("INSERT OR IGNORE INTO site_state (site_id) VALUES (?)", (r["id"],))
        was = conn.execute("SELECT offline FROM site_state WHERE site_id=?", (r["id"],)).fetchone()["offline"]
        if age > settings.STALE_AFTER_SECONDS and not was:
            out.append(_raise_alert(conn, r, "Offline", [f"no reading for {int(age // 60)} min {int(age % 60)} s"], first, now))
            conn.execute("UPDATE site_state SET offline=1 WHERE site_id=?", (r["id"],))
        elif age <= settings.STALE_AFTER_SECONDS and was:
            out.append(_raise_alert(conn, r, "Clear", ["sensor reporting again"], first, now))
            conn.execute("UPDATE site_state SET offline=0 WHERE site_id=?", (r["id"],))
    return out


def _escalate(conn, cfg, everyone: List[Dict[str, Any]], now: datetime) -> None:
    """Warning/Critical alerts nobody acknowledged within N minutes go to tier-2 recipients.
    At most ONE escalation per severity per episode: reminders never re-escalate."""
    cutoff = (now - timedelta(minutes=cfg["escalate_after_minutes"])).isoformat()
    rows = conn.execute(
        "SELECT * FROM alerts WHERE acknowledged=0 AND escalated=0 AND drill=0 AND severity IN ('Warning','Critical') "
        "AND ts <= ? "
        "AND EXISTS (SELECT 1 FROM site_state st WHERE st.site_id=alerts.site_id AND st.severity IS NOT NULL) "
        "AND NOT EXISTS (SELECT 1 FROM alerts e WHERE e.site_id=alerts.site_id AND e.severity=alerts.severity "
        "  AND e.escalated=1 AND e.drill=0 AND e.id > COALESCE("
        "    (SELECT MAX(c.id) FROM alerts c WHERE c.site_id=alerts.site_id AND c.severity='Clear'), 0)) "
        "ORDER BY id",
        (cutoff,),
    ).fetchall()
    tier2 = [r for r in everyone if (r["tier"] or 1) >= 2]
    done = set()
    for a in rows:
        key = (a["site_id"], a["severity"])
        if key in done:
            continue
        done.add(key)
        conn.execute("UPDATE alerts SET escalated=1 WHERE id=?", (a["id"],))
        if not tier2:
            print(f"[alerts] alert {a['id']} unacknowledged but there are no tier-2 recipients to escalate to")
            continue
        n = cfg["escalate_after_minutes"]
        text = f"[NetZero Wave] ESCALATION: no acknowledgement after {n} min\n\n{a['message']}"
        sms = f"NetZero Wave ESCALATION: {a['severity']} at {a['site_name']} not acknowledged for {n} min. Check the dashboard."
        payload = {"alert_id": a["id"], "escalation": True, "site_id": a["site_id"], "site_name": a["site_name"],
                   "severity": a["severity"], "ts": a["ts"], "message": text}
        _enqueue(conn, a["id"], tier2, text, sms, payload, now)


def evaluate_alerts(now: Optional[datetime] = None) -> List[int]:
    """Check every site; return ids of alerts created. Synchronous and transactional:
    the write lock is held for the whole pass, so concurrent callers serialise."""
    cfg = read_config()
    if not cfg["enabled"]:
        return []
    now = now or utcnow()
    conn = get_db()
    created: List[int] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        sites = conn.execute("SELECT * FROM sites").fetchall()
        everyone = [dict(r) for r in conn.execute("SELECT * FROM recipients WHERE active=1").fetchall()]
        first = [r for r in everyone if (r["tier"] or 1) <= 1]
        for s in sites:
            created += _evaluate_site(conn, s, cfg, everyone, now)
        created += _check_offline(conn, first, now)
        _escalate(conn, cfg, everyone, now)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return created


def raise_drill(site_id: str, actor: str) -> Optional[int]:
    """Test dispatch. Clearly labelled DRILL, goes to every active recipient, and is
    excluded from reminders, escalation and state tracking."""
    conn = get_db()
    try:
        s = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if not s:
            return None
        recipients = [dict(r) for r in conn.execute("SELECT * FROM recipients WHERE active=1").fetchall()]
        aid = _raise_alert(conn, s, "Critical", ["manual drill / test dispatch"], recipients, utcnow(), drill=True)
        auth.audit(conn, actor, "alert.drill", f"site {site_id} -> alert {aid}")
        conn.commit()
        return aid
    finally:
        conn.close()


def acknowledge(conn, alert_id: int, actor: str) -> Optional[sqlite3.Row]:
    """Acknowledge an alert AND earlier unacknowledged alerts of the same kind at that site
    (reminders pile up while nobody answers; one tap should clear the episode)."""
    row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    if not row:
        return None
    if not row["acknowledged"]:
        kinds = ("Offline",) if row["severity"] == "Offline" else ("Warning", "Critical")
        marks = ",".join("?" * len(kinds))
        conn.execute(
            f"UPDATE alerts SET acknowledged=1, acknowledged_by=?, acknowledged_at=? "
            f"WHERE site_id=? AND acknowledged=0 AND drill=0 AND id<=? AND severity IN ({marks})",
            (actor, utcnow().isoformat(), row["site_id"], alert_id, *kinds),
        )
        auth.audit(conn, actor, "alert.ack", f"alert {alert_id} ({row['site_name']})")
        conn.commit()
        row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    return row


def record_reading(conn, site_id: str, flow: float, debris: float, flood: float, solar: float) -> int:
    score = composite_score(flow, debris, flood, solar)
    ts = utcnow().isoformat()
    conn.execute("UPDATE sites SET flow=?, debris=?, flood=?, solar=?, last_seen=? WHERE id=?",
                 (flow, debris, flood, solar, ts, site_id))
    conn.execute("INSERT INTO readings (site_id, ts, flow, debris, flood, solar, score) VALUES (?,?,?,?,?,?,?)",
                 (site_id, ts, flow, debris, flood, solar, score))
    return score


def deliveries_for(conn, alert_id: int, legacy_json: Optional[str], reveal: bool) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT recipient_id, name, org, channel, target, status, detail, attempts FROM outbox WHERE alert_id=? ORDER BY id",
        (alert_id,),
    ).fetchall()
    out = [dict(r) for r in rows] if rows else json.loads(legacy_json or "[]")
    for d in out:
        if d.get("status") == "sending":
            d["status"] = "pending"
        if not reveal:
            d["target"] = mask_target(d.get("channel", ""), str(d.get("target", "")))
    return out


# ---------------------------------------------------------------
# Outbox: delivery with retry/backoff (runs in the background leader)
# ---------------------------------------------------------------
async def process_outbox(limit: int = 50) -> int:
    now = utcnow()
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT * FROM outbox WHERE status IN ('pending','sending') AND next_try <= ? ORDER BY id LIMIT ?",
            (now.isoformat(), limit),
        ).fetchall()
        claim_until = (now + timedelta(seconds=60)).isoformat()  # a crashed worker's claim simply expires
        for r in rows:
            conn.execute("UPDATE outbox SET status='sending', next_try=? WHERE id=?", (claim_until, r["id"]))
        conn.commit()
    finally:
        conn.close()
    if not rows:
        return 0

    items = [{"channel": r["channel"], "target": r["target"], "text": r["text"],
              "payload": json.loads(r["payload"] or "{}")} for r in rows]
    results = await alert_service.deliver(items, dry_run=settings.SIMULATION_MODE)

    conn = get_db()
    try:
        for r, res in zip(rows, results):
            attempts = r["attempts"] + 1
            status, nxt = res["status"], r["next_try"]
            if status == "failed" and attempts < MAX_ATTEMPTS:
                status = "pending"  # retry with exponential backoff: 30 s, 60 s, 120 s, 240 s
                nxt = (utcnow() + timedelta(seconds=30 * 2 ** (attempts - 1))).isoformat()
            conn.execute("UPDATE outbox SET status=?, detail=?, attempts=?, next_try=? WHERE id=?",
                         (status, str(res.get("detail", ""))[:180], attempts, nxt, r["id"]))
        conn.commit()
    finally:
        conn.close()
    return len(rows)


# ---------------------------------------------------------------
# Background work: simulator (demo only), evaluation, outbox, retention
# ---------------------------------------------------------------
def sim_step() -> None:
    """One tick of the random-walk simulator. Only ever runs in SIMULATION_MODE."""
    conn = get_db()
    cur = conn.cursor()
    for r in cur.execute("SELECT * FROM sites").fetchall():
        flow = clamp(r["flow"] + random.uniform(-0.09, 0.09), 0.2, 3.6)
        debris = clamp(r["debris"] + random.uniform(-2, 2), 5, 98)
        flood = clamp(r["flood"] + random.uniform(-1, 1), 10, 98)
        solar = clamp(r["solar"] + random.uniform(-1, 1), 20, 99)
        record_reading(conn, r["id"], flow, debris, flood, solar)
    for u in cur.execute("SELECT * FROM units WHERE active=1").fetchall():
        plastic_load = clamp(u["plastic_load"] + random.uniform(-3, 3), 10, 98)
        bales = u["bales"] + (1 if random.random() < 0.15 else 0)
        cur.execute("UPDATE units SET plastic_load=?, bales=? WHERE id=?", (plastic_load, bales, u["id"]))
    conn.commit()
    conn.close()


def acquire_leader(holder: str, ttl: float = 15.0) -> bool:
    """DB lease so exactly ONE process (of many uvicorn workers) runs the background jobs."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = time.time()
        row = conn.execute("SELECT holder, expires FROM leader WHERE name='background'").fetchone()
        ok = row is None or row["holder"] == holder or row["expires"] < now
        if ok:
            conn.execute(
                "INSERT INTO leader (name, holder, expires) VALUES ('background', ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET holder=excluded.holder, expires=excluded.expires",
                (holder, now + ttl),
            )
        conn.commit()
        return ok
    finally:
        conn.close()


def prune_readings(now: Optional[datetime] = None) -> int:
    cutoff = ((now or utcnow()) - timedelta(days=settings.READINGS_RETENTION_DAYS)).isoformat()
    conn = get_db()
    n = conn.execute("DELETE FROM readings WHERE ts < ?", (cutoff,)).rowcount
    conn.commit()
    conn.close()
    return n


async def background_loop() -> None:
    holder = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
    last = {"sim": 0.0, "eval": 0.0, "outbox": 0.0, "prune": 0.0}
    while True:
        await asyncio.sleep(1)
        try:
            if not await asyncio.to_thread(acquire_leader, holder):
                continue
            t = time.monotonic()
            if settings.SIMULATION_MODE and t - last["sim"] >= 3:
                last["sim"] = t
                await asyncio.to_thread(sim_step)
            if t - last["eval"] >= 3:
                last["eval"] = t
                await asyncio.to_thread(evaluate_alerts)
            if t - last["outbox"] >= 2:
                last["outbox"] = t
                await process_outbox()
            if t - last["prune"] >= 3600:
                last["prune"] = t
                await asyncio.to_thread(prune_readings)
        except Exception as exc:  # never let the loop die
            print(f"[background] {type(exc).__name__}: {exc}")
