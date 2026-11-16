"""Authentication, roles, device keys, audit log and rate limiting.

Stdlib only. Roles: viewer < operator < admin.
  viewer   - read dashboards and the alert log
  operator - acknowledge alerts, send drills, manage recipients, deploy units
  admin    - everything, plus users, device keys, removing recipients, audit log
Buoys do NOT use user accounts: each site has its own device key (X-Device-Key).
"""
import base64
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import settings

ROLES = {"viewer": 1, "operator": 2, "admin": 3}
USERNAME_RE = re.compile(r"^[a-z0-9._-]{3,32}$")
MIN_PASSWORD = 10


# ---------------- passwords ----------------
def hash_password(password: str, iterations: Optional[int] = None) -> str:
    iterations = iterations or settings.PBKDF2_ITERATIONS
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iters, salt, digest = stored.split("$")
        if scheme != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iters))
        return hmac.compare_digest(dk.hex(), digest)
    except Exception:
        return False


_DUMMY_HASH = hash_password("dummy-password-for-timing", iterations=1000)


# ---------------- signed, expiring tokens ----------------
def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


_runtime_secret: Optional[str] = None


def load_secret(conn) -> None:
    """Demo mode without SECRET_KEY: use a random key stored in the database, so several uvicorn
    workers (or a restart that keeps the DB) all agree. A per-process random key would sign a token
    in one worker and reject it in the next, bouncing users back to the sign-in page."""
    global _runtime_secret
    if settings.SECRET_KEY:
        return
    conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT OR IGNORE INTO kv (key, value) VALUES ('secret', ?)", (secrets.token_urlsafe(48),))
    _runtime_secret = conn.execute("SELECT value FROM kv WHERE key='secret'").fetchone()["value"]


def _key() -> bytes:
    k = settings.SECRET_KEY or _runtime_secret
    if not k:
        raise RuntimeError("No signing key available: init_db() has not run")
    return k.encode()


def _sign(body: str) -> str:
    return _b64(hmac.new(_key(), body.encode(), hashlib.sha256).digest())


def make_token(username: str, role: str, ttl_seconds: Optional[int] = None) -> str:
    ttl = ttl_seconds if ttl_seconds is not None else settings.TOKEN_TTL_HOURS * 3600
    body = _b64(json.dumps({"sub": username, "role": role, "exp": int(time.time()) + ttl}).encode())
    return f"{body}.{_sign(body)}"


def read_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        body, sig = token.split(".")
        if not hmac.compare_digest(sig, _sign(body)):
            return None
        claims = json.loads(_unb64(body))
        if int(claims["exp"]) < time.time():
            return None
        return claims
    except Exception:
        return None


# ---------------- device keys (one per buoy / site) ----------------
def new_device_key() -> str:
    return "nzw_" + secrets.token_urlsafe(32)


def _hash_key(key: str) -> str:  # keys are high-entropy random, so a plain hash is enough
    return hashlib.sha256(key.encode()).hexdigest()


def set_device_key(conn, site_id: str) -> str:
    key = new_device_key()
    conn.execute(
        "INSERT INTO devices (site_id, key_hash, created_at) VALUES (?,?,?) "
        "ON CONFLICT(site_id) DO UPDATE SET key_hash=excluded.key_hash, created_at=excluded.created_at",
        (site_id, _hash_key(key), _now()),
    )
    return key


def check_device_key(conn, site_id: str, key: Optional[str]) -> bool:
    row = conn.execute("SELECT key_hash FROM devices WHERE site_id=?", (site_id,)).fetchone()
    stored = row["key_hash"] if row else _hash_key("no-such-device")  # constant-ish work either way
    ok = hmac.compare_digest(_hash_key(key or ""), stored)
    return bool(row) and ok and bool(key)


# ---------------- tables, users, audit ----------------
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_tables(cur) -> None:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, pw_hash TEXT NOT NULL, "
        "role TEXT NOT NULL, active INTEGER DEFAULT 1, created_at TEXT NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS devices (site_id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, "
        "actor TEXT NOT NULL, action TEXT NOT NULL, detail TEXT)"
    )


def audit(conn, actor: str, action: str, detail: str = "") -> None:
    conn.execute("INSERT INTO audit (ts, actor, action, detail) VALUES (?,?,?,?)", (_now(), actor, action, detail[:500]))


def bootstrap_admin(conn) -> None:
    """Create the first admin from ADMIN_PASSWORD (mandatory in live mode).

    Normally this only runs while there are no users. With RESET_ADMIN_PASSWORD=true the admin account
    is created, re-enabled and given ADMIN_PASSWORD even if it already exists (lock-out recovery on hosts
    without a shell, e.g. Render's free tier). Turn the flag off again afterwards."""
    if settings.RESET_ADMIN_PASSWORD:
        if len(settings.ADMIN_PASSWORD) < MIN_PASSWORD:
            raise RuntimeError(f"RESET_ADMIN_PASSWORD needs ADMIN_PASSWORD of at least {MIN_PASSWORD} characters")
        conn.execute(
            "INSERT INTO users (username, pw_hash, role, active, created_at) VALUES (?,?,?,1,?) "
            "ON CONFLICT(username) DO UPDATE SET pw_hash=excluded.pw_hash, role='admin', active=1",
            (settings.ADMIN_USERNAME, hash_password(settings.ADMIN_PASSWORD), "admin", _now()),
        )
        audit(conn, "system", "admin.password_reset", f"{settings.ADMIN_USERNAME} via RESET_ADMIN_PASSWORD")
        print(f"[startup] '{settings.ADMIN_USERNAME}' password set from ADMIN_PASSWORD (RESET_ADMIN_PASSWORD=true). "
              "Remove RESET_ADMIN_PASSWORD after you sign in, or every restart will overwrite the password.")
        return
    if conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]:
        return
    pw = settings.ADMIN_PASSWORD
    generated = False
    if not pw:
        if not settings.SIMULATION_MODE:
            raise RuntimeError("ADMIN_PASSWORD must be set to create the first admin when SIMULATION_MODE=false")
        pw, generated = secrets.token_urlsafe(12), True
    if len(pw) < MIN_PASSWORD:
        raise RuntimeError(f"ADMIN_PASSWORD must be at least {MIN_PASSWORD} characters")
    inserted = conn.execute(  # OR IGNORE: several workers may start at the same moment
        "INSERT OR IGNORE INTO users (username, pw_hash, role, active, created_at) VALUES (?,?,?,1,?)",
        (settings.ADMIN_USERNAME, hash_password(pw), "admin", _now()),
    ).rowcount
    if generated and inserted:
        print(f"[startup] Demo admin created -> username: {settings.ADMIN_USERNAME}  password: {pw}  (shown once)")


def authenticate(conn, username: str, password: str) -> Optional[Dict[str, str]]:
    row = conn.execute(
        "SELECT username, pw_hash, role, active FROM users WHERE username=?", ((username or "").strip().lower(),)
    ).fetchone()
    ok = verify_password(password or "", row["pw_hash"] if row else _DUMMY_HASH)
    if row and ok and row["active"]:
        return {"username": row["username"], "role": row["role"]}
    return None


# ---------------- rate limiting (per process) ----------------
class RateLimiter:
    """Sliding window. `allow()` records a hit; `blocked()/record()/reset()` suit failure counting."""

    def __init__(self, max_calls: int, window_seconds: float):
        self.max, self.window = max_calls, window_seconds
        self._hits: Dict[Any, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def _prune(self, key, now):
        q = self._hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        return q

    def blocked(self, key) -> bool:
        with self._lock:
            return len(self._prune(key, time.monotonic())) >= self.max

    def record(self, key) -> None:
        with self._lock:
            self._prune(key, time.monotonic()).append(time.monotonic())

    def reset(self, key) -> None:
        with self._lock:
            self._hits.pop(key, None)

    def allow(self, key) -> bool:
        with self._lock:
            q = self._prune(key, time.monotonic())
            if len(q) >= self.max:
                return False
            q.append(time.monotonic())
            return True
