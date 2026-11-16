"""Central configuration. Everything comes from environment variables (or .env).

SIMULATION_MODE (default: true) is the master safety switch:
  true  -> random-walk sensor simulator runs, every notification is forced to
           dry-run ("simulated"), and the UI shows a DEMO DATA banner.
  false -> no simulator, real channels are used, and SECRET_KEY / ADMIN_PASSWORD
           are mandatory so the site can't start unauthenticated.
"""
import os
from pathlib import Path

try:  # optional convenience
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

BASE_DIR = Path(__file__).resolve().parent


def _flag(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


SIMULATION_MODE = _flag("SIMULATION_MODE", True)
RUN_BACKGROUND = _flag("RUN_BACKGROUND", True)  # leader election makes this safe with many workers

DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "storm_baler.db"))
RIVER_GEOJSON = Path(os.getenv("RIVER_GEOJSON", str(BASE_DIR / "calumpang_rivers.geojson")))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]

TOKEN_TTL_HOURS = int(os.getenv("TOKEN_TTL_HOURS", "12"))
PBKDF2_ITERATIONS = int(os.getenv("PBKDF2_ITERATIONS", "600000"))
ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME", "admin").strip().lower() or "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "password").strip()  # env UIs often add stray spaces/newlines
# Recovery switch: apply ADMIN_PASSWORD to the admin account at startup even if it already exists.
RESET_ADMIN_PASSWORD = _flag("RESET_ADMIN_PASSWORD", False)

# A deployed buoy that has not reported for this long is declared OFFLINE.
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "30" if SIMULATION_MODE else "900"))
READINGS_RETENTION_DAYS = int(os.getenv("READINGS_RETENTION_DAYS", "2" if SIMULATION_MODE else "90"))

_secret = os.getenv("SECRET_KEY", "").strip()
if not _secret:
    if not SIMULATION_MODE:
        raise RuntimeError("SECRET_KEY must be set when SIMULATION_MODE=false (use: python -c \"import secrets;print(secrets.token_urlsafe(48))\")")
    # Demo mode without SECRET_KEY: auth.load_secret() keeps a random key in the database so every
    # worker process (and every restart that keeps the DB) signs and verifies with the SAME key.
    _secret = ""
elif not SIMULATION_MODE and len(_secret) < 32:
    raise RuntimeError("SECRET_KEY must be at least 32 characters when SIMULATION_MODE=false")
SECRET_KEY = _secret
