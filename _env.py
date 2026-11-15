"""Test environment: live mode (so nothing is auto-seeded), temp DB, fast password hashing.
Import this BEFORE importing engine/main."""
import os
import tempfile

_d = tempfile.mkdtemp(prefix="nzw-test-")
os.environ.update(
    SIMULATION_MODE="false",
    SECRET_KEY="k" * 48,
    ADMIN_USERNAME="admin",
    ADMIN_PASSWORD="correct-horse-battery",
    PBKDF2_ITERATIONS="1000",
    DB_PATH=os.path.join(_d, "test.db"),
    RUN_BACKGROUND="0",
    STALE_AFTER_SECONDS="60",
)
for k in ("TELEGRAM_BOT_TOKEN", "VONAGE_API_KEY", "VONAGE_API_SECRET", "VONAGE_FROM_NUMBER", "GENERIC_WEBHOOK_URL"):
    os.environ[k] = ""
