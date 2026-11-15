"""Auth primitives and channel senders. Channel tests use a hand-made fake client,
so no network and no real credentials are involved."""
from tests import _env  # noqa: F401

import asyncio
import time
import unittest

import alerts
import auth
import engine
from tests.test_engine import fresh_db


class FakeResp:
    def __init__(self, status=200, body=None):
        self.status_code, self._body = status, body if body is not None else {}
        self.text = str(self._body)

    def json(self):
        return self._body


class FakeClient:
    def __init__(self, resp):
        self.resp, self.calls = resp, []

    async def post(self, url, **kw):
        self.calls.append((url, kw))
        return self.resp


class AuthTests(unittest.TestCase):
    def test_password_roundtrip(self):
        h = auth.hash_password("a-long-password")
        self.assertTrue(auth.verify_password("a-long-password", h))
        self.assertFalse(auth.verify_password("wrong", h))
        self.assertFalse(auth.verify_password("x", "garbage"))

    def test_token_roundtrip_expiry_and_tamper(self):
        t = auth.make_token("ana", "operator")
        self.assertEqual(auth.read_token(t)["sub"], "ana")
        self.assertIsNone(auth.read_token(auth.make_token("ana", "operator", ttl_seconds=-1)))
        body, sig = t.split(".")
        self.assertIsNone(auth.read_token(body + "." + sig[:-2] + "AA"))
        forged = auth.make_token("ana", "admin").split(".")[0] + "." + sig
        self.assertIsNone(auth.read_token(forged))
        self.assertIsNone(auth.read_token("nonsense"))

    def test_bootstrap_admin_and_authenticate(self):
        fresh_db()
        self.assertEqual(auth.authenticate(engine.get_db(), "ADMIN", "correct-horse-battery")["role"], "admin")
        self.assertIsNone(auth.authenticate(engine.get_db(), "admin", "nope"))
        self.assertIsNone(auth.authenticate(engine.get_db(), "ghost", "nope"))
        conn = engine.get_db()
        conn.execute("UPDATE users SET active=0")
        conn.commit()
        conn.close()
        self.assertIsNone(auth.authenticate(engine.get_db(), "admin", "correct-horse-battery"))

    def test_device_keys(self):
        fresh_db()
        conn = engine.get_db()
        key = auth.set_device_key(conn, "S1")
        conn.commit()
        self.assertTrue(auth.check_device_key(conn, "S1", key))
        self.assertFalse(auth.check_device_key(conn, "S1", "nzw_wrong"))
        self.assertFalse(auth.check_device_key(conn, "S2", key))      # key is per site
        self.assertFalse(auth.check_device_key(conn, "S1", None))
        key2 = auth.set_device_key(conn, "S1")                         # rotation invalidates the old key
        self.assertFalse(auth.check_device_key(conn, "S1", key))
        self.assertTrue(auth.check_device_key(conn, "S1", key2))
        conn.close()

    def test_rate_limiter(self):
        rl = auth.RateLimiter(2, 0.2)
        self.assertTrue(rl.allow("u"))
        self.assertTrue(rl.allow("u"))
        self.assertFalse(rl.allow("u"))
        self.assertTrue(rl.allow("other"))
        time.sleep(0.25)
        self.assertTrue(rl.allow("u"))
        f = auth.RateLimiter(2, 60)
        f.record("k"); f.record("k")
        self.assertTrue(f.blocked("k"))
        f.reset("k")
        self.assertFalse(f.blocked("k"))


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self._tok, self._v = alerts.TELEGRAM_BOT_TOKEN, (alerts.VONAGE_API_KEY, alerts.VONAGE_API_SECRET, alerts.VONAGE_FROM_NUMBER)

    def tearDown(self):
        alerts.TELEGRAM_BOT_TOKEN = self._tok
        alerts.VONAGE_API_KEY, alerts.VONAGE_API_SECRET, alerts.VONAGE_FROM_NUMBER = self._v

    def test_telegram_uses_each_recipients_own_chat_id(self):
        alerts.TELEGRAM_BOT_TOKEN = "123:SECRET"
        c = FakeClient(FakeResp(200, {"ok": True}))
        for chat in ("-1001111111111", "-1002222222222"):
            res = asyncio.run(alerts.send_one(c, "telegram", chat, "hi", {}))
            self.assertEqual(res["status"], "sent")
        self.assertEqual([kw["json"]["chat_id"] for _, kw in c.calls], ["-1001111111111", "-1002222222222"])

    def test_telegram_failure_never_leaks_the_bot_token(self):
        alerts.TELEGRAM_BOT_TOKEN = "123:SECRET"
        c = FakeClient(FakeResp(400, "bad request for bot123:SECRET"))
        res = asyncio.run(alerts.send_one(c, "telegram", "-100", "hi", {}))
        self.assertEqual(res["status"], "failed")
        self.assertNotIn("SECRET", res["detail"])

    def test_no_credentials_means_simulated_and_dry_run_never_calls_out(self):
        alerts.TELEGRAM_BOT_TOKEN = ""
        c = FakeClient(FakeResp())
        self.assertEqual(asyncio.run(alerts.send_one(c, "telegram", "-100", "hi", {}))["status"], "simulated")
        alerts.TELEGRAM_BOT_TOKEN = "123:SECRET"
        self.assertEqual(asyncio.run(alerts.send_one(c, "telegram", "-100", "hi", {}, dry_run=True))["status"], "simulated")
        self.assertEqual(c.calls, [])

    def test_sms_digits_only_ascii_and_failure_reporting(self):
        alerts.VONAGE_API_KEY, alerts.VONAGE_API_SECRET, alerts.VONAGE_FROM_NUMBER = "k", "s", "NZW"
        c = FakeClient(FakeResp(200, {"messages": [{"status": "0", "message-id": "m1"}]}))
        res = asyncio.run(alerts.send_one(c, "sms", "+639171234567", "Site \u2014 Batangas \u2265 85", {}))
        sent = c.calls[0][1]["json"]
        self.assertEqual((res["status"], sent["to"]), ("sent", "639171234567"))
        self.assertTrue(sent["text"].isascii())
        c2 = FakeClient(FakeResp(200, {"messages": [{"status": "4", "error-text": "Invalid credentials"}]}))
        self.assertEqual(asyncio.run(alerts.send_one(c2, "sms", "+639171234567", "x", {}))["status"], "failed")

    def test_message_kinds(self):
        args = dict(site_name="S", flow=1.0, flood=50, reasons=["r"], lat=13.7, lon=121.0, barangay="B")
        self.assertIn("ALL CLEAR", alerts.build_message(severity="Clear", **args))
        self.assertIn("SENSOR OFFLINE", alerts.build_message(severity="Offline", **args))
        self.assertIn("DRILL", alerts.build_message(severity="Critical", drill=True, **args))
        self.assertIn("CRITICAL FLOOD ALERT", alerts.build_message(severity="Critical", **args))


if __name__ == "__main__":
    unittest.main()
