"""HTTP-level tests (auth, roles, ingest, masking). Needs `pip install -r requirements.txt`;
skipped automatically when FastAPI isn't installed."""
from tests import _env  # noqa: F401

import unittest

try:
    from fastapi.testclient import TestClient

    import main
    HAVE_FASTAPI = True
except ImportError:  # pragma: no cover
    HAVE_FASTAPI = False

import engine
from tests.test_engine import add_recipient, fresh_db, readings

ADMIN_PW = "correct-horse-battery"
PW = "another-long-password"


def hdr(token):
    return {"Authorization": f"Bearer {token}"}


@unittest.skipUnless(HAVE_FASTAPI, "fastapi not installed")
class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fresh_db()
        cls._cm = TestClient(main.app)
        cls.c = cls._cm.__enter__()  # runs the lifespan (init_db); RUN_BACKGROUND=0 so no loops
        cls.admin = cls.login("admin", ADMIN_PW)
        for name, role in (("vera", "viewer"), ("olga", "operator"), ("olaf", "operator")):
            r = cls.c.post("/api/users", json={"username": name, "password": PW, "role": role}, headers=hdr(cls.admin))
            assert r.status_code == 200, r.text
        cls.viewer, cls.op, cls.op2 = cls.login("vera", PW), cls.login("olga", PW), cls.login("olaf", PW)

    @classmethod
    def tearDownClass(cls):
        cls._cm.__exit__(None, None, None)

    @classmethod
    def login(cls, user, pw):
        r = cls.c.post("/api/auth/login", json={"username": user, "password": pw})
        assert r.status_code == 200, r.text
        return r.json()["token"]

    # ---- access control ----
    def test_public_endpoints(self):
        self.assertEqual(self.c.get("/healthz").status_code, 200)
        meta = self.c.get("/api/meta").json()
        self.assertFalse(meta["simulation"])

    def test_everything_else_needs_a_token(self):
        for path in ("/api/sites", "/api/units", "/api/alerts", "/api/recipients", "/api/alert-config", "/api/audit"):
            self.assertEqual(self.c.get(path).status_code, 401, path)
        self.assertEqual(self.c.get("/api/sites", headers=hdr("garbage")).status_code, 401)

    def test_login_rejects_bad_credentials(self):
        r = self.c.post("/api/auth/login", json={"username": "admin", "password": "nope"})
        self.assertEqual(r.status_code, 401)

    def test_role_enforcement(self):
        self.assertEqual(self.c.get("/api/sites", headers=hdr(self.viewer)).status_code, 200)
        self.assertEqual(self.c.get("/api/recipients", headers=hdr(self.viewer)).status_code, 403)
        self.assertEqual(self.c.post("/api/alerts/test/S1", headers=hdr(self.viewer)).status_code, 403)
        body = {"name": "Duty", "channel": "telegram", "target": "-100123456789"}
        self.assertEqual(self.c.post("/api/recipients", json=body, headers=hdr(self.viewer)).status_code, 403)
        r = self.c.post("/api/recipients", json=body, headers=hdr(self.op))
        self.assertEqual(r.status_code, 200, r.text)
        rid = r.json()["id"]
        self.assertEqual(self.c.delete(f"/api/recipients/{rid}", headers=hdr(self.op)).status_code, 403)  # admin only
        self.assertEqual(self.c.delete(f"/api/recipients/{rid}", headers=hdr(self.admin)).status_code, 200)
        self.assertEqual(self.c.get("/api/users", headers=hdr(self.op)).status_code, 403)

    def test_recipient_target_validation(self):
        for body in (
            {"name": "x", "channel": "telegram", "target": "not-an-id"},
            {"name": "x", "channel": "sms", "target": "09171234567"},
            {"name": "x", "channel": "fax", "target": "123"},
        ):
            self.assertEqual(self.c.post("/api/recipients", json=body, headers=hdr(self.op)).status_code, 400, body)

    def test_drills_are_rate_limited(self):
        codes = [self.c.post("/api/alerts/test/S1", headers=hdr(self.op2)).status_code for _ in range(4)]
        self.assertEqual(codes, [200, 200, 200, 429])

    def test_login_lockout(self):
        codes = [self.c.post("/api/auth/login", json={"username": "lockme", "password": "x"}).status_code for _ in range(6)]
        self.assertEqual(codes[:5], [401] * 5)
        self.assertEqual(codes[5], 429)

    def test_disabled_user_loses_access_immediately(self):
        self.c.post("/api/users", json={"username": "temp", "password": PW, "role": "viewer"}, headers=hdr(self.admin))
        tok = self.login("temp", PW)
        self.assertEqual(self.c.get("/api/auth/me", headers=hdr(tok)).status_code, 200)
        self.assertEqual(self.c.delete("/api/users/temp", headers=hdr(self.admin)).status_code, 200)
        self.assertEqual(self.c.get("/api/auth/me", headers=hdr(tok)).status_code, 401)
        dup = self.c.post("/api/users", json={"username": "vera", "password": PW, "role": "viewer"}, headers=hdr(self.admin))
        self.assertEqual(dup.status_code, 409)

    # ---- ingest ----
    def test_ingest_requires_a_device_key_scoped_to_its_site(self):
        good = {"flow": 1.2, "debris": 50, "flood": 40, "solar": 70}
        self.assertEqual(self.c.post("/api/ingest/S3", json=good).status_code, 401)
        key = self.c.post("/api/sites/S3/device-key", headers=hdr(self.admin)).json()["device_key"]
        self.assertEqual(self.c.post("/api/ingest/S3", json=good, headers={"X-Device-Key": "nzw_wrong"}).status_code, 401)
        self.assertEqual(self.c.post("/api/ingest/S4", json=good, headers={"X-Device-Key": key}).status_code, 401)
        ok = self.c.post("/api/ingest/S3", json=good, headers={"X-Device-Key": key})
        self.assertEqual(ok.status_code, 200, ok.text)

    def test_ingest_rejects_out_of_range_values(self):
        key = self.c.post("/api/sites/S5/device-key", headers=hdr(self.admin)).json()["device_key"]
        for bad in ({"flow": -1, "debris": 5, "flood": 5, "solar": 5},
                    {"flow": 1, "debris": 5, "flood": 500, "solar": 5},
                    {"flow": 1, "debris": 5, "flood": 5}):
            r = self.c.post("/api/ingest/S5", json=bad, headers={"X-Device-Key": key})
            self.assertEqual(r.status_code, 422, bad)

    # ---- alerts ----
    def test_viewers_see_masked_targets_and_operators_can_acknowledge(self):
        add_recipient("API1", channel="sms", target="+639170000009")
        readings(75, site="S6")
        ids = engine.evaluate_alerts()
        self.assertTrue(ids)
        v = self.c.get("/api/alerts", headers=hdr(self.viewer)).json()
        o = self.c.get("/api/alerts", headers=hdr(self.op)).json()
        self.assertTrue(all(d["target"].startswith("\u2022") for a in v for d in a["deliveries"]))
        self.assertTrue(any(d["target"] == "+639170000009" for a in o for d in a["deliveries"]))
        self.assertEqual(self.c.post(f"/api/alerts/{ids[0]}/ack", headers=hdr(self.viewer)).status_code, 403)
        r = self.c.post(f"/api/alerts/{ids[0]}/ack", headers=hdr(self.op))
        self.assertEqual((r.status_code, r.json()["acknowledged_by"]), (200, "olga"))

    def test_thresholds_are_read_only(self):
        for method in ("put", "patch", "post"):
            r = getattr(self.c, method)("/api/alert-config", headers=hdr(self.admin), json={"flood_warning": 99})
            self.assertEqual(r.status_code, 405, method)

    def test_sites_status_matches_alert_engine(self):
        readings(90, site="S7")
        engine.evaluate_alerts()
        sites = {s["id"]: s for s in self.c.get("/api/sites", headers=hdr(self.viewer)).json()}
        self.assertEqual(sites["S7"]["status"], "High")


if __name__ == "__main__":
    unittest.main()
