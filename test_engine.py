"""Alert-engine tests: confirmation, hysteresis, reminders, escalation, all-clear,
offline detection, drills, outbox retry. Pure stdlib + sqlite (no web framework)."""
from tests import _env  # noqa: F401  (must come first)

import asyncio
import os
import unittest
from datetime import timedelta

import alerts as alert_service
import auth
import engine
import settings

CFG = engine.read_config()
SITE = "S2"  # flood 64 / flow 1.4 in the seed: nominal


def fresh_db():
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(settings.DB_PATH + suffix)
        except FileNotFoundError:
            pass
    engine.init_db()


def add_recipient(rid="T1", channel="telegram", target="-1001111111111", tier=1, active=1):
    conn = engine.get_db()
    conn.execute("INSERT INTO recipients (id,name,org,channel,target,active,tier) VALUES (?,?,?,?,?,?,?)",
                 (rid, f"Rcpt {rid}", "Org", channel, target, active, tier))
    conn.commit()
    conn.close()


def readings(flood, n=3, flow=1.0, site=SITE):
    conn = engine.get_db()
    for _ in range(n):
        engine.record_reading(conn, site, flow, 50, flood, 80)
    conn.commit()
    conn.close()


def q(sql, *args):
    conn = engine.get_db()
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return rows


class ClassifyTests(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(engine.classify(1.0, 60, CFG)[0], None)
        self.assertEqual(engine.classify(1.0, 72, CFG)[0], "Warning")
        self.assertEqual(engine.classify(1.0, 85, CFG)[0], "Critical")
        self.assertEqual(engine.classify(2.6, 10, CFG)[0], "Warning")
        self.assertEqual(engine.classify(3.0, 10, CFG)[0], "Critical")

    def test_hysteresis_holds_then_releases(self):
        self.assertEqual(engine.classify(1.0, 70, CFG, "Warning")[0], "Warning")   # inside the 3-pt margin
        self.assertIsNone(engine.classify(1.0, 68, CFG, "Warning")[0])              # below it: released
        self.assertEqual(engine.classify(1.0, 83, CFG, "Critical")[0], "Critical")
        self.assertEqual(engine.classify(1.0, 81, CFG, "Critical")[0], "Warning")   # steps down, doesn't clear

    def test_confirmation_window(self):
        spike, ok = {"flow": 1.0, "flood": 90}, {"flow": 1.0, "flood": 50}
        self.assertEqual(engine.confirmed([spike, ok, ok], CFG, None)[0], None)
        self.assertEqual(engine.confirmed([spike, spike, ok], CFG, None)[0], "Critical")
        self.assertEqual(engine.confirmed([spike], CFG, None)[0], None)  # not enough data yet


class AlertFlowTests(unittest.TestCase):
    def setUp(self):
        fresh_db()
        add_recipient("T1", tier=1)
        add_recipient("T2", channel="sms", target="+639170000001", tier=2)
        self.t0 = engine.utcnow()

    def test_single_spike_does_not_alert(self):
        readings(50, 2)
        readings(95, 1)
        self.assertEqual(engine.evaluate_alerts(self.t0), [])

    def test_sustained_warning_alerts_once_and_goes_to_tier1_only(self):
        readings(75)
        ids = engine.evaluate_alerts(self.t0)
        self.assertEqual(len(ids), 1)
        rows = q("SELECT recipient_id, status FROM outbox WHERE alert_id=?", ids[0])
        self.assertEqual([r["recipient_id"] for r in rows], ["T1"])
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(minutes=1)), [])

    def test_reminder_until_acknowledged(self):
        readings(75)
        first = engine.evaluate_alerts(self.t0)[0]
        again = engine.evaluate_alerts(self.t0 + timedelta(minutes=11))
        self.assertEqual(len(again), 1)  # unacknowledged -> reminder
        conn = engine.get_db()
        conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (again[0],))
        conn.commit()
        conn.close()
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(minutes=25)), [])
        self.assertNotEqual(first, again[0])

    def test_escalation_to_tier2_once_even_with_reminders(self):
        readings(75)
        aid = engine.evaluate_alerts(self.t0)[0]
        engine.evaluate_alerts(self.t0 + timedelta(minutes=9))  # too early: no reminder, no escalation
        self.assertEqual(q("SELECT escalated FROM alerts WHERE id=?", aid)[0]["escalated"], 0)
        engine.evaluate_alerts(self.t0 + timedelta(minutes=16))
        self.assertEqual(q("SELECT escalated FROM alerts WHERE id=?", aid)[0]["escalated"], 1)
        self.assertEqual(len(q("SELECT * FROM outbox WHERE alert_id=? AND recipient_id='T2'", aid)), 1)
        for m in (30, 45, 60):  # reminders keep going to tier 1; tier 2 is not pinged again
            engine.evaluate_alerts(self.t0 + timedelta(minutes=m))
        self.assertEqual(len(q("SELECT * FROM outbox WHERE recipient_id='T2'")), 1)

    def test_ack_clears_the_whole_episode_and_records_who(self):
        readings(75)
        engine.evaluate_alerts(self.t0)
        last = engine.evaluate_alerts(self.t0 + timedelta(minutes=11))[0]  # a reminder
        conn = engine.get_db()
        row = engine.acknowledge(conn, last, "ana")
        conn.close()
        self.assertEqual((row["acknowledged"], row["acknowledged_by"]), (1, "ana"))
        self.assertEqual(q("SELECT COUNT(*) AS c FROM alerts WHERE acknowledged=0")[0]["c"], 0)
        self.assertEqual(q("SELECT actor, action FROM audit")[-1]["action"], "alert.ack")

    def test_acknowledged_alert_is_not_escalated(self):
        readings(75)
        aid = engine.evaluate_alerts(self.t0)[0]
        conn = engine.get_db()
        conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (aid,))
        conn.commit()
        conn.close()
        engine.evaluate_alerts(self.t0 + timedelta(minutes=30))
        self.assertEqual(q("SELECT escalated FROM alerts WHERE id=?", aid)[0]["escalated"], 0)

    def test_escalation_to_critical_is_a_new_alert(self):
        readings(75)
        engine.evaluate_alerts(self.t0)
        readings(90)
        ids = engine.evaluate_alerts(self.t0 + timedelta(seconds=10))
        self.assertEqual(len(ids), 1)
        self.assertEqual(q("SELECT severity FROM alerts WHERE id=?", ids[0])[0]["severity"], "Critical")

    def test_hovering_does_not_flap_and_all_clear_is_sent(self):
        readings(75)
        engine.evaluate_alerts(self.t0)
        readings(71)  # under the 72 line but inside the 3-pt margin: still Warning, no new alert
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(seconds=30)), [])
        self.assertEqual(q("SELECT severity FROM site_state WHERE site_id=?", SITE)[0]["severity"], "Warning")
        readings(60)
        ids = engine.evaluate_alerts(self.t0 + timedelta(seconds=60))
        self.assertEqual(len(ids), 1)
        clear = q("SELECT severity, acknowledged FROM alerts WHERE id=?", ids[0])[0]
        self.assertEqual((clear["severity"], clear["acknowledged"]), ("Clear", 1))
        self.assertIsNone(q("SELECT severity FROM site_state WHERE site_id=?", SITE)[0]["severity"])

    def test_all_clear_reaches_tier2_only_after_escalation(self):
        readings(75)
        engine.evaluate_alerts(self.t0)
        engine.evaluate_alerts(self.t0 + timedelta(minutes=16))  # escalates
        readings(55)
        cid = engine.evaluate_alerts(self.t0 + timedelta(minutes=20))[0]
        who = sorted(r["recipient_id"] for r in q("SELECT recipient_id FROM outbox WHERE alert_id=?", cid))
        self.assertEqual(who, ["T1", "T2"])

    def test_offline_detection_and_recovery(self):
        conn = engine.get_db()
        conn.execute("INSERT INTO units (id,site_id,deployed_at,plastic_load,bales,active) VALUES ('PB-01',?,?,10,0,1)",
                     (SITE, (self.t0 - timedelta(hours=1)).isoformat()))
        conn.execute("UPDATE sites SET last_seen=? WHERE id=?", ((self.t0 - timedelta(seconds=120)).isoformat(), SITE))
        conn.commit()
        conn.close()
        ids = engine.evaluate_alerts(self.t0)
        self.assertEqual([q("SELECT severity FROM alerts WHERE id=?", i)[0]["severity"] for i in ids], ["Offline"])
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(seconds=5)), [])  # no repeat
        readings(50, 1)  # buoy reports again -> last_seen refreshed
        ids = engine.evaluate_alerts(engine.utcnow())
        self.assertEqual([q("SELECT severity FROM alerts WHERE id=?", i)[0]["severity"] for i in ids], ["Clear"])

    def test_undeployed_sites_never_go_offline(self):
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(days=1)), [])

    def test_drill_is_labelled_and_never_escalated_or_reminded(self):
        aid = engine.raise_drill(SITE, "tester")
        row = q("SELECT message, drill, acknowledged FROM alerts WHERE id=?", aid)[0]
        self.assertIn("DRILL", row["message"])
        self.assertEqual((row["drill"], row["acknowledged"]), (1, 1))
        self.assertEqual(len(q("SELECT * FROM outbox WHERE alert_id=?", aid)), 2)  # every active recipient
        self.assertEqual(engine.evaluate_alerts(self.t0 + timedelta(hours=1)), [])
        self.assertEqual(q("SELECT escalated FROM alerts WHERE id=?", aid)[0]["escalated"], 0)

    def test_site_risk_matches_alert_state(self):
        self.assertEqual(engine.site_risk(SITE), "Low")
        readings(90)
        engine.evaluate_alerts(self.t0)
        self.assertEqual(engine.site_risk(SITE), "High")  # same source of truth as the Critical alert


class SetupTests(unittest.TestCase):
    def test_live_mode_deactivates_placeholder_recipients(self):
        fresh_db()
        add_recipient("P1", channel="sms", target="+639171234567")
        add_recipient("P2", target="-1001234567890")
        add_recipient("Real", target="-100555555555")
        engine.init_db()
        active = {r["id"] for r in q("SELECT id FROM recipients WHERE active=1")}
        self.assertEqual(active, {"Real"})

    def test_target_validation(self):
        v = engine.validate_target
        self.assertIsNone(v("telegram", "-1001234567890"))
        self.assertIsNone(v("telegram", "@drrmo_batangas"))
        self.assertIsNotNone(v("telegram", "not an id"))
        self.assertIsNone(v("sms", "+639171234567"))
        self.assertIsNotNone(v("sms", "09171234567"))

    def test_masking(self):
        self.assertEqual(engine.mask_target("sms", "+639171234567"), "\u2022\u2022\u2022567")
        self.assertEqual(engine.mask_target("webhook", "https://secret"), "webhook")


class OutboxTests(unittest.TestCase):
    def setUp(self):
        fresh_db()
        add_recipient("T1")
        readings(75)
        self.aid = engine.evaluate_alerts(engine.utcnow())[0]
        self._orig = alert_service.deliver

    def tearDown(self):
        alert_service.deliver = self._orig

    def _run(self):
        conn = engine.get_db()  # make every pending row due now
        conn.execute("UPDATE outbox SET next_try='2000-01-01T00:00:00+00:00' WHERE status IN ('pending','sending')")
        conn.commit()
        conn.close()
        return asyncio.run(engine.process_outbox())

    def test_failure_retries_then_gives_up(self):
        async def failing(items, dry_run=False):
            return [{"status": "failed", "detail": "boom"} for _ in items]
        alert_service.deliver = failing
        for attempt in range(1, engine.MAX_ATTEMPTS + 1):
            self.assertEqual(self._run(), 1)
            row = q("SELECT status, attempts FROM outbox WHERE alert_id=?", self.aid)[0]
            self.assertEqual(row["attempts"], attempt)
            self.assertEqual(row["status"], "pending" if attempt < engine.MAX_ATTEMPTS else "failed")
        self.assertEqual(self._run(), 0)

    def test_success_marks_sent_and_is_not_resent(self):
        calls = []
        async def ok(items, dry_run=False):
            calls.append(len(items))
            return [{"status": "sent", "detail": "ok"} for _ in items]
        alert_service.deliver = ok
        self.assertEqual(self._run(), 1)
        self.assertEqual(self._run(), 0)
        self.assertEqual(calls, [1])
        conn = engine.get_db()
        d = engine.deliveries_for(conn, self.aid, None, reveal=False)
        conn.close()
        self.assertEqual(d[0]["status"], "sent")
        self.assertTrue(d[0]["target"].startswith("\u2022"))

    def test_leader_lease_is_exclusive(self):
        self.assertTrue(engine.acquire_leader("a", ttl=30))
        self.assertTrue(engine.acquire_leader("a", ttl=30))
        self.assertFalse(engine.acquire_leader("b", ttl=30))
        conn = engine.get_db()
        conn.execute("UPDATE leader SET expires=0")
        conn.commit()
        conn.close()
        self.assertTrue(engine.acquire_leader("b"))

    def test_prune_readings(self):
        conn = engine.get_db()
        conn.execute("UPDATE readings SET ts='2000-01-01T00:00:00+00:00' WHERE id=1")
        conn.commit()
        conn.close()
        self.assertEqual(engine.prune_readings(), 1)


if __name__ == "__main__":
    unittest.main()
