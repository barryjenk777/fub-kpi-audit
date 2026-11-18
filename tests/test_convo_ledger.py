"""The conversation ledger's parsing and counting rules. These are the
numbers the Sunday email reports, so they are pinned here."""
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

import convo_ledger as cl             # noqa: E402

TEXT = (
    "Stars Link,CRM Link,Name,Source,"
    "Last Lead Reply Date (America/New_York),CRM Agent / Pond,CRM Stage,"
    "CRM Last Communication (America/New_York),"
    "Ylopo Last Outreach (America/New_York)\n"
    "x,https://team.followupboss.com/2/people/view/87175,Test Lead,Import,"
    "2026-11-15 11:25,Some Agent,Lead,2026-11-06 10:29,2026-11-15 11:19\n"
    "x,,No Link,Import,2026-11-15 11:25,Some Agent,Lead,,\n")
VOICE = (
    "Stars Link,CRM Link,Name,Source,Conversation Date (America/New_York),"
    "Call Outcome,Call Summary,CRM Agent / Pond,CRM Stage,"
    "CRM Last Communication (America/New_York),"
    "Ylopo Last Outreach (America/New_York)\n"
    "x,https://team.followupboss.com/2/people/view/42,Voice Lead,Ylopo,"
    "2026-11-14 18:22,Nurture,\"a, summary\",Shark Tank,Lead,,\n")


class ParseExport(unittest.TestCase):
    def test_text_export(self):
        out = cl.parse_export(TEXT)
        self.assertEqual(out["channel"], "text")
        self.assertEqual(len(out["rows"]), 1)
        self.assertEqual(out["skipped"], ["No Link"])
        r = out["rows"][0]
        self.assertEqual(r["person_id"], 87175)
        self.assertEqual(str(r["convo_day"]), "2026-11-15")
        # 11:25 Eastern in November is 16:25 UTC
        self.assertEqual(r["convo_at"].strftime("%H:%M"), "16:25")

    def test_voice_export(self):
        out = cl.parse_export(VOICE)
        self.assertEqual(out["channel"], "voice")
        self.assertEqual(out["rows"][0]["outcome"], "Nurture")

    def test_other_csv_is_refused(self):
        with self.assertRaises(ValueError):
            cl.parse_export("a,b\n1,2\n")


class Counting(unittest.TestCase):
    def test_engaged_alert_alone_is_not_a_transfer(self):
        self.assertFalse(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_engaged_alert"}))

    def test_export_or_handoff_evidence_counts(self):
        self.assertTrue(cl.is_confirmed({"in_export": True}))
        self.assertTrue(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_disposition"}))
        self.assertTrue(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_followup_alert"}))


if __name__ == "__main__":
    unittest.main()


class HandoffWatchRules(unittest.TestCase):
    def test_sphere_is_left_alone(self):
        self.assertTrue(cl.is_sphere({"source": "Sphere"}))
        self.assertTrue(cl.is_sphere({"source": "SOI"}))
        self.assertTrue(cl.is_sphere({"source": "Zillow", "tags": ["Sphere"]}))
        self.assertFalse(cl.is_sphere({"source": "Ylopo PPC+", "tags": []}))

    def test_a_lead_who_said_no_is_not_offered(self):
        for said in ("No", "STOP", "This is not jasmine", "wrong number",
                     "not interested thanks"):
            self.assertTrue(cl._NEGATIVE_REPLY.search(said), said)
        for said in ("Not yet, not this year", "Yes", "Either or is perfect.",
                     "No problem, call me"):
            self.assertFalse(cl._NEGATIVE_REPLY.search(said), said)

    def test_contact_stage_is_treated_like_trash(self):
        for stage in ("Contact", "Trash", "Closed", "Under Contract"):
            self.assertTrue(any(d in stage.lower() for d in cl._DEAD_STAGES))
        for stage in ("Lead", "A - Hot 1-3 Months", "C - Cold 6+ Months"):
            self.assertFalse(any(d in stage.lower() for d in cl._DEAD_STAGES))

    def test_fhalen_transfers_count(self):
        self.assertTrue(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "isa_call"}))


class OwnerAttempt(unittest.TestCase):
    """Nov 18 2026: an agent talked to a lead twice, the AI texted her
    again a week later, and the 24 hour rule gave her to someone else. An
    owner who has tried keeps the lead."""
    AI = {"userName": "Team Lead", "isIncoming": False,
          "created": "2026-11-04T22:32:58Z",
          "note": "Call Outcome from AI Voice\nCall Disposition: VOICEMAIL"}
    DIAL = {"userName": "Agent One", "isIncoming": False,
            "duration": 11, "created": "2026-11-16T16:20:36Z", "note": None}

    def test_a_no_connect_dial_still_counts(self):
        t = cl.owner_attempt([self.AI, self.DIAL], [], "Agent One")
        self.assertEqual(t["kind"], "called")
        self.assertEqual(t["count"], 1)

    def test_a_text_counts(self):
        t = cl.owner_attempt([], [{"userName": "Agent One",
                                   "isIncoming": False,
                                   "created": "2026-11-11T22:11:52Z"}],
                             "Agent One")
        self.assertEqual(t["kind"], "texted")

    def test_ai_calls_and_other_agents_do_not_count(self):
        self.assertIsNone(cl.owner_attempt([self.AI], [], "Team Lead"))
        self.assertIsNone(cl.owner_attempt([self.DIAL], [], "Agent Two"))

    def test_inbound_and_no_owner_do_not_count(self):
        inbound = dict(self.DIAL, isIncoming=True)
        self.assertIsNone(cl.owner_attempt([inbound], [], "Agent One"))
        self.assertIsNone(cl.owner_attempt([self.DIAL], [], ""))
