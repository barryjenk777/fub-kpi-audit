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
    def test_loose_alert_alone_is_not_counted(self):
        self.assertFalse(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_loose_alert"}))

    def test_export_or_strong_evidence_counts(self):
        self.assertTrue(cl.is_confirmed({"in_export": True}))
        self.assertTrue(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_disposition"}))
        self.assertTrue(cl.is_confirmed(
            {"in_export": False, "fub_evidence": "fub_strict_alert"}))


if __name__ == "__main__":
    unittest.main()
