"""Tests for maverick_parsers against real Grok Bot payloads.

Run: python3 -m unittest tests.test_maverick_parsers
Pure-python module, no app/db imports, so this runs on the local 3.9
as well as Railway's interpreter.
"""

import os
import unittest

import maverick_parsers as mp

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _fixture(name):
    with open(os.path.join(FIX, name)) as f:
        return f.read()


class DetectKind(unittest.TestCase):
    def test_pipeline_dashboard(self):
        self.assertEqual(mp.detect_kind(_fixture("pipeline_dashboard.txt")),
                         "pipeline_dashboard")

    def test_rules_page(self):
        self.assertEqual(mp.detect_kind(_fixture("rules_empty.txt")), "rules")

    def test_status(self):
        self.assertEqual(
            mp.detect_kind("GROKBOT STATUS: run ok 2026-10-28 13:02 ET"),
            "status")

    def test_legacy_sentinels(self):
        self.assertEqual(mp.detect_kind("MAVERICK GRADING DASHBOARD\nfoo"),
                         "grading_dashboard")
        self.assertEqual(mp.detect_kind("MAVERICK RULES BOARD\nfoo"),
                         "rules")

    def test_unknown(self):
        self.assertEqual(mp.detect_kind("hello world"), "unknown")

    def test_dashboard_prose_mentioning_rules_is_not_rules(self):
        # The pipeline page's own prose names the "Rules & Alerts feature"
        raw = _fixture("pipeline_dashboard.txt")
        self.assertIn("Rules", raw)
        self.assertEqual(mp.detect_kind(raw), "pipeline_dashboard")


class PipelineDashboard(unittest.TestCase):
    def setUp(self):
        self.data, self.keys, self.warnings = \
            mp.parse_pipeline_dashboard(_fixture("pipeline_dashboard.txt"))

    def test_headline_metrics(self):
        self.assertEqual(self.data["pct_nonstaged_48h"], 78.0)
        self.assertEqual(self.data["new_leads"], 444)
        self.assertEqual(self.data["window"], "09/28/2026 - 10/28/2026")
        self.assertEqual(self.data["rules_moved_leads"], 12)

    def test_all_eight_stages(self):
        stages = {s["stage"]: s for s in self.data["stages"]}
        self.assertEqual(len(stages), 8)
        self.assertEqual(stages["New / No Contact Made"]["count"], 345)
        self.assertEqual(stages["New / No Contact Made"]["pct"], 78.0)
        self.assertEqual(
            stages["Attempted Contact / Unresponsive"]["count"], 26)
        self.assertEqual(stages["Long Term Nurture"]["count"], 35)
        self.assertEqual(
            stages["Actively Listed / Showing Homes"]["count"], 38)
        self.assertEqual(stages["Closed / Under Contract"]["count"], 0)

    def test_no_warnings_on_real_payload(self):
        self.assertEqual(self.warnings, [])

    def test_empty_input_warns(self):
        data, keys, warnings = mp.parse_pipeline_dashboard("")
        self.assertEqual(data, {})
        self.assertTrue(warnings)


class RulesPage(unittest.TestCase):
    def test_empty_board_is_valid_zero_rows_no_warning(self):
        data, keys, warnings = mp.parse_rules_page(_fixture("rules_empty.txt"))
        self.assertEqual(data["groups"], [])
        self.assertEqual(data["rule_rows"], [])
        self.assertEqual(warnings, [])

    def test_group_summaries(self):
        raw = ("AGENT RULES & ALERTS\n"
               "New Lead Rules\n5% Past Due\n10% At Risk\n85% Complete\n"
               "Nurture Rules\n0% Past Due\n2% At Risk\n98% Complete\n"
               "3 (12%) Past Due\n")
        data, keys, warnings = mp.parse_rules_page(raw)
        self.assertEqual(len(data["groups"]), 2)
        self.assertEqual(data["groups"][0]["group"], "New Lead Rules")
        self.assertEqual(data["groups"][0]["complete_pct"], 85)
        self.assertEqual(data["rule_rows"],
                         [{"past_due": 3, "past_due_pct": 12}])
        self.assertEqual(warnings, [])


if __name__ == "__main__":
    unittest.main()
