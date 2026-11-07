"""Golden tests on the money paths (Nov 6 2026 audit, punch list #9).

These cover the pure logic that turns FUB's messy fields into dollars
and accountability: the percent-vs-dollar commission rule, source
bucketing and referral fees, admin-deal exclusion, and the Realtor.com
call ladder. They run without a database or FUB credentials (db is
stubbed), so CI can run them on every push.
"""
import sys
import types
import unittest

# revenue.py and realtor_ladder.py import db at module level; stub it so
# these tests run anywhere (CI, a laptop without Postgres creds).
_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

import revenue                      # noqa: E402
import realtor_ladder               # noqa: E402


class TestCommissionRule(unittest.TestCase):
    """commissionValue <= 10 on a priced deal is a PERCENT, else dollars.
    The rule that turned '3' on a $725k deal into $21,750, not $3."""

    def test_percent_of_price(self):
        self.assertEqual(revenue.gci_from(725000, 3), 21750.0)

    def test_three_percent_of_400k(self):
        self.assertEqual(revenue.gci_from(400000, 2.5), 10000.0)

    def test_dollar_amount_passes_through(self):
        self.assertEqual(revenue.gci_from(500000, 15000), 15000.0)

    def test_exactly_ten_is_percent(self):
        self.assertEqual(revenue.gci_from(300000, 10), 30000.0)

    def test_eleven_is_dollars(self):
        self.assertEqual(revenue.gci_from(300000, 11), 11.0)

    def test_small_value_without_price_is_dollars(self):
        # No price to take a percent of: pass the raw value through.
        self.assertEqual(revenue.gci_from(0, 3), 3.0)
        self.assertEqual(revenue.gci_from(None, 3), 3.0)

    def test_zero_and_none_are_none(self):
        self.assertIsNone(revenue.gci_from(500000, 0))
        self.assertIsNone(revenue.gci_from(500000, None))


class TestSourceBuckets(unittest.TestCase):
    """Source strings route to fee buckets; fees Barry set Nov 4 2026."""

    def test_ylopo_variants(self):
        self.assertEqual(revenue.bucket_for("Ylopo"), "ylopo")
        self.assertEqual(revenue.bucket_for("Ylopo PPC+"), "ylopo")

    def test_realtor_exact_source_string(self):
        # The shootout lane writes exactly "Realtor.com" (48h guard
        # depends on it); the bucket must catch it.
        self.assertEqual(revenue.bucket_for("Realtor.com"), "realtor.com")

    def test_probate_and_zbuyer(self):
        self.assertEqual(revenue.bucket_for("Probate Leads"), "probate")
        self.assertEqual(revenue.bucket_for("zBuyer"), "zbuyer")

    def test_empty_and_unknown_fall_to_other(self):
        self.assertEqual(revenue.bucket_for(""), "other")
        self.assertEqual(revenue.bucket_for(None), "other")
        self.assertEqual(revenue.bucket_for("Open House"), "other")

    def test_fee_table(self):
        self.assertEqual(revenue.REFERRAL_FEES["ylopo"], 0.40)
        self.assertEqual(revenue.REFERRAL_FEES["probate"], 0.25)
        self.assertEqual(revenue.REFERRAL_FEES["zbuyer"], 0.0)
        self.assertEqual(revenue.REFERRAL_FEES["realtor.com"], 0.0)

    def test_net_gci_math(self):
        gci = revenue.gci_from(725000, 3)
        net = round(gci * (1 - revenue.REFERRAL_FEES["ylopo"]), 2)
        self.assertEqual(net, 13050.0)


class TestAdminDealFilter(unittest.TestCase):
    """Dotloop onboarding loops are paperwork, never money."""

    def test_onboarding_loop_is_admin(self):
        self.assertTrue(revenue.is_admin_deal(
            {"name": "Legacy Onboarding - Jessica Mandley"}))

    def test_commission_split_is_admin(self):
        self.assertTrue(revenue.is_admin_deal(
            {"name": "Commission Split Agreement - Damon"}))

    def test_real_listing_is_not_admin(self):
        self.assertFalse(revenue.is_admin_deal(
            {"name": "3633 Britt Terrace - Purchase"}))

    def test_missing_name_is_not_admin(self):
        self.assertFalse(revenue.is_admin_deal({}))


class TestRealtorLadder(unittest.TestCase):
    """$80-a-lead accountability: cumulative calls due by day since claim."""

    def test_day_zero_is_two_calls(self):
        self.assertEqual(realtor_ladder.due_calls(0), 2)

    def test_full_step_function(self):
        expected = {0: 2, 1: 4, 2: 5, 3: 6, 4: 6, 5: 7, 6: 7,
                    7: 8, 8: 8, 9: 8, 10: 9, 11: 9, 12: 9, 13: 9}
        for day, due in expected.items():
            self.assertEqual(realtor_ladder.due_calls(day), due,
                             "day %d" % day)

    def test_day_fourteen_plus_hits_total(self):
        self.assertEqual(realtor_ladder.due_calls(14),
                         realtor_ladder.LADDER_TOTAL_CALLS)
        self.assertEqual(realtor_ladder.due_calls(30),
                         realtor_ladder.LADDER_TOTAL_CALLS)

    def test_ladder_is_monotonic(self):
        prev = 0
        for day in range(0, 20):
            cur = realtor_ladder.due_calls(day)
            self.assertGreaterEqual(cur, prev, "day %d regressed" % day)
            prev = cur

    def test_contact_thresholds(self):
        # A conversation is 120s outbound or 30s inbound; changing these
        # silently changes who the ladder stops chasing.
        self.assertEqual(realtor_ladder.CONTACT_CALL_SECS, 120)
        self.assertEqual(realtor_ladder.CONTACT_INBOUND_SECS, 30)
        self.assertEqual(realtor_ladder.LADDER_WINDOW_DAYS, 14)


if __name__ == "__main__":
    unittest.main()
