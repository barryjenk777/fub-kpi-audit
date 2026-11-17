"""After-hours rule for Realtor.com leads (Barry, Nov 17 2026).

Nobody calls or texts a lead between 9pm and 8am Eastern. A lead that
registers in that window skips the phone ring and goes to the whole team
in Slack; the claimer waits for the 8am release. And a phone-ring answer
never takes a lead a working agent already holds (the Nov 16 lead that was
claimed at 9:11pm and moved to a second agent by an 8am ring).

Pure functions only, so this runs without a clock, FUB or a database.
"""
import sys
import types
import unittest
from datetime import datetime

_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

import realtor_ladder as rl          # noqa: E402


def _at(h, m=0, day=16):
    return datetime(2026, 11, day, h, m)


class TestQuietWindow(unittest.TestCase):
    def test_window_edges(self):
        self.assertFalse(rl.is_quiet(_at(20, 59)))
        self.assertTrue(rl.is_quiet(_at(21, 0)))      # 9:00pm is closed
        self.assertTrue(rl.is_quiet(_at(21, 5)))      # the Nov 16 lead
        self.assertTrue(rl.is_quiet(_at(2, 30)))
        self.assertTrue(rl.is_quiet(_at(7, 59)))
        self.assertFalse(rl.is_quiet(_at(8, 0)))      # 8:00am is open
        self.assertFalse(rl.is_quiet(_at(12, 0)))

    def test_minutes_until_open(self):
        self.assertEqual(rl.minutes_until_open(_at(12, 0)), 0)
        self.assertEqual(rl.minutes_until_open(_at(21, 5)), 10 * 60 + 55)
        self.assertEqual(rl.minutes_until_open(_at(0, 0)), 8 * 60)
        self.assertEqual(rl.minutes_until_open(_at(7, 58)), 2)
        # Never a zero-length offer while still closed.
        self.assertEqual(rl.minutes_until_open(
            datetime(2026, 11, 16, 7, 59, 40)), 1)

    def test_last_open(self):
        self.assertEqual(rl.last_open(_at(8, 20, day=17)), _at(8, 0, day=17))
        self.assertEqual(rl.last_open(_at(19, 0, day=17)), _at(8, 0, day=17))
        self.assertEqual(rl.last_open(_at(6, 0, day=17)), _at(8, 0, day=16))


class TestCallClock(unittest.TestCase):
    def test_daytime_claim_keeps_its_real_age(self):
        # Claimed at 2pm, 7 minutes ago; 8am was 6 hours back.
        self.assertEqual(rl.call_clock(7, 367), (7, False))

    def test_overnight_claim_starts_at_8am(self):
        # Claimed 9:11pm; at 8:12am the claim is 661 minutes old but the
        # agent has only been allowed to dial for 12.
        self.assertEqual(rl.call_clock(661, 12), (12, True))

    def test_claim_made_just_before_close_is_held_too(self):
        # Claimed 8:57pm with no dial; the clock stopped at 9pm.
        self.assertEqual(rl.call_clock(668, 5), (5, True))

    def test_empty_values(self):
        self.assertEqual(rl.call_clock(None, 30), (0, False))


class TestNightOfferMarker(unittest.TestCase):
    def test_marker(self):
        self.assertTrue(rl.is_night_offer(
            "After hours. Paid Realtor.com lead.\nEvidence: created 9:05pm"))
        self.assertFalse(rl.is_night_offer(
            "Paid Realtor.com lead, shared with competing agents"))
        self.assertFalse(rl.is_night_offer(None))


class TestAnswerKeepsOwner(unittest.TestCase):
    def test_working_owner_keeps_the_lead(self):
        # The Nov 17 morning: one agent claimed, a second answered the ring.
        self.assertEqual(
            rl.answer_keeps_owner("Agent One", "Agent Two", False),
            "Agent One")

    def test_unowned_or_pond_owner_lets_the_answer_assign(self):
        self.assertIsNone(rl.answer_keeps_owner("", "Agent Two", True))
        self.assertIsNone(rl.answer_keeps_owner(None, "Agent Two", True))
        self.assertIsNone(
            rl.answer_keeps_owner("Barry Jenkins", "Agent Two", True))

    def test_owner_answering_their_own_lead(self):
        self.assertIsNone(
            rl.answer_keeps_owner("Agent Two", "Agent Two", False))


if __name__ == "__main__":
    unittest.main()
