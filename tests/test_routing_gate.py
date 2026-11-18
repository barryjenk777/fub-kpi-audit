"""The front door: one set of rules for every lane (Nov 18 2026).

Each test is an incident or a rule Barry stated. If a lane ever routes a
lead these tests say it should not, the rule leaked again.
"""
import unittest
from datetime import datetime, timezone

import routing_gate as g

DAY = datetime(2026, 11, 18, 10, 30)
NIGHT = datetime(2026, 11, 18, 0, 43)


def _lead(**kw):
    base = {"stage": "Lead", "source": "Ylopo", "tags": [],
            "assignedTo": ""}
    base.update(kw)
    return base


def _go(lane="ai_text", lead=None, now=DAY, **kw):
    kw.setdefault("proof_hours", 0)
    return g.decide(lane, lead or _lead(), now, **kw)


class TestAllowed(unittest.TestCase):
    def test_contact_and_trash_never_route(self):
        for stage in ("Contact", "Trash", "trash "):
            got = _go(lead=_lead(stage=stage))
            self.assertEqual(got["action"], g.BLOCK)
            self.assertEqual(got["question"], 1)

    def test_a_deal_in_progress_never_routes(self):
        self.assertEqual(_go(lead=_lead(stage="Under Contract"))["action"],
                         g.BLOCK)

    def test_a_lead_who_asked_us_to_stop_never_routes(self):
        got = _go(lead=_lead(tags=["HANDRAISER", "ai_opt_out"]))
        self.assertEqual(got["action"], g.BLOCK)
        self.assertIn("asked us to stop", got["reason"])

    def test_registry_tag_alone_does_not_block(self):
        # Barry, Nov 18 2026: these leads registered themselves.
        got = _go(lead=_lead(tags=["Y_DNC_REGISTRY_TRUE"]))
        self.assertEqual(got["action"], g.ROUTE)

    def test_sphere_is_never_reassigned(self):
        got = _go("reassign", _lead(source="Sphere", assignedTo="Agent One"),
                  owner_is_working=True)
        self.assertEqual(got["action"], g.BLOCK)


class TestProof(unittest.TestCase):
    def test_no_proof_no_route(self):
        self.assertEqual(_go(proof_hours=None)["question"], 2)

    def test_stale_conversion_never_routes(self):
        self.assertEqual(_go(proof_hours=72)["action"], g.BLOCK)

    def test_longer_timeline_lane_has_a_longer_window(self):
        self.assertEqual(_go("ai_nurture", proof_hours=10 * 24)["action"],
                         g.ROUTE)
        self.assertEqual(_go("ai_nurture", proof_hours=20 * 24)["action"],
                         g.BLOCK)

    def test_waiting_lanes_do_not_need_fresh_proof(self):
        self.assertEqual(_go("recovery", proof_hours=None)["action"],
                         g.ROUTE)

    def test_age_is_read_from_the_evidence_line(self):
        now = datetime(2026, 11, 18, 15, 0, tzinfo=timezone.utc)
        notes = ("Longer timeline\nEvidence: texted us 2026-11-17; "
                 "asked about 12 Oak St on 2026-11-02")
        self.assertEqual(g.proof_age_hours(notes, now), 24)
        self.assertIsNone(g.proof_age_hours(
            "Evidence on record: none. No inbound.", now))
        self.assertIsNone(g.proof_age_hours(None, now))


class TestClaim(unittest.TestCase):
    def test_the_agent_who_talked_keeps_the_lead(self):
        # Nov 17 2026: a 12 minute call with one agent, then the lead was
        # offered to three others.
        got = _go(talked_agent="Agent One")
        self.assertEqual(got["action"], g.KEEP)
        self.assertEqual(got["agent"], "Agent One")

    def test_an_owned_lead_is_never_offered_around_its_owner(self):
        got = _go(lead=_lead(assignedTo="Agent Two"), owner_is_working=True)
        self.assertEqual(got["action"], g.KEEP)
        self.assertEqual(got["agent"], "Agent Two")

    def test_reassign_needs_an_owner_who_has_not_talked(self):
        owned = _lead(assignedTo="Agent Two")
        self.assertEqual(_go("reassign", owned,
                             owner_is_working=True)["action"], g.ROUTE)
        self.assertEqual(_go("reassign", owned, owner_is_working=True,
                             talked_agent="Agent Two")["action"], g.KEEP)
        self.assertEqual(_go("reassign", _lead())["action"], g.BLOCK)


class TestTime(unittest.TestCase):
    def test_nothing_routes_between_9pm_and_8am(self):
        for hour in (21, 23, 0, 7):
            got = _go(now=datetime(2026, 11, 18, hour, 5))
            self.assertEqual(got["action"], g.HOLD, hour)
        for hour in (8, 12, 20):
            got = _go(now=datetime(2026, 11, 18, hour, 59))
            self.assertEqual(got["action"], g.ROUTE, hour)

    def test_a_dead_lead_is_blocked_not_held(self):
        # Order matters: a Trash lead at midnight must not come back at 8am.
        got = _go(lead=_lead(stage="Trash"), now=NIGHT)
        self.assertEqual(got["action"], g.BLOCK)

    def test_every_reason_is_plain_and_has_no_dashes(self):
        cases = [_go(), _go(now=NIGHT), _go(proof_hours=None),
                 _go(lead=_lead(stage="Contact")),
                 _go(talked_agent="Agent One")]
        for c in cases:
            self.assertTrue(c["reason"])
            self.assertNotIn("—", c["reason"])
            self.assertNotIn("–", c["reason"])


if __name__ == "__main__":
    unittest.main()
