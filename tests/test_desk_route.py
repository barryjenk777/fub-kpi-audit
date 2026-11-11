"""Lead Desk qualification and delivery-log tests.

These lock the Nov 4 2026 truth test as it exists in code. They do not
decide whether a Ylopo note "From:" line should become a human-ready
transfer. Notes are not native texts; the gate does not read them.

Run: python3 -m unittest tests.test_desk_route
"""

import unittest
from datetime import datetime, timedelta, timezone

import desk_route as dr


NOW = datetime(2026, 11, 4, 16, 52, tzinfo=timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class NativeInboundGate(unittest.TestCase):
    def test_fresh_incoming_text_qualifies(self):
        text = {"isIncoming": True, "created": _iso(NOW - timedelta(minutes=20))}
        self.assertTrue(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_outbound_text_does_not_qualify(self):
        text = {"isIncoming": False, "created": _iso(NOW - timedelta(minutes=5))}
        self.assertFalse(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_missing_is_incoming_does_not_qualify(self):
        text = {"created": _iso(NOW - timedelta(minutes=5))}
        self.assertFalse(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_stale_incoming_text_does_not_qualify(self):
        text = {"isIncoming": True,
                "created": _iso(NOW - timedelta(hours=24, seconds=1))}
        self.assertFalse(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_boundary_24h_still_qualifies(self):
        text = {"isIncoming": True, "created": _iso(NOW - timedelta(hours=24))}
        self.assertTrue(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_ylopo_note_is_not_a_native_text(self):
        # Affirmative and declining note bodies are equally invisible.
        # The gate does not parse them, and must not be treated as doing so.
        affirmative = {
            "body": "From: Donn Franklin\nYes, please connect me with an agent",
            "created": _iso(NOW - timedelta(minutes=30)),
        }
        decline = {
            "body": "From: Ryan\nI'm fine, I will reach out if needed",
            "created": _iso(NOW - timedelta(minutes=20)),
        }
        self.assertFalse(dr.text_is_recent_inbound(affirmative, NOW, hours=24))
        self.assertFalse(dr.text_is_recent_inbound(decline, NOW, hours=24))

    def test_body_is_not_an_input(self):
        text = {"isIncoming": True, "message": "",
                "created": _iso(NOW - timedelta(minutes=1))}
        self.assertTrue(dr.text_is_recent_inbound(text, NOW, hours=24))

    def test_connected_inbound_call_qualifies(self):
        call = {"isIncoming": True, "duration": 402,
                "isOutgoing": None,
                "created": _iso(NOW - timedelta(minutes=2))}
        self.assertTrue(dr.call_is_recent_inbound(call, NOW, hours=24))

    def test_zero_duration_or_outbound_call_does_not_qualify(self):
        recent = _iso(NOW - timedelta(minutes=2))
        self.assertFalse(dr.call_is_recent_inbound(
            {"isIncoming": True, "duration": 0, "created": recent}, NOW, 24))
        self.assertFalse(dr.call_is_recent_inbound(
            {"isIncoming": False, "duration": 90, "created": recent}, NOW, 24))
        self.assertFalse(dr.call_is_recent_inbound(
            {"duration": 90, "isOutgoing": None, "created": recent}, NOW, 24))

    def test_garbage_timestamp_is_not_inbound(self):
        self.assertFalse(dr.text_is_recent_inbound(
            {"isIncoming": True, "created": ""}, NOW, 24))


class DeskDecisions(unittest.TestCase):
    def test_priority_only_owned_is_not_a_claim_even_with_a_note(self):
        # Ryan / Donn shape: Y_AI_PRIORITY, owned, no human-ask tag.
        # A note From-line is not passed in, because the gate cannot see it.
        route = dr.evaluate_desk_route(
            ["y_ai_priority"], live="1", owned=True, inbound_recent=False)
        self.assertFalse(route["claim_offer"])
        self.assertFalse(route["engaged"])
        self.assertEqual(route["lane"], "AI priority")
        self.assertEqual(route["detail"], "priority")
        self.assertEqual(route["reason"], "priority_signal_not_human_ask")

    def test_priority_only_native_inbound_still_not_a_human_ask(self):
        route = dr.evaluate_desk_route(
            ["Y_AI_PRIORITY"], live="1", owned=False, inbound_recent=True)
        self.assertFalse(route["claim_offer"])
        self.assertEqual(route["lane"], "AI priority")
        self.assertEqual(route["ask"], [])

    def test_human_ask_without_inbound_is_a_follow_up_not_a_claim(self):
        # Jon shape: unsuccessful / needs-follow-up voice tag, no native inbound.
        route = dr.evaluate_desk_route(
            ["isa_transfer_unsuccessful", "ai_voice_needs_follow_up"],
            live="1", owned=True, inbound_recent=False)
        self.assertFalse(route["claim_offer"])
        self.assertEqual(route["lane"], "AI follow-up attempt")
        self.assertEqual(route["detail"], "voice")
        self.assertEqual(route["source_kind"], "ai_voice")
        self.assertEqual(route["reason"], "human_ask_without_native_inbound_24h")

    def test_owned_human_ask_with_inbound_mirrors_and_does_not_offer(self):
        route = dr.evaluate_desk_route(
            ["ai_needs_follow_up"], live="1", owned=True, inbound_recent=True)
        self.assertFalse(route["claim_offer"])
        self.assertTrue(route["engaged"])
        self.assertEqual(route["lane"], "AI conversion")
        self.assertEqual(route["decision"], "mirror_owner")

    def test_unowned_human_ask_with_inbound_offers_when_live(self):
        route = dr.evaluate_desk_route(
            ["ai_needs_follow_up"], live="1", owned=False, inbound_recent=True)
        self.assertTrue(route["claim_offer"])
        self.assertEqual(route["source_kind"], "ai_text")
        self.assertEqual(route["reason"], "unowned_human_ask_native_inbound_24h")

    def test_same_lead_does_not_offer_when_desk_is_not_live(self):
        route = dr.evaluate_desk_route(
            ["ai_needs_follow_up"], live="0", owned=False, inbound_recent=True)
        self.assertFalse(route["claim_offer"])
        self.assertEqual(route["lane"], "AI conversion")
        self.assertEqual(route["reason"], "human_ask_native_inbound_desk_not_live")

    def test_sms_conversion_source_and_voice_precedence(self):
        sms = dr.evaluate_desk_route(
            ["sms_conversion"], live="1", owned=False, inbound_recent=True)
        self.assertEqual(sms["source_kind"], "blue_text")
        both = dr.evaluate_desk_route(
            ["sms_conversion", "ai_needs_follow_up"],
            live="1", owned=False, inbound_recent=True)
        self.assertEqual(both["source_kind"], "ai_text")
        voice = dr.evaluate_desk_route(
            ["isa_transfer_unsuccessful"],
            live="1", owned=False, inbound_recent=True)
        self.assertEqual(voice["source_kind"], "ai_voice")
        self.assertEqual(voice["detail"], "text")

    def test_decision_is_deterministic(self):
        kwargs = dict(fresh_tags=["ai_needs_follow_up"], live="1",
                      owned=False, inbound_recent=True)
        self.assertEqual(dr.evaluate_desk_route(**kwargs),
                         dr.evaluate_desk_route(**kwargs))


class DeliveryLog(unittest.TestCase):
    def test_line_is_one_record_and_has_no_body_slot(self):
        line = dr.routing_delivery_line(
            person_id=304402, event_class="new_lead_email",
            decision="not_sent", reason="send_exception",
            destination="email", attempted_at="2026-11-04T21:10:25+00:00",
            slack_ts="", error="NameError\nFrom: secret reply text")
        self.assertEqual(line.count("\n"), 0)
        self.assertIn("person_id=304402", line)
        self.assertIn("decision=not_sent", line)
        self.assertIn("error=NameError_From:_secret_reply_text", line)
        self.assertNotIn("message=", line)
        self.assertNotIn("body=", line)


if __name__ == "__main__":
    unittest.main()
