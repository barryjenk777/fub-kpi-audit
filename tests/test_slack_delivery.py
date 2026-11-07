"""Slack delivery ack for the cash-lane group DM fallback.

No live Slack calls. requests.post is replaced. The bot token is a fake.

Run: python3 -m unittest tests.test_slack_delivery
"""

import os
import unittest
from unittest.mock import patch

import slack_client


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


SECRET = "SECRET CONVERSATION TEXT that must not be logged"


class GroupDmFallback(unittest.TestCase):
    def setUp(self):
        self._token = os.environ.get("SLACK_BOT_TOKEN")
        os.environ["SLACK_BOT_TOKEN"] = "xoxb-test-not-a-real-token"

    def tearDown(self):
        if self._token is None:
            os.environ.pop("SLACK_BOT_TOKEN", None)
        else:
            os.environ["SLACK_BOT_TOKEN"] = self._token

    def _patch_posts(self, fail_channels=()):
        bodies = []

        def fake_post(url, **kwargs):
            body = kwargs.get("json") or {}
            bodies.append((url, body))
            if url.endswith("/conversations.open"):
                return _Resp({"ok": False, "error": "missing_scope"})
            channel = body.get("channel")
            if channel in fail_channels:
                return _Resp({"ok": False, "error": "channel_not_found"})
            return _Resp({"ok": True, "ts": "1500000000.%s" % (len(bodies))})

        return bodies, fake_post

    def test_missing_scope_falls_back_and_acks_only_real_posts(self):
        bodies, fake_post = self._patch_posts()
        with patch("slack_client.requests.post", fake_post), \
                self.assertLogs("slack", level="INFO") as logs:
            ts = slack_client.group_dm(["U_FHALEN", "U_ANA"], SECRET)
        self.assertTrue(ts)
        opened = [b for url, b in bodies if url.endswith("/conversations.open")]
        self.assertEqual(opened[0]["users"], "U_FHALEN,U_ANA")
        posted = [b.get("channel") for url, b in bodies
                  if url.endswith("/chat.postMessage")]
        self.assertIn("U_FHALEN", posted)
        self.assertIn("U_ANA", posted)
        blob = "\n".join(logs.output)
        self.assertNotIn(SECRET, blob)
        self.assertIn("missing_scope", blob)
        self.assertIn("decision=sent", blob)
        self.assertIn("slack_ts=%s" % ts, blob)

    def test_partial_fallback_is_not_a_full_success(self):
        _bodies, fake_post = self._patch_posts(fail_channels={"U_ANA"})
        with patch("slack_client.requests.post", fake_post), \
                self.assertLogs("slack", level="INFO") as logs:
            ts = slack_client.group_dm(["U_FHALEN", "U_ANA"], SECRET)
        self.assertTrue(ts)
        blob = "\n".join(logs.output)
        self.assertIn("error=partial", blob)
        self.assertIn("decision=not_sent", blob)
        self.assertNotIn(SECRET, blob)

    def test_total_fallback_failure_returns_no_ts(self):
        _bodies, fake_post = self._patch_posts(
            fail_channels={"U_FHALEN", "U_ANA"})
        with patch("slack_client.requests.post", fake_post), \
                self.assertLogs("slack", level="INFO") as logs:
            ts = slack_client.group_dm(["U_FHALEN", "U_ANA"], SECRET)
        self.assertIsNone(ts)
        blob = "\n".join(logs.output)
        self.assertIn("error=unacked", blob)
        self.assertNotIn("decision=sent", blob)
        self.assertNotIn(SECRET, blob)

    def test_direct_post_failure_does_not_return_a_ts(self):
        def fake_post(url, **kwargs):
            return _Resp({"ok": False, "error": "not_in_channel"})

        with patch("slack_client.requests.post", fake_post), \
                self.assertLogs("slack", level="INFO") as logs:
            ts = slack_client.post_message("C0TEST", SECRET)
        self.assertIsNone(ts)
        blob = "\n".join(logs.output)
        self.assertIn("decision=not_sent", blob)
        self.assertIn("error=not_in_channel", blob)
        self.assertNotIn(SECRET, blob)


if __name__ == "__main__":
    unittest.main()
