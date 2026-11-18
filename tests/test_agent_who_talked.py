"""The agent who already talked to the lead keeps the lead (Nov 18 2026).

A Storage Pond lead called in and spoke with one agent for 12 minutes.
Eleven hours later she was offered to three other agents. A real phone
conversation with a working agent now decides the owner.
"""
import sys
import types
import unittest
from datetime import datetime, timezone

_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

try:
    import convo_ledger as cl
except Exception:                     # pragma: no cover
    cl = None

NOW = datetime(2026, 11, 18, 5, 43, tzinfo=timezone.utc)
WORKING = {"Agent One", "Agent Two"}


def _who(calls, **kw):
    return cl.agent_who_talked(calls, lambda n: n in WORKING, now=NOW, **kw)


@unittest.skipIf(cl is None, "convo_ledger needs the app's dependencies")
class TestAgentWhoTalked(unittest.TestCase):
    def test_the_nov_17_call(self):
        got = _who([{"userName": "Agent One", "duration": 707,
                     "isIncoming": True, "created": "2026-11-17T18:56:37Z"}])
        self.assertEqual(got["agent"], "Agent One")
        self.assertEqual(got["seconds"], 707)

    def test_short_calls_and_voicemails_do_not_count(self):
        self.assertIsNone(_who([{"userName": "Agent One", "duration": 15,
                                 "created": "2026-11-17T18:56:37Z"}]))
        self.assertIsNone(_who([{"userName": "Agent One", "duration": 119,
                                 "created": "2026-11-17T18:56:37Z"}]))

    def test_old_conversations_do_not_count(self):
        self.assertIsNone(_who([{"userName": "Agent One", "duration": 900,
                                 "created": "2026-11-15T18:56:37Z"}]))

    def test_only_working_agents_count(self):
        # Barry, Fhalen, paused agents and the AI line are not owners.
        self.assertIsNone(_who([{"userName": "Barry Jenkins", "duration": 900,
                                 "created": "2026-11-17T18:56:37Z"}]))
        self.assertIsNone(_who([{"userName": None, "duration": 900,
                                 "created": "2026-11-17T18:56:37Z"}]))

    def test_most_recent_conversation_wins(self):
        got = _who([
            {"userName": "Agent One", "duration": 300,
             "created": "2026-11-17T14:00:00Z"},
            {"userName": "Agent Two", "duration": 200,
             "created": "2026-11-17T20:00:00Z"},
        ])
        self.assertEqual(got["agent"], "Agent Two")

    def test_bad_records_are_skipped(self):
        self.assertIsNone(_who([{"userName": "Agent One", "duration": "x",
                                 "created": "nope"}, {}]))
        self.assertIsNone(_who(None))


if __name__ == "__main__":
    unittest.main()
