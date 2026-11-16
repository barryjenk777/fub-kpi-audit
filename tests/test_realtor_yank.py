"""A yanked Realtor.com claim must be able to change hands (Nov 16 2026).

The claim-to-call enforcer yanks a claim after REALTOR_CALL_YANK_MIN with
no dial and reopens the shootout to the rest of the roster. The yank only
marks our own offer row terminal; FUB keeps the yanked agent's name on
the lead. Two guards used to read that as "someone owns this" and refuse
the second claimer:

  1. db.resolve_dispatch_offer's one-winner-per-lead check counted the
     yanked row (accepted inside the last hour) as a winner.
  2. app._dispatch_accept's ownership re-check saw the yanked agent on
     assignedTo in FUB and blocked the reassignment.

These tests pin both. They run without Postgres or FUB: the ownership
decision is a pure function, and the SQL guards run against a fake
cursor on the real db module.
"""
import importlib.util
import os
import sys
import types
import unittest
from contextlib import contextmanager

# realtor_ladder imports db at module level; stub it the same way
# test_money_paths does so either file can load first.
_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

import realtor_ladder               # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_real_db():
    """The real db.py under a private name, so the stub above stays in
    place for every other test module in the same run."""
    spec = importlib.util.spec_from_file_location(
        "db_real_for_yank_tests", os.path.join(_ROOT, "db.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeCursor:
    def __init__(self, results):
        self.results = list(results)
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.results.pop(0) if self.results else None

    def fetchall(self):
        return self.results.pop(0) if self.results else []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur


def _wire(db, cur):
    @contextmanager
    def _conn():
        yield _FakeConn(cur)
    db.get_conn = _conn
    db.is_available = lambda: True


class TestOwnerBlocksClaim(unittest.TestCase):
    """The accept re-check decision, FUB owner vs the agent claiming."""

    def test_working_owner_still_blocks(self):
        # The Nov 2026 audit rule is untouched: never overwrite a working
        # agent FUB put on the lead since the offer went out.
        self.assertTrue(realtor_ladder.owner_blocks_claim(
            "Agent One", "Agent Two", False, set()))

    def test_yanked_claimer_does_not_block(self):
        self.assertFalse(realtor_ladder.owner_blocks_claim(
            "Agent One", "Agent Two", False, {"Agent One"}))

    def test_a_different_yanked_agent_does_not_unblock(self):
        # Only the released agent's own name is waved through.
        self.assertTrue(realtor_ladder.owner_blocks_claim(
            "Agent Three", "Agent Two", False, {"Agent One"}))

    def test_pond_dark_owner_and_self_never_block(self):
        self.assertFalse(realtor_ladder.owner_blocks_claim(
            None, "Agent Two", True, set()))
        self.assertFalse(realtor_ladder.owner_blocks_claim(
            "", "Agent Two", True, set()))
        self.assertFalse(realtor_ladder.owner_blocks_claim(
            "Barry Jenkins", "Agent Two", True, set()))
        self.assertFalse(realtor_ladder.owner_blocks_claim(
            "Agent Two", "Agent Two", False, set()))

    def test_empty_released_list_fails_closed(self):
        # released_claimers returns an empty set on any error, and an
        # empty set must leave the block standing.
        self.assertTrue(realtor_ladder.owner_blocks_claim(
            "Agent One", "Agent Two", False, None))


@unittest.skipIf(sys.version_info < (3, 10),
                 "db.py uses 3.10 syntax; CI runs this on 3.12")
class TestYankSql(unittest.TestCase):
    """The two queries that decide whether a yanked lead can be claimed."""

    def setUp(self):
        self.db = _load_real_db()

    def test_one_winner_guard_ignores_yanked_claims(self):
        # person lookup, no live winner found, then the UPDATE's latency.
        cur = _FakeCursor([("123",), None, (42.0,)])
        _wire(self.db, cur)
        self.assertEqual(self.db.resolve_dispatch_offer(7, "accepted"), 42.0)
        guard = next(s for s, _ in cur.sql
                     if "accepted_at > NOW() - INTERVAL '1 hour'" in s)
        self.assertIn("terminal = FALSE", guard)

    def test_one_winner_guard_still_refuses_a_live_winner(self):
        cur = _FakeCursor([("123",), (1,)])
        _wire(self.db, cur)
        self.assertIsNone(self.db.resolve_dispatch_offer(7, "accepted"))
        self.assertFalse(any(s.startswith("UPDATE") for s, _ in cur.sql))

    def test_released_claimers_query(self):
        cur = _FakeCursor([[("Agent One",), (None,)]])
        _wire(self.db, cur)
        self.assertEqual(self.db.released_claimers("123"), {"Agent One"})
        sql, params = cur.sql[0]
        # Released means: claimed, yanked, and still no dial. A late dial
        # stamps first_call_at and makes them a working owner again.
        self.assertIn("accepted_at IS NOT NULL", sql)
        self.assertIn("terminal = TRUE", sql)
        self.assertIn("first_call_at IS NULL", sql)
        self.assertEqual(params[:2], ("123", "realtor"))

    def test_released_claimers_fails_closed(self):
        self.db.is_available = lambda: True

        def _boom():
            raise RuntimeError("db down")
        self.db.get_conn = _boom
        self.assertEqual(self.db.released_claimers("123"), set())


if __name__ == "__main__":
    unittest.main()
