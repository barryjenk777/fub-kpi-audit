"""Offer wording must match what happened to the lead (Nov 17 2026).

A lead reassigned under the 24 hour rule went out reading "live one, the
AI converted", a day after the conversation. The text now has three
stories and these tests keep them apart. Also pins the no-dash rule.
"""
import sys
import types
import unittest

_stub = types.ModuleType("db")
_stub.get_conn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db in tests"))
_stub.is_available = lambda: False
sys.modules.setdefault("db", _stub)

try:
    import dispatch
except Exception:                     # pragma: no cover
    dispatch = None


@unittest.skipIf(dispatch is None, "dispatch needs the app's dependencies")
class TestOfferText(unittest.TestCase):
    def _all(self):
        fresh = dispatch._offer_text(
            "fresh", "Jullan", "Robert in Newport News",
            "just replied to our AI by text", 5)
        waiting = dispatch._offer_text(
            "waiting", "Jullan", "Robert in Newport News", "", 5)
        nurture = dispatch._offer_text(
            "nurture", "Jullan", "Robert in Newport News", "", 5)
        return fresh, waiting, nurture

    def test_waiting_lead_never_reads_as_fresh(self):
        _fresh, waiting, _n = self._all()
        low = waiting.lower()
        for word in ("just", "live one", "converted", "new lead", "fresh"):
            self.assertNotIn(word, low)
        self.assertIn("still has not gotten a call", waiting)

    def test_fresh_lead_says_what_happened(self):
        fresh, _w, _n = self._all()
        self.assertIn("just replied to our AI by text", fresh)
        self.assertNotIn("converted", fresh.lower())

    def test_nurture_is_calm(self):
        _f, _w, nurture = self._all()
        self.assertIn("no fire drill", nurture)
        self.assertNotIn("just", nurture.lower())

    def test_a_held_lead_never_says_just(self):
        # Nov 18 2026: a lead proven 11 hours late went out at 12:43am
        # reading as if it had just happened.
        t = dispatch._offer_text("earlier", "Jullan", "Caren in Norfolk",
                                 "replied to our AI by text", 5)
        self.assertIn("earlier", t)
        self.assertNotIn("just", t.lower())
        self.assertNotIn("new lead", t.lower())

    def test_every_text_names_the_clock_and_has_no_dashes(self):
        for t in self._all():
            self.assertIn("5 minutes", t)
            self.assertNotIn("—", t)
            self.assertNotIn("–", t)
            self.assertLess(len(t), 240)


if __name__ == "__main__":
    unittest.main()
