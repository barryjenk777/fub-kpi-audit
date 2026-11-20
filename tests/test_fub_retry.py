"""A slow FUB answer must not kill a scheduled run: reads retry, writes
never do (FUB may have applied a write before the line went quiet)."""
import os
import sys
import unittest
from unittest import mock

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import fub_client                      # noqa: E402


class _Ok:
    status_code = 200
    headers = {}

    def raise_for_status(self):
        pass

    def json(self):
        return {"ok": True}


def _client(side_effect):
    c = fub_client.FUBClient(api_key="test")
    c.session = mock.Mock()
    c.session.request.side_effect = side_effect
    return c


@mock.patch.object(fub_client.time, "sleep", lambda s: None)
class NetworkRetry(unittest.TestCase):
    def test_get_survives_a_read_timeout(self):
        c = _client([requests.exceptions.ReadTimeout("slow"), _Ok()])
        self.assertEqual(c._request("GET", "people"), {"ok": True})
        self.assertEqual(c.session.request.call_count, 2)

    def test_get_gives_up_after_three_tries(self):
        c = _client(requests.exceptions.ReadTimeout("slow"))
        with self.assertRaises(requests.exceptions.ReadTimeout):
            c._request("GET", "people")
        self.assertEqual(c.session.request.call_count, 3)

    def test_a_write_is_never_repeated(self):
        for method in ("POST", "PUT", "DELETE"):
            c = _client(requests.exceptions.ReadTimeout("slow"))
            with self.assertRaises(requests.exceptions.ReadTimeout):
                c._request(method, "people/1")
            self.assertEqual(c.session.request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
