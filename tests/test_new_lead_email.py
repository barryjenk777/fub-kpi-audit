"""New-lead immediate email: recipient resolution, ack, and retry.

Hermetic. No SendGrid, FUB, Slack, or database. External send functions
are replaced before the mailer runs.

Run: python3 -m unittest tests.test_new_lead_email
"""

import inspect
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pond_mailer


LEAD = "lead@example.test"
OVERRIDE = "qa-redirect@example.test"


class _MemDB:
    def __init__(self):
        self.rows = []
        self.events = []
        self._next = 1

    def ensure_pond_email_log_table(self):
        return None

    def has_received_new_lead_immediate(self, person_id):
        return any(r["person_id"] == person_id and not r["deleted"]
                   for r in self.rows)

    def has_received_pond_sms(self, person_id):
        return False

    def get_lead_email_history(self, person_id):
        return {"emails_sent": 0}

    def log_pond_email(self, **kwargs):
        row_id = self._next
        self._next += 1
        self.rows.append({"id": row_id, "deleted": False, **kwargs})
        return row_id

    def delete_pond_email_log(self, log_id):
        for row in self.rows:
            if row["id"] == log_id and not row["deleted"]:
                row["deleted"] = True
                return True
        return False

    def update_pond_email_sg_id(self, log_id, sg_message_id):
        for row in self.rows:
            if row["id"] == log_id:
                row["sg_message_id"] = sg_message_id
        return None

    def log_automation_event(self, **kwargs):
        self.events.append(kwargs)
        return len(self.events)


def _person(**extra):
    created = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    base = {
        "id": 304402,
        "firstName": "Test",
        "lastName": "Lead",
        "created": created,
        "tags": [],
        "emails": [{"value": LEAD, "isPrimary": True}],
        "phones": [],
    }
    base.update(extra)
    return base


class _FakeFUB:
    people = []

    def __init__(self, *args, **kwargs):
        pass

    def get_people(self, **kwargs):
        return list(self.people)

    def get_events_for_person(self, *args, **kwargs):
        return []


class Recipient(unittest.TestCase):
    def test_production_uses_the_lead_address(self):
        self.assertEqual(pond_mailer.new_lead_email_recipient(LEAD, None), LEAD)
        self.assertEqual(pond_mailer.new_lead_email_recipient(LEAD, ""), LEAD)

    def test_explicit_override_is_opt_in(self):
        self.assertEqual(
            pond_mailer.new_lead_email_recipient(LEAD, OVERRIDE), OVERRIDE)

    def test_mailer_signature_defines_the_override(self):
        params = inspect.signature(pond_mailer.run_new_lead_mailer).parameters
        self.assertIn("to_override", params)
        self.assertIsNone(params["to_override"].default)


class DeliverAck(unittest.TestCase):
    def test_provider_exception_removes_the_cooldown_row(self):
        mem = _MemDB()

        def boom(*args, **kwargs):
            raise RuntimeError("provider down")

        with self.assertRaises(RuntimeError):
            pond_mailer.deliver_new_lead_email(
                pid=1, name="Test Lead", to_email=LEAD, subject="hi",
                body_text="body", body_html=None, behavior={},
                dry_run=False, send_fn=boom, db=mem)
        self.assertTrue(all(r["deleted"] for r in mem.rows))
        self.assertFalse(mem.has_received_new_lead_immediate(1))
        self.assertEqual(mem.events[-1]["success"], False)
        self.assertEqual(mem.events[-1]["payload"]["decision"], "not_sent")
        self.assertNotIn(LEAD, str(mem.events[-1]["payload"]))

    def test_unacknowledged_result_is_not_a_send(self):
        mem = _MemDB()

        def queued(*args, **kwargs):
            return {"status": "queued", "sg_message_id": None}

        with self.assertRaises(RuntimeError):
            pond_mailer.deliver_new_lead_email(
                pid=1, name="Test Lead", to_email=LEAD, subject="hi",
                body_text="body", body_html=None, behavior={},
                dry_run=False, send_fn=queued, db=mem)
        self.assertFalse(mem.has_received_new_lead_immediate(1))
        self.assertEqual(mem.events[-1]["payload"]["reason"],
                         "provider_not_acknowledged")

    def test_acknowledged_send_keeps_the_cooldown_row(self):
        mem = _MemDB()
        seen = {}

        def ok(to_email, subject, body_text, body_html, dry_run=False):
            seen["to"] = to_email
            return {"status": "sent", "sg_message_id": "sg-test"}

        out = pond_mailer.deliver_new_lead_email(
            pid=1, name="Test Lead", to_email=LEAD, subject="hi",
            body_text="body", body_html=None, behavior={"view_count": 0},
            dry_run=False, to_override=OVERRIDE, send_fn=ok, db=mem)
        self.assertEqual(seen["to"], OVERRIDE)
        self.assertEqual(out["result"]["status"], "sent")
        self.assertTrue(mem.has_received_new_lead_immediate(1))
        self.assertEqual(mem.rows[0]["email_address"], LEAD)
        self.assertEqual(mem.events[-1]["payload"]["decision"], "sent")


class MailerLoop(unittest.TestCase):
    def _run(self, mem, people, send_fn, to_override=None):
        _FakeFUB.people = people

        def _generated(*args, **kwargs):
            return {"subject_options": ["quick question"],
                    "body": "Test,\n\nHermetic body."}

        with patch("fub_client.FUBClient", _FakeFUB), \
                patch("pond_mailer.generate_new_lead_email", _generated), \
                patch("db.ensure_pond_email_log_table",
                      mem.ensure_pond_email_log_table), \
                patch("db.has_received_new_lead_immediate",
                      mem.has_received_new_lead_immediate), \
                patch("db.has_received_pond_sms", mem.has_received_pond_sms), \
                patch("db.get_lead_email_history", mem.get_lead_email_history), \
                patch("db.log_pond_email", mem.log_pond_email), \
                patch("db.delete_pond_email_log", mem.delete_pond_email_log), \
                patch("db.update_pond_email_sg_id", mem.update_pond_email_sg_id), \
                patch("db.log_automation_event", mem.log_automation_event), \
                patch("pond_mailer.send_email", send_fn):
            return pond_mailer.run_new_lead_mailer(
                dry_run=True, to_override=to_override)

    def test_failed_send_can_be_retried_and_success_is_once(self):
        mem = _MemDB()
        calls = {"n": 0}

        def flaky(to_email, subject, body_text, body_html, dry_run=False):
            calls["n"] += 1
            calls["to"] = to_email
            if calls["n"] == 1:
                raise RuntimeError("name 'to_override' is not defined")
            return {"status": "dry_run", "sg_message_id": None}

        person = _person()
        self._run(mem, [person], flaky)
        self.assertEqual(calls["n"], 1)
        self.assertFalse(mem.has_received_new_lead_immediate(person["id"]))
        self.assertTrue(any(e.get("payload", {}).get("decision") == "not_sent"
                            for e in mem.events))

        self._run(mem, [person], flaky)
        self.assertEqual(calls["n"], 2)
        self.assertEqual(calls["to"], LEAD)
        self.assertTrue(mem.has_received_new_lead_immediate(person["id"]))

        self._run(mem, [person], flaky)
        self.assertEqual(calls["n"], 2)

    def test_opt_out_and_already_owned_do_not_send(self):
        mem = _MemDB()
        calls = []

        def send_fn(*args, **kwargs):
            calls.append(args)
            return {"status": "dry_run", "sg_message_id": None}

        self._run(mem, [_person(tags=["NO_EMAIL"])], send_fn)
        self._run(mem, [_person(tags=["DO_NOT_CALL"], id=2)], send_fn)
        self._run(mem, [_person(assignedUserId=99, id=3)], send_fn)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
