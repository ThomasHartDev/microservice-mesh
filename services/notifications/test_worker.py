from __future__ import annotations

import json
import threading
import unittest

from worker import (
    CANCELLED,
    COMPLETED,
    Contact,
    MemoryDirectory,
    PermanentError,
    RecordingMailer,
    RecordingSms,
    TransientError,
    Worker,
    parse_envelope,
)

A = "550e8400-e29b-41d4-a716-446655440000"
B = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
C = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
D = "6ba7b812-9dad-11d1-80b4-00c04fd430c8"
E = "6ba7b813-9dad-11d1-80b4-00c04fd430c8"
AT = "2026-08-29T12:00:00.000Z"


def env(typ: str, payload: dict, mid: str = A, source: str = "orders", **over: object) -> dict:
    body: dict = {
        "message_id": mid, "correlation_id": B, "type": typ, "schema_version": "1.0.0",
        "occurred_at": AT, "source": source, "payload": payload,
    }
    body.update(over)
    return body


def completed() -> dict:
    return {"order_id": A, "payment_id": C, "reservation_id": B, "total_cents": 3000}


def cancelled(comps: list[str] | None = None) -> dict:
    return {
        "order_id": A, "reason": "payment_failed",
        "compensations": comps if comps is not None else ["release_inventory", "notify_customer"],
    }


def make(email: str | None = "a@b.co", phone: str | None = "+15555550100"):
    directory = MemoryDirectory()
    directory.put(A, Contact(email, phone))
    mailer, sms = RecordingMailer(), RecordingSms()
    ids = iter(["n1", "n2", "n3"])
    return Worker(directory, mailer, sms, new_id=lambda: next(ids)), mailer, sms


class WorkerTest(unittest.TestCase):
    def test_completed_sends_email_and_sms(self) -> None:
        w, mailer, sms = make()
        out = w.handle(env(COMPLETED, completed(), mid=D))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "sent", "sms": "sent"})
        self.assertEqual(mailer.sent[0].to, "a@b.co")
        self.assertIn("3000", mailer.sent[0].body)
        self.assertEqual(sms.sent[0].to, "+15555550100")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=D)).kind, "replayed")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_cancel_email_only_when_notify_customer(self) -> None:
        w, mailer, sms = make()
        out = w.handle(env(CANCELLED, cancelled(), mid=D))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "sent", "sms": "skipped"})
        self.assertEqual(len(sms.sent), 0)
        self.assertIn("payment_failed", mailer.sent[0].body)

    def test_cancel_without_notify_skips(self) -> None:
        w, mailer, sms = make()
        out = w.handle(env(CANCELLED, cancelled(comps=["release_inventory"]), mid=D))
        self.assertEqual(out.kind, "skipped")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)

    def test_email_then_sms_retry_does_not_double_email(self) -> None:
        w, mailer, sms = make()
        sms.errors.append(TransientError("timeout"))
        first = w.handle(env(COMPLETED, completed(), mid=D))
        self.assertEqual(first.kind, "retry")
        self.assertEqual(first.dispatch.channels, {"email": "sent", "sms": "pending"})
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 0))
        second = w.handle(env(COMPLETED, completed(), mid=D))
        self.assertEqual(second.kind, "sent")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_permanent_email_failure_still_sends_sms(self) -> None:
        w, mailer, sms = make()
        mailer.errors.append(PermanentError("bounce"))
        out = w.handle(env(COMPLETED, completed(), mid=D))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "failed", "sms": "sent"})
        self.assertEqual((len(mailer.sent), len(sms.sent)), (0, 1))

    def test_completed_after_cancel_conflicts(self) -> None:
        w, mailer, _ = make()
        self.assertEqual(w.handle(env(CANCELLED, cancelled(), mid=D)).kind, "sent")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=E)).kind, "conflict")
        self.assertEqual(len(mailer.sent), 1)

    def test_missing_contact_skips_channels(self) -> None:
        w, mailer, sms = make(email=None, phone=None)
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=D)).kind, "skipped")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)

    def test_ignores_non_terminal_types(self) -> None:
        w, _, _ = make()
        raw = env("events.order_created", {"order_id": A, "customer_id": A})
        self.assertEqual(w.handle(raw).kind, "ignored")

    def test_rejects_poison_and_edges(self) -> None:
        w, _, _ = make()
        self.assertEqual(w.handle(None).kind, "rejected")
        self.assertEqual(w.handle("{").kind, "rejected")
        self.assertEqual(w.handle([]).kind, "rejected")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid="not-a-uuid")).kind, "rejected")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=D, source="payments")).kind, "rejected")
        extra = completed()
        extra["extra"] = 1
        self.assertEqual(w.handle(env(COMPLETED, extra, mid=D)).kind, "rejected")
        cents = completed()
        cents["total_cents"] = True
        self.assertEqual(w.handle(env(COMPLETED, cents, mid=D)).kind, "rejected")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=D, schema_version="9.0.0")).kind, "rejected")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=D, occurred_at="2026-08-29")).kind, "rejected")
        parsed, errs = parse_envelope(json.dumps(env(COMPLETED, completed(), mid=D)))
        self.assertFalse(errs)
        self.assertEqual(parsed.type, COMPLETED)

    def test_concurrent_redelivery_sends_once(self) -> None:
        w, mailer, sms = make()
        payload = env(COMPLETED, completed(), mid=D)
        results: list[str] = []

        def run() -> None:
            results.append(w.handle(payload).kind)

        threads = [threading.Thread(target=run) for _ in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))
        self.assertIn("sent", results)
        self.assertTrue(set(results) <= {"sent", "replayed"})


if __name__ == "__main__":
    unittest.main()
