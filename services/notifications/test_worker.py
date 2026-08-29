from __future__ import annotations

import json
import threading
import unittest

from worker import (
    CANCELLED,
    COMPLETED,
    CREATED,
    Contact,
    MemoryDirectory,
    PermanentError,
    RecordingMailer,
    RecordingSms,
    TransientError,
    Worker,
    parse_envelope,
)

ORDER = "550e8400-e29b-41d4-a716-446655440000"
CUSTOMER = "6ba7b814-9dad-11d1-80b4-00c04fd430c8"
RESERVATION = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
PAYMENT = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
TERM_MID = "6ba7b812-9dad-11d1-80b4-00c04fd430c8"
OTHER_MID = "6ba7b813-9dad-11d1-80b4-00c04fd430c8"
CREATED_MID = "6ba7b815-9dad-11d1-80b4-00c04fd430c8"
CORR = "6ba7b816-9dad-11d1-80b4-00c04fd430c8"
AT = "2026-08-29T12:00:00.000Z"


def env(typ: str, payload: dict, mid: str = TERM_MID, source: str = "orders", **over: object) -> dict:
    body: dict = {
        "message_id": mid, "correlation_id": CORR, "type": typ, "schema_version": "1.0.0",
        "occurred_at": AT, "source": source, "payload": payload,
    }
    body.update(over)
    return body


def created() -> dict:
    return {
        "order_id": ORDER,
        "customer_id": CUSTOMER,
        "items": [{"sku": "SKU-1", "quantity": 2, "unit_price_cents": 1500}],
        "currency": "USD",
        "total_cents": 3000,
    }


def completed() -> dict:
    return {
        "order_id": ORDER, "payment_id": PAYMENT, "reservation_id": RESERVATION, "total_cents": 3000,
    }


def cancelled(comps: list[str] | None = None) -> dict:
    return {
        "order_id": ORDER, "reason": "payment_failed",
        "compensations": comps if comps is not None else ["release_inventory", "notify_customer"],
    }


def make(email: str | None = "a@b.co", phone: str | None = "+15555550100"):
    directory = MemoryDirectory()
    directory.put(CUSTOMER, Contact(email, phone))
    mailer, sms = RecordingMailer(), RecordingSms()
    ids = iter(["n1", "n2", "n3"])
    return Worker(directory, mailer, sms, new_id=lambda: next(ids)), mailer, sms


def seed_created(w: Worker, mid: str = CREATED_MID):
    return w.handle(env(CREATED, created(), mid=mid))


class WorkerTest(unittest.TestCase):
    def test_completed_sends_email_and_sms(self) -> None:
        w, mailer, sms = make()
        self.assertEqual(seed_created(w).kind, "ok")
        out = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "sent", "sms": "sent"})
        self.assertEqual(mailer.sent[0].to, "a@b.co")
        self.assertIn("3000", mailer.sent[0].body)
        self.assertEqual(sms.sent[0].to, "+15555550100")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=TERM_MID)).kind, "replayed")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_cancel_email_only_when_notify_customer(self) -> None:
        w, mailer, sms = make()
        seed_created(w)
        out = w.handle(env(CANCELLED, cancelled(), mid=TERM_MID))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "sent", "sms": "skipped"})
        self.assertEqual(len(sms.sent), 0)
        self.assertIn("payment_failed", mailer.sent[0].body)

    def test_cancel_without_notify_skips(self) -> None:
        w, mailer, sms = make()
        seed_created(w)
        out = w.handle(env(CANCELLED, cancelled(comps=["release_inventory"]), mid=TERM_MID))
        self.assertEqual(out.kind, "skipped")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)

    def test_email_then_sms_retry_does_not_double_email(self) -> None:
        w, mailer, sms = make()
        seed_created(w)
        sms.errors.append(TransientError("timeout"))
        first = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(first.kind, "retry")
        self.assertEqual(first.dispatch.channels, {"email": "sent", "sms": "pending"})
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 0))
        second = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(second.kind, "sent")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_permanent_email_failure_still_sends_sms(self) -> None:
        w, mailer, sms = make()
        seed_created(w)
        mailer.errors.append(PermanentError("bounce"))
        out = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(out.kind, "sent")
        self.assertEqual(out.dispatch.channels, {"email": "failed", "sms": "sent"})
        self.assertEqual((len(mailer.sent), len(sms.sent)), (0, 1))

    def test_completed_after_cancel_conflicts(self) -> None:
        w, mailer, _ = make()
        seed_created(w)
        self.assertEqual(w.handle(env(CANCELLED, cancelled(), mid=TERM_MID)).kind, "sent")
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=OTHER_MID)).kind, "conflict")
        self.assertEqual(len(mailer.sent), 1)

    def test_missing_contact_skips_channels(self) -> None:
        w, mailer, sms = make(email=None, phone=None)
        seed_created(w)
        self.assertEqual(w.handle(env(COMPLETED, completed(), mid=TERM_MID)).kind, "skipped")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)

    def test_completed_before_created_sends_once_after_join(self) -> None:
        w, mailer, sms = make()
        first = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(first.kind, "parked")
        self.assertIsNone(first.dispatch)
        view = w.store.orders[ORDER]
        self.assertIsNone(view.terminal)
        self.assertEqual(view.parked.message_id, TERM_MID)
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)
        joined = seed_created(w)
        self.assertEqual(joined.kind, "sent")
        self.assertEqual(joined.dispatch.channels, {"email": "sent", "sms": "sent"})
        self.assertEqual(mailer.sent[0].to, "a@b.co")
        self.assertEqual(sms.sent[0].to, "+15555550100")
        self.assertEqual(view.customer_id, CUSTOMER)
        self.assertIsNone(view.parked)
        self.assertEqual(view.terminal, "completed")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_created_then_completed_sends_immediately(self) -> None:
        w, mailer, sms = make()
        self.assertEqual(seed_created(w).kind, "ok")
        self.assertEqual(w.store.orders[ORDER].customer_id, CUSTOMER)
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)
        out = w.handle(env(COMPLETED, completed(), mid=TERM_MID))
        self.assertEqual(out.kind, "sent")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_parked_completed_redelivery_does_not_double_send(self) -> None:
        w, mailer, sms = make()
        payload = env(COMPLETED, completed(), mid=TERM_MID)
        self.assertEqual(w.handle(payload).kind, "parked")
        self.assertEqual(w.handle(payload).kind, "parked")
        view = w.store.orders[ORDER]
        self.assertEqual(view.parked.message_id, TERM_MID)
        self.assertIsNone(view.terminal)
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)
        self.assertEqual(seed_created(w).kind, "sent")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))
        self.assertEqual(w.handle(payload).kind, "replayed")
        self.assertEqual((len(mailer.sent), len(sms.sent)), (1, 1))

    def test_cancel_without_notify_skips_after_created(self) -> None:
        w, mailer, sms = make()
        cancel = env(CANCELLED, cancelled(comps=["release_inventory"]), mid=TERM_MID)
        self.assertEqual(w.handle(cancel).kind, "parked")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)
        self.assertEqual(seed_created(w).kind, "skipped")
        self.assertEqual(len(mailer.sent) + len(sms.sent), 0)
        self.assertEqual(w.store.orders[ORDER].terminal, "cancelled")
        self.assertIsNone(w.store.orders[ORDER].parked)

    def test_ignores_non_terminal_types(self) -> None:
        w, _, _ = make()
        raw = env(
            "events.inventory_reserved",
            {
                "order_id": ORDER,
                "reservation_id": RESERVATION,
                "items": [{"sku": "SKU-1", "quantity": 2, "warehouse_id": "wh-east"}],
            },
            mid=TERM_MID,
            source="inventory",
        )
        self.assertEqual(w.handle(raw).kind, "ignored")

    def test_rejects_poison_and_edges(self) -> None:
        w, _, _ = make()

        def rejected(raw: object, path: str) -> None:
            out = w.handle(raw)
            self.assertEqual(out.kind, "rejected")
            self.assertTrue(out.errors)
            self.assertEqual(out.errors[0].path, path)

        rejected(None, "$")
        rejected("{", "$")
        rejected([], "$")
        rejected(env(COMPLETED, completed(), mid="not-a-uuid"), "message_id")
        rejected(env(COMPLETED, completed(), mid=TERM_MID, source="payments"), "source")
        extra = completed()
        extra["extra"] = 1
        rejected(env(COMPLETED, extra, mid=TERM_MID), "payload.extra")
        cents = completed()
        cents["total_cents"] = True
        rejected(env(COMPLETED, cents, mid=TERM_MID), "payload.total_cents")
        rejected(env(COMPLETED, completed(), mid=TERM_MID, schema_version="9.0.0"), "schema_version")
        rejected(env(COMPLETED, completed(), mid=TERM_MID, occurred_at="2026-08-29"), "occurred_at")
        rejected(env(COMPLETED, completed(), mid=TERM_MID, causation_id="not-a-uuid"), "causation_id")

        empty_items = created()
        empty_items["items"] = []
        rejected(env(CREATED, empty_items, mid=CREATED_MID), "payload.items")
        bonus = created()
        bonus["bonus"] = True
        rejected(env(CREATED, bonus, mid=CREATED_MID), "payload.bonus")
        missing = created()
        del missing["customer_id"]
        rejected(env(CREATED, missing, mid=CREATED_MID), "payload.customer_id")
        qty = created()
        qty["items"] = [{"sku": "SKU-1", "quantity": 0, "unit_price_cents": 1500}]
        rejected(env(CREATED, qty, mid=CREATED_MID), "payload.items[0].quantity")
        currency = created()
        currency["currency"] = "JPY"
        rejected(env(CREATED, currency, mid=CREATED_MID), "payload.currency")
        negative = created()
        negative["total_cents"] = -1
        rejected(env(CREATED, negative, mid=CREATED_MID), "payload.total_cents")
        rejected(env(CREATED, created(), mid=CREATED_MID, source="payments"), "source")

        parsed, errs = parse_envelope(json.dumps(env(COMPLETED, completed(), mid=TERM_MID)))
        self.assertFalse(errs)
        self.assertEqual(parsed.type, COMPLETED)
        parsed_created, created_errs = parse_envelope(json.dumps(env(CREATED, created(), mid=CREATED_MID)))
        self.assertFalse(created_errs)
        self.assertEqual(parsed_created.type, CREATED)

    def test_concurrent_redelivery_sends_once(self) -> None:
        w, mailer, sms = make()
        seed_created(w)
        payload = env(COMPLETED, completed(), mid=TERM_MID)
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
