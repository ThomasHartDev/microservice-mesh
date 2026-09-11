from __future__ import annotations

import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "notifications"))
sys.path.insert(0, str(ROOT / "python"))

from worker import (  # noqa: E402
    CREATED,
    Contact,
    MemoryDirectory,
    RecordingMailer,
    RecordingSms,
    Worker,
    health_request,
    parse_envelope,
)

ORDER = "550e8400-e29b-41d4-a716-446655440000"
CUSTOMER = "6ba7b814-9dad-11d1-80b4-00c04fd430c8"
CREATED_MID = "6ba7b815-9dad-11d1-80b4-00c04fd430c8"
CORR = "6ba7b816-9dad-11d1-80b4-00c04fd430c8"
AT = "2026-08-29T12:00:00.000Z"
TP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def created_env(**over: object) -> dict:
    body: dict = {
        "message_id": CREATED_MID,
        "correlation_id": CORR,
        "type": CREATED,
        "schema_version": "1.0.0",
        "occurred_at": AT,
        "source": "orders",
        "payload": {
            "order_id": ORDER,
            "customer_id": CUSTOMER,
            "items": [{"sku": "SKU-1", "quantity": 2, "unit_price_cents": 1500}],
            "currency": "USD",
            "total_cents": 3000,
        },
    }
    body.update(over)
    return body


class NotificationsWorkerTraceTest(unittest.TestCase):
    def test_valid_traceparent_continues_and_logs(self) -> None:
        env, errors = parse_envelope(created_env(traceparent=TP.upper()))
        self.assertEqual(errors, ())
        self.assertIsNotNone(env)
        self.assertEqual(env.traceparent, TP)
        directory = MemoryDirectory()
        directory.put(CUSTOMER, Contact("a@b.co", None))
        worker = Worker(directory, RecordingMailer(), RecordingSms(), new_id=lambda: "n1")
        buf = io.StringIO()
        with redirect_stdout(buf):
            out = worker.handle(created_env(traceparent=TP))
        self.assertEqual(out.kind, "ok")
        rec = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(rec["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736")
        self.assertEqual(rec["correlation_id"], CORR)
        self.assertNotEqual(rec["span_id"], "00f067aa0ba902b7")
        self.assertRegex(rec["span_id"], r"^[0-9a-f]{16}$")

    def test_rejects_all_zero_and_version_ff_traceparent(self) -> None:
        zero_trace = created_env(traceparent="00-00000000000000000000000000000000-00f067aa0ba902b7-01")
        zero_span = created_env(traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
        version_ff = created_env(traceparent="ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        for raw in (zero_trace, zero_span, version_ff):
            env, errors = parse_envelope(raw)
            self.assertIsNone(env)
            self.assertEqual(errors[0].path, "traceparent")
            worker = Worker(MemoryDirectory(), RecordingMailer(), RecordingSms())
            self.assertEqual(worker.handle(raw).kind, "rejected")

    def test_ready_returns_503_when_a_named_check_fails(self) -> None:
        down = [{"name": "process", "ok": True}, {"name": "broker", "ok": False}]
        status, body = health_request("GET", "/ready", down)
        self.assertEqual(status, 503)
        rec = json.loads(body)
        self.assertFalse(rec["ready"])
        self.assertEqual(rec["status"], "degraded")
        live_status, _ = health_request("GET", "/health", down)
        self.assertEqual(live_status, 200)
        ok_status, ok_body = health_request("GET", "/ready")
        self.assertEqual(ok_status, 200)
        self.assertTrue(json.loads(ok_body)["ready"])


if __name__ == "__main__":
    unittest.main()
