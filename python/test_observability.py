from __future__ import annotations

import json
import unittest

from observability import (
    continue_from,
    format_log,
    format_traceparent,
    handle_health_request,
    health_report,
    parse_traceparent,
    start_span,
)

TP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


class ObservabilityTest(unittest.TestCase):
    def test_traceparent_logs_and_health(self) -> None:
        ctx = parse_traceparent(TP.upper())
        self.assertEqual(format_traceparent(ctx), TP)
        self.assertTrue(ctx["sampled"])
        self.assertIsNone(parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"))
        self.assertIsNone(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"))
        self.assertIsNone(parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
        root = start_span(None)
        child = start_span(root)
        self.assertEqual(child["traceId"], root["traceId"])
        self.assertEqual(child["parentSpanId"], root["spanId"])
        hop = continue_from(TP)
        self.assertEqual(hop["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736")
        rec = json.loads(
            format_log(level="info", service="notifications", msg="sent", span=hop, ts="2026-09-11T00:00:00.000Z")
        )
        self.assertEqual(rec["trace_id"], hop["traceId"])
        self.assertEqual(rec["parent_span_id"], hop["parentSpanId"])
        down = [{"name": "process", "ok": True}, {"name": "nats", "ok": False}]
        self.assertFalse(health_report("notifications", down)["ready"])
        self.assertEqual(handle_health_request("GET", "/health", "notifications", down)[0], 200)
        self.assertEqual(handle_health_request("GET", "/ready", "notifications", down)[0], 503)
        self.assertEqual(handle_health_request("POST", "/health", "notifications", down)[0], 404)


if __name__ == "__main__":
    unittest.main()
