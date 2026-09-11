from __future__ import annotations

import json
import re
import secrets
from datetime import datetime, timezone
from typing import Mapping

TRACE_RE = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$", re.I)
ZERO_TRACE, ZERO_SPAN = "00000000000000000000000000000000", "0000000000000000"


def parse_traceparent(header: str) -> dict[str, object] | None:
    m = TRACE_RE.fullmatch(header.strip())
    if not m:
        return None
    version, trace_id, span_id, flags = (p.lower() for p in m.groups())
    if version == "ff" or trace_id == ZERO_TRACE or span_id == ZERO_SPAN:
        return None
    return {"traceId": trace_id, "spanId": span_id, "sampled": (int(flags, 16) & 1) == 1}


def format_traceparent(ctx: Mapping[str, object]) -> str:
    return f"00-{ctx['traceId']}-{ctx['spanId']}-{'01' if ctx.get('sampled') else '00'}"


def _rand(size: int, zero: str) -> str:
    out = secrets.token_hex(size)
    return out if out != zero else out[:-1] + "1"


def start_span(parent: Mapping[str, object] | None) -> dict[str, object]:
    span_id = _rand(8, ZERO_SPAN)
    if not parent:
        return {"traceId": _rand(16, ZERO_TRACE), "spanId": span_id, "sampled": True}
    return {
        "traceId": parent["traceId"],
        "spanId": span_id,
        "parentSpanId": parent["spanId"],
        "sampled": bool(parent.get("sampled")),
    }


def continue_from(header: str | None) -> dict[str, object]:
    return start_span(parse_traceparent(header) if header else None)


def format_log(
    *,
    level: str,
    service: str,
    msg: str,
    span: Mapping[str, object] | None = None,
    correlation_id: str | None = None,
    ts: str | None = None,
    fields: Mapping[str, str] | None = None,
) -> str:
    rec: dict[str, object] = {
        "ts": ts or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "level": level,
        "service": service,
        "msg": msg,
    }
    if span:
        rec["trace_id"] = str(span["traceId"])
        rec["span_id"] = str(span["spanId"])
        if span.get("parentSpanId"):
            rec["parent_span_id"] = str(span["parentSpanId"])
    if correlation_id:
        rec["correlation_id"] = correlation_id
    if fields:
        rec.update(fields)
    return json.dumps(rec, separators=(",", ":"))


def health_report(service: str, checks: list[dict[str, object]]) -> dict[str, object]:
    ready = all(bool(c.get("ok")) for c in checks)
    return {
        "status": "ok" if ready else "degraded",
        "live": True,
        "ready": ready,
        "service": service,
        "checks": list(checks),
    }


def handle_health_request(
    method: str, url: str, service: str, checks: list[dict[str, object]]
) -> tuple[int, str]:
    path = url.split("?", 1)[0]
    if method != "GET" or path not in ("/health", "/ready"):
        return 404, json.dumps({"error": "not_found"}, separators=(",", ":"))
    report = health_report(service, checks)
    status = 200 if path == "/health" or report["ready"] else 503
    return status, json.dumps(report, separators=(",", ":"))
