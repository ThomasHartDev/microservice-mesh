from __future__ import annotations

import json
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I
)
DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
SCHEMA = "1.0.0"
SOURCES = frozenset({"gateway", "orders", "inventory", "payments", "notifications"})
CANCEL_REASONS = frozenset({"inventory_failed", "payment_failed", "customer_cancelled"})
COMPENSATIONS = frozenset({"release_inventory", "refund_payment", "notify_customer"})
COMPLETED, CANCELLED = "events.order_completed", "events.order_cancelled"
TERMINAL = frozenset({COMPLETED, CANCELLED})
SHELL = ("message_id", "correlation_id", "type", "schema_version", "occurred_at", "source", "payload")
PAYLOADS = {
    COMPLETED: ("order_id", "payment_id", "reservation_id", "total_cents"),
    CANCELLED: ("order_id", "reason", "compensations"),
}


class TransientError(Exception): pass
class PermanentError(Exception): pass





@dataclass(frozen=True)
class FieldError:
    path: str
    message: str


@dataclass(frozen=True)
class Contact:
    email: str | None
    phone: str | None


@dataclass(frozen=True)
class Email:
    to: str
    subject: str
    body: str
    order_id: str
    message_id: str


@dataclass(frozen=True)
class Sms:
    to: str
    body: str
    order_id: str
    message_id: str


@dataclass(frozen=True)
class Envelope:
    message_id: str
    correlation_id: str
    type: str
    schema_version: str
    occurred_at: str
    source: str
    payload: dict[str, Any]


@dataclass
class Dispatch:
    message_id: str
    order_id: str
    kind: str
    channels: dict[str, str]
    email_to: str | None
    phone_to: str | None
    notification_id: str
    correlation_id: str
    subject: str
    email_body: str
    sms_body: str


@dataclass
class OrderView:
    order_id: str
    terminal: str | None = None


@dataclass(frozen=True)
class Outcome:
    kind: str
    errors: tuple[FieldError, ...] = ()
    dispatch: Dispatch | None = None
    order_id: str | None = None


class MemoryDirectory:
    def __init__(self) -> None:
        self._rows: dict[str, Contact] = {}
        self._lock = threading.Lock()

    def put(self, order_id: str, contact: Contact) -> None:
        with self._lock:
            self._rows[order_id] = contact

    def lookup(self, order_id: str) -> Contact | None:
        with self._lock:
            return self._rows.get(order_id)


class _Recorder:
    def __init__(self) -> None:
        self.sent: list[Any] = []
        self.errors: list[Exception] = []
        self._lock = threading.Lock()

    def _push(self, msg: Any) -> None:
        with self._lock:
            if self.errors:
                raise self.errors.pop(0)
            self.sent.append(msg)


class RecordingMailer(_Recorder):
    def send(self, msg: Email) -> None:
        self._push(msg)


class RecordingSms(_Recorder):
    def send(self, msg: Sms) -> None:
        self._push(msg)


class MemoryStore:
    def __init__(self) -> None:
        self.orders: dict[str, OrderView] = {}
        self.by_message: dict[str, Dispatch] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._mu = threading.Lock()

    def lock_for(self, order_id: str) -> threading.Lock:
        with self._mu:
            return self._locks.setdefault(order_id, threading.Lock())


def _int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(UUID_RE.match(value))


def _fail(path: str, msg: str) -> tuple[None, tuple[FieldError, ...]]:
    return None, (FieldError(path, msg),)


def _payload(typ: str, payload: Any) -> FieldError | None:
    keys = PAYLOADS[typ]
    if not isinstance(payload, dict):
        return FieldError("payload", "object")
    for key in keys:
        if key not in payload:
            return FieldError(f"payload.{key}", "required")
    extra = next((k for k in payload if k not in keys), None)
    if extra:
        return FieldError(f"payload.{extra}", "additional")
    for key in ("order_id", "payment_id", "reservation_id"):
        if key in payload and not _uuid(payload[key]):
            return FieldError(f"payload.{key}", "uuid")
    cents = payload.get("total_cents")
    if "total_cents" in payload and (not _int(cents) or cents < 0):
        return FieldError("payload.total_cents", "integer")
    if "reason" in payload and payload["reason"] not in CANCEL_REASONS:
        return FieldError("payload.reason", "enum")
    comps = payload.get("compensations")
    if "compensations" in payload and (
        not isinstance(comps, list) or any(c not in COMPENSATIONS for c in comps)
    ):
        return FieldError("payload.compensations", "enum")
    return None


def parse_envelope(raw: Any) -> tuple[Envelope | None, tuple[FieldError, ...]]:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return _fail("$", "malformed json")
    if not isinstance(raw, dict):
        return _fail("$", "object")
    for key in SHELL:
        if key not in raw:
            return _fail(key, "required")
    if any(k not in SHELL and k != "causation_id" for k in raw):
        return _fail("additional", "additional")
    mid, cid, typ, ver = raw["message_id"], raw["correlation_id"], raw["type"], raw["schema_version"]
    at, src, payload = raw["occurred_at"], raw["source"], raw["payload"]
    if not _uuid(mid) or not _uuid(cid):
        return _fail("message_id" if not _uuid(mid) else "correlation_id", "uuid")
    if not isinstance(typ, str) or not typ:
        return _fail("type", "minLength 1")
    if not isinstance(ver, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", ver):
        return _fail("schema_version", "pattern")
    if not isinstance(at, str) or not DT_RE.match(at):
        return _fail("occurred_at", "date-time")
    try:
        datetime.fromisoformat(at.replace("Z", "+00:00"))
    except ValueError:
        return _fail("occurred_at", "date-time")
    if src not in SOURCES:
        return _fail("source", "enum")
    if not isinstance(payload, dict):
        return _fail("payload", "object")
    if typ not in TERMINAL:
        return Envelope(mid, cid, typ, ver, at, src, payload), ()
    if ver != SCHEMA:
        return _fail("schema_version", f"expected {SCHEMA}, got {ver}")
    if src != "orders":
        return _fail("source", "expected orders")
    bad = _payload(typ, payload)
    return (None, (bad,)) if bad else (Envelope(mid, cid, typ, ver, at, src, payload), ())


class Worker:
    def __init__(
        self,
        directory: MemoryDirectory,
        mailer: Any,
        sms: Any,
        store: MemoryStore | None = None,
        new_id: Callable[[], str] | None = None,
    ) -> None:
        self.directory, self.mailer, self.sms = directory, mailer, sms
        self.store = store or MemoryStore()
        self.new_id = new_id or (lambda: str(uuid.uuid4()))

    def handle(self, raw: Any) -> Outcome:
        env, errors = parse_envelope(raw)
        if errors or env is None:
            return Outcome("rejected", errors=errors)
        if env.type not in TERMINAL:
            return Outcome("ignored")
        oid = str(env.payload["order_id"])
        with self.store.lock_for(oid):
            return self._locked(env, oid)

    def _locked(self, env: Envelope, oid: str) -> Outcome:
        existing = self.store.by_message.get(env.message_id)
        if existing is not None:
            return self._send(existing, True)
        view = self.store.orders.get(oid)
        if view is None:
            view = OrderView(oid)
            self.store.orders[oid] = view
        kind = "completed" if env.type == COMPLETED else "cancelled"
        if view.terminal is not None:
            return Outcome("conflict", order_id=oid)
        contact = self.directory.lookup(oid)
        notify = kind != "cancelled" or "notify_customer" in list(env.payload.get("compensations") or [])
        email_to = contact.email if notify and contact and contact.email else None
        phone_to = contact.phone if notify and kind == "completed" and contact and contact.phone else None
        if kind == "completed":
            subject = f"Order {oid} completed"
            email_body = f"Paid {env.payload['total_cents']} cents. Payment {env.payload['payment_id']}."
            sms_body = f"Order {oid} paid {env.payload['total_cents']}c"
        else:
            subject = f"Order {oid} cancelled"
            email_body = f"Reason: {env.payload['reason']}."
            sms_body = ""
        dispatch = Dispatch(
            env.message_id, oid, kind,
            {"email": "pending" if email_to else "skipped", "sms": "pending" if phone_to else "skipped"},
            email_to, phone_to, self.new_id(), env.correlation_id, subject, email_body, sms_body,
        )
        self.store.by_message[env.message_id] = dispatch
        view.terminal = kind
        return self._send(dispatch, False)

    def _send(self, dispatch: Dispatch, resumed: bool) -> Outcome:
        sent_now = 0
        if dispatch.channels.get("email") == "pending" and dispatch.email_to:
            try:
                self.mailer.send(Email(
                    dispatch.email_to, dispatch.subject, dispatch.email_body,
                    dispatch.order_id, dispatch.message_id,
                ))
                dispatch.channels["email"] = "sent"
                sent_now += 1
            except TransientError:
                return Outcome("retry", dispatch=dispatch, order_id=dispatch.order_id)
            except PermanentError:
                dispatch.channels["email"] = "failed"
        if dispatch.channels.get("sms") == "pending" and dispatch.phone_to:
            try:
                self.sms.send(Sms(dispatch.phone_to, dispatch.sms_body, dispatch.order_id, dispatch.message_id))
                dispatch.channels["sms"] = "sent"
                sent_now += 1
            except TransientError:
                return Outcome("retry", dispatch=dispatch, order_id=dispatch.order_id)
            except PermanentError:
                dispatch.channels["sms"] = "failed"
        if "pending" in dispatch.channels.values():
            return Outcome("retry", dispatch=dispatch, order_id=dispatch.order_id)
        if sent_now:
            return Outcome("sent", dispatch=dispatch, order_id=dispatch.order_id)
        if resumed:
            return Outcome("replayed", dispatch=dispatch, order_id=dispatch.order_id)
        if all(v == "skipped" for v in dispatch.channels.values()):
            return Outcome("skipped", dispatch=dispatch, order_id=dispatch.order_id)
        return Outcome("failed", dispatch=dispatch, order_id=dispatch.order_id)
