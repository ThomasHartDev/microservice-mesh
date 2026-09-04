from __future__ import annotations

import re
import threading
from typing import Callable, Optional

MAX_DELIVER = 3


class SubjectError(Exception):
    pass


class ClosedError(Exception):
    def __init__(self) -> None:
        super().__init__("broker closed")


class Delivery:
    def __init__(self, subject: str, data: bytearray, ack: Callable[[], None], nack: Callable[[], None]) -> None:
        self.subject = subject
        self.data = data
        self.ack = ack
        self.nack = nack


Handler = Callable[[Delivery], None]


def valid_subject(value: str, wildcards: bool) -> bool:
    if not value:
        return False
    tokens = value.split(".")
    for i, tok in enumerate(tokens):
        if tok == "":
            return False
        if tok == ">":
            return wildcards and i == len(tokens) - 1
        if tok == "*":
            if not wildcards:
                return False
            continue
        if re.search(r"[*>\s]", tok):
            return False
    return True


def match_subject(subject: str, pattern: str) -> bool:
    if not valid_subject(subject, False) or not valid_subject(pattern, True):
        return False
    s = subject.split(".")
    p = pattern.split(".")
    i = 0
    for tok in p:
        if tok == ">":
            return i < len(s)
        if i >= len(s) or (tok != "*" and tok != s[i]):
            return False
        i += 1
    return i == len(s)


class MemoryBroker:
    def __init__(self) -> None:
        self._closed = False
        self._subs: list[dict] = []
        self._rr: dict[str, int] = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def publish(self, subject: str, data: bytes) -> None:
        with self._lock:
            if self._closed:
                raise ClosedError()
            if not valid_subject(subject, False):
                raise SubjectError("invalid subject")
            payload = bytes(data)
            targets = self._pick(subject)
        for sub in targets:
            self._deliver(sub, subject, payload, 1)

    def subscribe(self, pattern: str, handler: Handler, queue: Optional[str] = None) -> Callable[[], None]:
        with self._lock:
            if self._closed:
                raise ClosedError()
            if not valid_subject(pattern, True):
                raise SubjectError("invalid pattern")
            if queue is not None and queue.strip() == "":
                raise SubjectError("invalid queue")
            sub = {"id": self._next_id, "pattern": pattern, "queue": queue, "handler": handler}
            self._next_id += 1
            self._subs.append(sub)
            sid = sub["id"]

        def unsubscribe() -> None:
            with self._lock:
                self._subs[:] = [s for s in self._subs if s["id"] != sid]

        return unsubscribe

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._subs.clear()

    def _pick(self, subject: str, want_pattern: Optional[str] = None, want_queue: Optional[str] = None) -> list[dict]:
        groups: dict[str, list[dict]] = {}
        fanout: list[dict] = []
        for sub in self._subs:
            if want_pattern is not None and (sub["pattern"] != want_pattern or sub["queue"] != want_queue):
                continue
            if not match_subject(subject, sub["pattern"]):
                continue
            if sub["queue"] is None:
                fanout.append(sub)
                continue
            key = f"{sub['pattern']}\0{sub['queue']}"
            groups.setdefault(key, []).append(sub)
        targets = list(fanout)
        for key, members in groups.items():
            i = self._rr.get(key, 0) % len(members)
            self._rr[key] = i + 1
            targets.append(members[i])
        return targets

    def _deliver(self, sub: dict, subject: str, data: bytes, attempt: int) -> None:
        nacked = False

        def ack() -> None:
            return

        def nack() -> None:
            nonlocal nacked
            nacked = True

        try:
            sub["handler"](Delivery(subject, bytearray(data), ack, nack))
        except Exception:
            nacked = True
        if nacked and attempt < MAX_DELIVER:
            if sub["queue"] is None:
                nxt = sub
            else:
                with self._lock:
                    picked = self._pick(subject, sub["pattern"], sub["queue"])
                nxt = picked[0] if picked else sub
            self._deliver(nxt, subject, data, attempt + 1)
