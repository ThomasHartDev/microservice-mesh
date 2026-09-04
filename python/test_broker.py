import unittest

from broker import (
    MAX_DELIVER,
    ClosedError,
    MemoryBroker,
    SubjectError,
    match_subject,
    valid_subject,
)


class BrokerTest(unittest.TestCase):
    def test_subject_tokens(self) -> None:
        self.assertTrue(valid_subject("commands.place_order", False))
        self.assertFalse(valid_subject(">", False))
        self.assertFalse(valid_subject("http.POST /v1/orders", False))
        self.assertTrue(match_subject("orders.created", "orders.*"))
        self.assertTrue(match_subject("foo.bar.baz", "foo.>"))
        self.assertFalse(match_subject("foo", "foo.>"))
        self.assertFalse(match_subject("a.b.c", "a.*"))

    def test_fanout_and_queue_group(self) -> None:
        bus = MemoryBroker()
        fan: list[str] = []
        q = [0, 0]
        bus.subscribe("jobs", lambda d: (fan.append(d.data.decode()), d.ack()))
        bus.subscribe("jobs", lambda d: q.__setitem__(0, q[0] + 1) or d.ack(), queue="workers")
        bus.subscribe("jobs", lambda d: q.__setitem__(1, q[1] + 1) or d.ack(), queue="workers")
        for n in ["1", "2", "3", "4"]:
            bus.publish("jobs", n.encode())
        self.assertEqual(fan, ["1", "2", "3", "4"])
        self.assertEqual(q, [2, 2])

    def test_nack_and_throw_redelivery(self) -> None:
        bus = MemoryBroker()
        n = {"v": 0}

        def handler(d) -> None:
            n["v"] += 1
            if n["v"] < 3:
                d.nack()
            else:
                d.ack()

        bus.subscribe("a.b", handler)
        bus.publish("a.b", b"x")
        self.assertEqual(n["v"], 3)
        poison = {"v": 0}

        def boom(_d) -> None:
            poison["v"] += 1
            raise RuntimeError("boom")

        bus.subscribe("poison", boom)
        bus.publish("poison", b"p")
        self.assertEqual(poison["v"], MAX_DELIVER)

    def test_subscribe_events_wildcard(self) -> None:
        bus = MemoryBroker()
        got: list[str] = []
        bus.subscribe("events.>", lambda d: (got.append(d.subject), d.ack()))
        bus.publish("events.orders.created", b"ok")
        bus.publish("commands.place_order", b"skip")
        self.assertEqual(got, ["events.orders.created"])

    def test_unsubscribe(self) -> None:
        bus = MemoryBroker()
        hits = {"v": 0}
        unsub = bus.subscribe("events.>", lambda d: (hits.__setitem__("v", hits["v"] + 1), d.ack()))
        bus.publish("events.orders.created", b"ok")
        unsub()
        bus.publish("events.orders.created", b"late")
        self.assertEqual(hits["v"], 1)

    def test_invalid_queue(self) -> None:
        bus = MemoryBroker()
        with self.assertRaises(SubjectError):
            bus.subscribe("jobs", lambda d: None, queue="")
        with self.assertRaises(SubjectError):
            bus.subscribe("jobs", lambda d: None, queue="   ")

    def test_queue_group_nack_goes_to_next_member(self) -> None:
        bus = MemoryBroker()
        who: list[int] = []
        bus.subscribe("jobs", lambda d: (who.append(1), d.nack()), queue="workers")
        bus.subscribe("jobs", lambda d: (who.append(2), d.ack()), queue="workers")
        bus.publish("jobs", b"x")
        self.assertEqual(who, [1, 2])

    def test_delivery_copies_payload(self) -> None:
        bus = MemoryBroker()
        seen: list[int] = []
        bus.subscribe("m", lambda d: (d.data.__setitem__(0, 9), d.ack()))
        bus.subscribe("m", lambda d: (seen.append(d.data[0]), d.ack()))
        bus.publish("m", bytes([1]))
        self.assertEqual(seen, [1])

    def test_closed_and_invalid_publish(self) -> None:
        bus = MemoryBroker()
        with self.assertRaises(SubjectError):
            bus.publish(">", b"x")
        bus.close()
        with self.assertRaises(ClosedError):
            bus.publish("x", b"x")


if __name__ == "__main__":
    unittest.main()
