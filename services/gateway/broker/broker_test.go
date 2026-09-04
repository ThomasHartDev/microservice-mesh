package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestMatchSubject(t *testing.T) {
	if !MatchSubject("orders.created", "orders.*") || MatchSubject("foo", "foo.>") || ValidSubject(">", false) {
		t.Fatal("match")
	}
	if MatchSubject("a.b.c", "a.*") || !MatchSubject("foo.bar.baz", "foo.>") {
		t.Fatal("tokens")
	}
}

func TestFanoutAndQueueGroup(t *testing.T) {
	bus := New()
	var fan, a, b atomic.Int32
	_, _ = bus.Subscribe("jobs", func(d Delivery) error { fan.Add(1); d.Ack(); return nil }, "")
	_, _ = bus.Subscribe("jobs", func(d Delivery) error { a.Add(1); d.Ack(); return nil }, "workers")
	_, _ = bus.Subscribe("jobs", func(d Delivery) error { b.Add(1); d.Ack(); return nil }, "workers")
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := bus.Publish(ctx, "jobs", []byte("n")); err != nil {
			t.Fatal(err)
		}
	}
	if fan.Load() != 4 || a.Load() != 2 || b.Load() != 2 {
		t.Fatalf("fan=%d a=%d b=%d", fan.Load(), a.Load(), b.Load())
	}
}

func TestNackRedelivery(t *testing.T) {
	bus := New()
	var n, poison atomic.Int32
	_, _ = bus.Subscribe("a.b", func(d Delivery) error {
		if n.Add(1) < 3 {
			d.Nack()
			return nil
		}
		d.Ack()
		return nil
	}, "")
	ctx := context.Background()
	if err := bus.Publish(ctx, "a.b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, _ = bus.Subscribe("poison", func(Delivery) error { poison.Add(1); return errors.New("boom") }, "")
	if err := bus.Publish(ctx, "poison", []byte("p")); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 3 || poison.Load() != MaxDeliver {
		t.Fatalf("n=%d poison=%d", n.Load(), poison.Load())
	}
}

func TestSubscribeEventsWildcard(t *testing.T) {
	bus := New()
	var hits atomic.Int32
	_, err := bus.Subscribe("events.>", func(d Delivery) error { hits.Add(1); d.Ack(); return nil }, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := bus.Publish(ctx, "events.orders.created", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, "commands.place_order", []byte("skip")); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits=%d", hits.Load())
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	var hits atomic.Int32
	unsub, err := bus.Subscribe("events.>", func(d Delivery) error { hits.Add(1); d.Ack(); return nil }, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := bus.Publish(ctx, "events.orders.created", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	unsub()
	if err := bus.Publish(ctx, "events.orders.created", []byte("late")); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits=%d", hits.Load())
	}
}

func TestInvalidQueue(t *testing.T) {
	bus := New()
	h := func(Delivery) error { return nil }
	if _, err := bus.Subscribe("jobs", h, "   "); !errors.Is(err, ErrQueue) {
		t.Fatalf("whitespace queue: %v", err)
	}
	if _, err := bus.Subscribe("jobs", h, "\t"); !errors.Is(err, ErrQueue) {
		t.Fatalf("tab queue: %v", err)
	}
}

func TestQueueGroupNackGoesToNextMember(t *testing.T) {
	bus := New()
	var who []int
	_, _ = bus.Subscribe("jobs", func(d Delivery) error { who = append(who, 1); d.Nack(); return nil }, "workers")
	_, _ = bus.Subscribe("jobs", func(d Delivery) error { who = append(who, 2); d.Ack(); return nil }, "workers")
	if err := bus.Publish(context.Background(), "jobs", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if len(who) != 2 || who[0] != 1 || who[1] != 2 {
		t.Fatalf("who=%v", who)
	}
}

func TestDeliveryCopiesPayload(t *testing.T) {
	bus := New()
	var second byte
	_, _ = bus.Subscribe("m", func(d Delivery) error { d.Data[0] = 9; d.Ack(); return nil }, "")
	_, _ = bus.Subscribe("m", func(d Delivery) error { second = d.Data[0]; d.Ack(); return nil }, "")
	if err := bus.Publish(context.Background(), "m", []byte{1}); err != nil {
		t.Fatal(err)
	}
	if second != 1 {
		t.Fatalf("shared buffer: second saw %d", second)
	}
}

func TestPublishRejectsWildcardAndClosed(t *testing.T) {
	bus := New()
	ctx := context.Background()
	if err := bus.Publish(ctx, ">", []byte("x")); !errors.Is(err, ErrSubject) {
		t.Fatal(err)
	}
	bus.Close()
	if err := bus.Publish(ctx, "a.b", []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New().Publish(cctx, "a.b", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
