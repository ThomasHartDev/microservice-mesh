package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestMatchAndBus(t *testing.T) {
	if !MatchSubject("orders.created", "orders.*") || MatchSubject("foo", "foo.>") || ValidSubject(">", false) {
		t.Fatal("match")
	}
	bus := New()
	var fan, a, b, n, poison atomic.Int32
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
	_, _ = bus.Subscribe("a.b", func(d Delivery) error {
		if n.Add(1) < 3 {
			d.Nack()
			return nil
		}
		d.Ack()
		return nil
	}, "")
	_ = bus.Publish(ctx, "a.b", []byte("x"))
	_, _ = bus.Subscribe("poison", func(Delivery) error { poison.Add(1); return errors.New("boom") }, "")
	_ = bus.Publish(ctx, "poison", []byte("p"))
	if n.Load() != 3 || poison.Load() != MaxDeliver {
		t.Fatalf("n=%d poison=%d", n.Load(), poison.Load())
	}
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
