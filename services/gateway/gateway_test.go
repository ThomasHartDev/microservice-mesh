package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const validJSON = `{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"SKU-1","quantity":2,"unit_price_cents":1500}],"currency":"USD","idempotency_key":"checkout-1"}`

func TestPlaceOrderHTTP(t *testing.T) {
	broker := &MemoryBroker{}
	g := New(broker)
	g.now = func() time.Time { return time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC) }

	rec := post(g, validJSON, "application/json", "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	var acc Accepted
	if err := json.Unmarshal(rec.Body.Bytes(), &acc); err != nil || acc.Type != MessageTypePlaceOrder {
		t.Fatalf("body %s", rec.Body)
	}
	if acc.CorrelationID != "6ba7b810-9dad-11d1-80b4-00c04fd430c8" || broker.Len() != 1 {
		t.Fatalf("corr=%s n=%d", acc.CorrelationID, broker.Len())
	}
	env := broker.Events[0].Envelope
	if env.OccurredAt != "2026-08-13T12:00:00.000Z" || env.Payload.Items[0].Quantity != 2 {
		t.Fatalf("env %+v", env)
	}

	rejects := []struct {
		body, ct string
		code     int
	}{
		{"", "application/json", 400},
		{"{", "application/json", 400},
		{`{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"A","quantity":1,"unit_price_cents":1}],"currency":"USD","idempotency_key":"checkout-1","extra":1}`, "application/json", 400},
		{`{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"A","quantity":"x","unit_price_cents":1}],"currency":"USD","idempotency_key":"checkout-1"}`, "application/json", 400},
		{`{"customer_id":"nope","items":[],"currency":"USD","idempotency_key":"checkout-1"}`, "application/json", 400},
		{`{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"","quantity":0,"unit_price_cents":-1}],"currency":"JPY","idempotency_key":"x"}`, "application/json", 400},
		{validJSON, "text/xml", 415},
		{validJSON + "\n{}", "application/json", 400},
	}
	for i, tc := range rejects {
		if got := post(g, tc.body, tc.ct, ""); got.Code != tc.code {
			t.Fatalf("reject %d got %d %s", i, got.Code, got.Body)
		}
	}
	big := `{"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"` + strings.Repeat("A", MaxBodyBytes) + `","quantity":1,"unit_price_cents":1}],"currency":"USD","idempotency_key":"checkout-1"}`
	if post(g, big, "application/json", "").Code != 413 {
		t.Fatal("413")
	}

	down := &MemoryBroker{Fail: errors.New("down")}
	g2 := New(down)
	if post(g2, validJSON, "application/json", "").Code != 503 {
		t.Fatal("503")
	}
	down.Fail = nil
	if post(g2, validJSON, "application/json", "").Code != 202 || down.Len() != 1 {
		t.Fatal("retry")
	}
	if post(g2, validJSON, "application/json", "").Code != 202 || down.Len() != 1 {
		t.Fatal("replay")
	}
	if post(g2, strings.Replace(validJSON, "SKU-1", "SKU-2", 1), "application/json", "").Code != 409 {
		t.Fatal("409")
	}
}

func TestConcurrentPublish(t *testing.T) {
	broker := &MemoryBroker{}
	g := New(broker)
	var wg sync.WaitGroup
	bodies := make([]string, 12)
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bodies[i] = post(g, validJSON, "application/json", "").Body.String()
		}(i)
	}
	wg.Wait()
	for i := range bodies {
		if bodies[i] != bodies[0] {
			t.Fatalf("worker %d", i)
		}
	}
	if broker.Len() != 1 {
		t.Fatalf("published %d", broker.Len())
	}
	g2 := New(&MemoryBroker{})
	var n atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		body := strings.Replace(validJSON, "checkout-1", "checkout-"+strconv.Itoa(i), 1)
		go func(body string) {
			defer wg.Done()
			if post(g2, body, "application/json", "").Code == 202 {
				n.Add(1)
			}
		}(body)
	}
	wg.Wait()
	if n.Load() != 8 {
		t.Fatalf("distinct %d", n.Load())
	}
	var buf bytes.Buffer
	if post(New(LogPublisher{W: &buf}), validJSON, "application/json", "").Code != 202 {
		t.Fatal("log")
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"routing_key":"commands.place_order"`)) {
		t.Fatalf("%s", buf.String())
	}
}

func post(g *Gateway, body, ct, corr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader(body))
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if corr != "" {
		req.Header.Set("X-Correlation-ID", corr)
	}
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	return rec
}
