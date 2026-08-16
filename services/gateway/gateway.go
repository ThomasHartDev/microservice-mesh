package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	MessageTypePlaceOrder = "commands.place_order"
	SourceGateway         = "gateway"
	SchemaVersion         = "1.0.0"
	RoutingKeyPlaceOrder  = "commands.place_order"
	MaxBodyBytes          = 64 << 10
)

var uuidRE = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

var currencies = map[string]struct{}{"USD": {}, "EUR": {}, "GBP": {}}

type FieldError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

type LineItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int    `json:"unit_price_cents"`
}

type PlaceOrder struct {
	CustomerID     string     `json:"customer_id"`
	Items          []LineItem `json:"items"`
	Currency       string     `json:"currency"`
	IdempotencyKey string     `json:"idempotency_key"`
}

type Envelope struct {
	MessageID     string     `json:"message_id"`
	CorrelationID string     `json:"correlation_id"`
	Type          string     `json:"type"`
	SchemaVersion string     `json:"schema_version"`
	OccurredAt    string     `json:"occurred_at"`
	Source        string     `json:"source"`
	Payload       PlaceOrder `json:"payload"`
}

type Accepted struct {
	MessageID     string `json:"message_id"`
	CorrelationID string `json:"correlation_id"`
	Type          string `json:"type"`
}

type Publisher interface {
	Publish(ctx context.Context, routingKey string, env Envelope) error
}

type Recorded struct {
	RoutingKey string
	Envelope   Envelope
}

type MemoryBroker struct {
	mu     sync.Mutex
	Events []Recorded
	Fail   error
}

func (b *MemoryBroker) Publish(ctx context.Context, routingKey string, env Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Fail != nil {
		return b.Fail
	}
	b.Events = append(b.Events, Recorded{RoutingKey: routingKey, Envelope: env})
	return nil
}

func (b *MemoryBroker) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.Events)
}

type LogPublisher struct{ W io.Writer }

func (p LogPublisher) Publish(_ context.Context, routingKey string, env Envelope) error {
	line, err := json.Marshal(struct {
		RoutingKey string   `json:"routing_key"`
		Envelope   Envelope `json:"envelope"`
	}{routingKey, env})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(p.W, "%s\n", line)
	return err
}

func ValidatePlaceOrder(o PlaceOrder) []FieldError {
	var errs []FieldError
	add := func(path, msg string) { errs = append(errs, FieldError{Path: path, Message: msg}) }
	if !uuidRE.MatchString(o.CustomerID) {
		add("customer_id", "uuid")
	}
	if len(o.Items) < 1 {
		add("items", "minItems 1")
	}
	for i, it := range o.Items {
		p := fmt.Sprintf("items[%d]", i)
		if it.SKU == "" {
			add(p+".sku", "minLength 1")
		}
		if it.Quantity <= 0 {
			add(p+".quantity", "> 0")
		}
		if it.UnitPriceCents < 0 {
			add(p+".unit_price_cents", ">= 0")
		}
	}
	if _, ok := currencies[o.Currency]; !ok {
		add("currency", "enum [USD,EUR,GBP]")
	}
	if len(o.IdempotencyKey) < 8 {
		add("idempotency_key", "minLength 8")
	}
	return errs
}

type errorBody struct {
	Errors []FieldError `json:"errors"`
}

type inflight struct {
	ready  chan struct{}
	hash   [32]byte
	status int
	resp   []byte
}

type Gateway struct {
	pub   Publisher
	now   func() time.Time
	newID func() string
	mu    sync.Mutex
	seen  map[string]*inflight
}

func New(pub Publisher) *Gateway {
	return &Gateway{
		pub: pub, now: func() time.Time { return time.Now().UTC() },
		newID: newUUIDv4, seen: make(map[string]*inflight),
	}
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", g.placeOrder)
	return mux
}

func (g *Gateway) placeOrder(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r.Header.Get("Content-Type")) {
		writeErr(w, http.StatusUnsupportedMediaType, FieldError{Path: "content-type", Message: "application/json"})
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil || len(raw) == 0 {
		writeErr(w, http.StatusBadRequest, FieldError{Path: "$", Message: "empty body"})
		return
	}
	if len(raw) > MaxBodyBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, FieldError{Path: "$", Message: "max 65536 bytes"})
		return
	}
	var req PlaceOrder
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || dec.More() {
		writeErr(w, http.StatusBadRequest, FieldError{Path: "$", Message: jsonMessage(err)})
		return
	}
	if errs := ValidatePlaceOrder(req); len(errs) > 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Errors: errs})
		return
	}

	hash := sha256.Sum256(raw)
	slot, replay, conflict := g.beginIdempotent(req.IdempotencyKey, hash)
	if conflict {
		writeErr(w, http.StatusConflict, FieldError{Path: "idempotency_key", Message: "reuse with a different body"})
		return
	}
	if replay {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(slot.status)
		_, _ = w.Write(slot.resp)
		return
	}

	corr := r.Header.Get("X-Correlation-ID")
	if !uuidRE.MatchString(corr) {
		corr = g.newID()
	}
	env := Envelope{
		MessageID: g.newID(), CorrelationID: corr, Type: MessageTypePlaceOrder,
		SchemaVersion: SchemaVersion, OccurredAt: g.now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Source: SourceGateway, Payload: req,
	}
	if err := g.pub.Publish(r.Context(), RoutingKeyPlaceOrder, env); err != nil {
		g.failIdempotent(req.IdempotencyKey, slot)
		writeErr(w, http.StatusServiceUnavailable, FieldError{Path: "broker", Message: "publish failed"})
		return
	}
	payload, _ := json.Marshal(Accepted{MessageID: env.MessageID, CorrelationID: env.CorrelationID, Type: env.Type})
	g.finishIdempotent(slot, http.StatusAccepted, payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(payload)
}

func (g *Gateway) beginIdempotent(key string, hash [32]byte) (slot *inflight, replay, conflict bool) {
	for {
		g.mu.Lock()
		if existing, ok := g.seen[key]; ok {
			g.mu.Unlock()
			<-existing.ready
			if existing.status == 0 {
				continue
			}
			if existing.hash != hash {
				return existing, false, true
			}
			return existing, true, false
		}
		slot = &inflight{ready: make(chan struct{}), hash: hash}
		g.seen[key] = slot
		g.mu.Unlock()
		return slot, false, false
	}
}

func (g *Gateway) finishIdempotent(slot *inflight, status int, resp []byte) {
	slot.status = status
	slot.resp = resp
	close(slot.ready)
}

func (g *Gateway) failIdempotent(key string, slot *inflight) {
	g.mu.Lock()
	if g.seen[key] == slot {
		delete(g.seen, key)
	}
	g.mu.Unlock()
	close(slot.ready)
}

func isJSON(ct string) bool {
	if ct == "" {
		return true
	}
	media, _, _ := strings.Cut(ct, ";")
	return strings.TrimSpace(media) == "application/json"
}

func jsonMessage(err error) string {
	if err == nil {
		return "trailing data"
	}
	if strings.Contains(err.Error(), "unknown field") {
		return "additional property"
	}
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	if errors.As(err, &syn) {
		return "malformed json"
	}
	if errors.As(err, &typ) {
		return "type"
	}
	return "invalid json"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err FieldError) {
	writeJSON(w, status, errorBody{Errors: []FieldError{err}})
}

func newUUIDv4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
