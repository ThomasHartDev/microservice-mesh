# microservice-mesh

Polyglot order-processing system: independent services talk over a message broker, then ship with docker-compose and Kubernetes.

## What this demonstrates

A realistic microservices mesh for placing an order, reserving inventory, charging payment, and notifying the customer. Services stay independently deployable and share explicit contracts so Go, TypeScript, and Python speak the same wire format.

## Concepts demonstrated

- Microservices topology (API gateway + domain services + worker)
- API gateway as a validating command producer (REST to broker)
- Command/event split (`commands.place_order` in, `events.order_created` out)
- Transactional outbox (persist the order and event, then publish; retry republishes the same envelope)
- Consumer idempotency on `message_id` and `idempotency_key` under at-least-once delivery
- Idempotency-key replay versus conflict (same body vs different body)
- Serialized handler critical section (single-writer per in-memory store)
- Integer money in cents with `Number.isSafeInteger` overflow rejection
- Correlation and causation ids across the command-to-event hop
- Fail-fast request validation and structured field errors
- Message envelope (message id, correlation id, schema version, occurred_at)
- At-least-once client retry when the broker is unavailable (HTTP 503)
- Message-broker integration (async commands and domain events)
- Contract-first design (shared schemas across languages)
- Polyglot service boundaries
- Containerization and local multi-service orchestration (coming)
- Kubernetes deployment with Helm and HPA (coming)

- NATS subject matching (`*` one token, `>` one or more remaining tokens)
- Queue groups (competing consumers) versus fan-out subscriptions
- Ack/nack with bounded redelivery (poison messages drop after max attempts)
- Shared broker client libraries (Go and TypeScript) over one routing catalog
- Shared broker client libraries (Go, TypeScript, and Python) over one routing catalog

- Idempotent consumer (message-id dedupe under at-least-once redelivery)
- Per-channel send state so a retry does not double-email
- Transient versus permanent notification-provider failure
- Poison-message rejection versus ignore of non-terminal types
- Poison-message rejection versus ignore of types the worker does not consume
- Out-of-order join: park a terminal event until order_created maps the customer

- Multi-stage container builds (build stage vs runtime stage)
- Least-privilege containers (numeric non-root USER, distroless static Go binary)
- Build-context filtering (.dockerignore last-match-wins glob, including negation)
- Layer cache hygiene (copy manifests before source)
- Two-phase authorize/capture (reserve a ledger hold, then charge)
- Stripe-style idempotency keys on reserve and charge (fingerprint replay vs conflict)
- Retryable processor errors do not occupy the key; declines and insufficient funds do
- Ledger available vs held cents, one payment intent per order
- Transactional outbox for `events.payment_charged` and `events.payment_failed`

- Compensating transactions (saga participant: reserve, then release)
- All-or-nothing multi-SKU holds with in-section rollback on oversell
- Pessimistic stock reservation (`on_hand` vs `reserved`, never `reserved > on_hand`)
- Cancel tombstone so a late `order_created` cannot hold stock after compensate
- Serialized critical section for last-unit races
- Transactional outbox for `events.inventory_reserved` and `events.inventory_reservation_failed`
- Retry until publish succeeds for `events.inventory_reserved` and `events.inventory_reservation_failed`

## What's implemented

- Project scaffold with TypeScript strict mode, Vitest, and CI
- API gateway in Go: REST `POST /v1/orders` validates the place-order contract and publishes `commands.place_order` to the broker
- Orders service (TypeScript): consumes `commands.place_order`, persists, emits `events.order_created`

- NATS subject matching (`*` one token, `>` one or more remaining tokens)
- Queue groups (competing consumers) versus fan-out subscriptions
- Ack/nack with bounded redelivery (poison messages drop after max attempts)
- Shared broker client libraries (Go and TypeScript) over one routing catalog
- Shared JSON Schema contracts and message envelope
- Broker wiring (NATS or RabbitMQ) with a shared client lib per language: in-memory NATS-style bus in TypeScript (`src/broker.ts`) and Go (`services/gateway/broker`), plus a gateway `EnvelopePublisher` that JSON-encodes envelopes onto catalog routing keys
- Shared broker client libraries (Go, TypeScript, and Python) over one routing catalog
- In-memory NATS-style bus in TypeScript (`src/broker.ts`), Go (`services/gateway/broker`), and Python (`python/broker.py`). Token subjects, `*` / `>` matching, queue groups, and max-3 nack redelivery. There is no NATS or RabbitMQ client, connection, or compose service yet. The gateway `EnvelopePublisher` JSON-encodes envelopes onto catalog routing keys and any `Publish(subject, []byte)` backend.
- Idempotent consumer (message-id dedupe under at-least-once redelivery)
- Per-channel send state so a retry does not double-email
- Transient versus permanent notification-provider failure
- Poison-message rejection versus ignore of non-terminal types
- Notifications worker in Python: consumes terminal events, sends (mock) email/SMS
- Poison-message rejection versus ignore of types the worker does not consume
- Out-of-order join: park a terminal event until order_created maps the customer
- Notifications worker in Python: consumes `events.order_created` plus terminal events, parks out-of-order terminals, sends (mock) email/SMS keyed by `customer_id`
- Multi-stage container builds (build stage vs runtime stage)
- Least-privilege containers (numeric non-root USER, distroless static Go binary)
- Build-context filtering (.dockerignore last-match-wins glob, including negation)
- Layer cache hygiene (copy manifests before source)
- Dockerfile per service (multi-stage, non-root) + .dockerignore: Go gateway (distroless), TypeScript orders/payments/inventory, Python notifications worker. Image policy is enforced in tests.
- Two-phase authorize/capture (reserve a ledger hold, then charge)
- Stripe-style idempotency keys on reserve and charge (fingerprint replay vs conflict)
- Retryable processor errors do not occupy the key; declines and insufficient funds do
- Ledger available vs held cents, one payment intent per order
- Transactional outbox for `events.payment_charged` and `events.payment_failed`
- Payments service: reserve/charge with idempotency keys, emits payment events
- Compensating transactions (saga participant: reserve, then release)
- All-or-nothing multi-SKU holds with in-section rollback on oversell
- Pessimistic stock reservation (`on_hand` vs `reserved`, never `reserved > on_hand`)
- Cancel tombstone so a late `order_created` cannot hold stock after compensate
- Serialized critical section for last-unit races
- Transactional outbox for `events.inventory_reserved` and `events.inventory_reservation_failed`
- Inventory service: reserve stock, handle oversell with compensation
- Retry until publish succeeds for `events.inventory_reserved` and `events.inventory_reservation_failed`
## Usage

```bash
pnpm install
pnpm run typecheck
pnpm test
```

`pnpm test` runs Vitest and `go test -race` for `services/gateway`. A valid `POST /v1/orders` returns `202` with `message_id` and `correlation_id`. Field errors return `400`. The same `idempotency_key` plus the same bytes replays the accept. A different body on that key returns `409`. Broker failure returns `503`.

The TypeScript orders service (`src/orders.ts`) is the catalog consumer of `commands.place_order`. It writes the order first, parks `events.order_created` in an in-memory outbox, then publishes to `events.orders.created`. A redelivered command with the same `message_id` or the same `idempotency_key` and body does not insert a second order. A broker failure leaves the row unpublished so the next handle republishes the original event envelope, including the same `message_id` and `causation_id`.

```bash
cd services/gateway && go run ./cmd/gateway
# POST localhost:8080/v1/orders
# {"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"SKU-1","quantity":2,"unit_price_cents":1500}],"currency":"USD","idempotency_key":"checkout-1"}
```

Stdout is the published envelope until a real broker adapter lands.

## License

MIT
