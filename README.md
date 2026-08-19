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
