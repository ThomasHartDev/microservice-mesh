# microservice-mesh

Polyglot order-processing system: independent services talk over a message broker, then ship with docker-compose and Kubernetes.

## What this demonstrates

A realistic microservices mesh for placing an order, reserving inventory, charging payment, and notifying the customer. Services stay independently deployable and share explicit contracts so Go, TypeScript, and Python speak the same wire format.

## Concepts demonstrated

- Microservices topology (API gateway + domain services + worker)
- API gateway as a validating command producer (REST to broker)
- Fail-fast request validation and structured field errors
- Message envelope (message id, correlation id, schema version, occurred_at)
- Idempotency-key replay versus conflict (same body vs different body)
- At-least-once client retry when the broker is unavailable (HTTP 503)
- Message-broker integration (async commands and domain events)
- Contract-first design (shared schemas across languages)
- Polyglot service boundaries
- Containerization and local multi-service orchestration (coming)
- Kubernetes deployment with Helm and HPA (coming)

## What's implemented

- Project scaffold with TypeScript strict mode, Vitest, and CI
- API gateway in Go: REST `POST /v1/orders` validates the place-order contract and publishes `commands.place_order` to the broker

## Usage

```bash
pnpm install
pnpm run typecheck
pnpm test
```

`pnpm test` runs Vitest and `go test -race` for `services/gateway`. A valid `POST /v1/orders` returns `202` with `message_id` and `correlation_id`. Field errors return `400`. The same `idempotency_key` plus the same bytes replays the accept. A different body on that key returns `409`. Broker failure returns `503`.

```bash
cd services/gateway && go run ./cmd/gateway
# POST localhost:8080/v1/orders
# {"customer_id":"550e8400-e29b-41d4-a716-446655440000","items":[{"sku":"SKU-1","quantity":2,"unit_price_cents":1500}],"currency":"USD","idempotency_key":"checkout-1"}
```

Stdout is the published envelope until a real broker adapter lands.

## License

MIT
