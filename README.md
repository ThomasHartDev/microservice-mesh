# microservice-mesh

Polyglot order-processing mesh: gateway and domain services coordinate over a message broker with shared contracts, then ship via docker-compose and Kubernetes.

## What this demonstrates

A realistic order flow across independently deployable services. The gateway accepts place-order, orders owns the saga, inventory and payments run as separate workers, and a Python notifications worker reacts to domain events. Cross-service messages share JSON Schema contracts and a common envelope so Go, TypeScript, and Python stay interoperable without sharing runtime code.

## Concepts demonstrated

- Contract-first design (JSON Schema payloads across languages)
- Command vs domain event separation
- Message envelope: correlation ID, causation ID, schema version
- Catalog-driven routing keys (topic bindings per type)
- Idempotency via `message_id` / `idempotency_key`
- Saga failure contracts (`order_cancelled` + compensations)
- Microservices topology (gateway, orders, inventory, payments, notifications)
- Polyglot boundaries over one wire format

## Topology

```
  HTTP POST /v1/orders → [gateway: Go] → [orders: TS]  (saga owner)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       [inventory: TS]  [payments: TS]  [notifications: Py]
```

Happy path: `place_order` → `order_created` → `inventory_reserved` → `payment_charged` → `order_completed`.

On failure, orders emits `order_cancelled` with compensations (`release_inventory`, `refund_payment`, `notify_customer`).

## What's implemented

- Project scaffold with TypeScript strict mode, Vitest, and CI
- Shared event/API contracts (JSON Schema) plus message catalog and envelope validation

## Usage

```bash
pnpm install && pnpm run typecheck && pnpm test
```

```ts
import { createEnvelope, parseEnvelope } from './src/index.js'

const created = createEnvelope({
  type: 'commands.place_order',
  source: 'gateway',
  correlation_id: '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
  message_id: '550e8400-e29b-41d4-a716-446655440000',
  payload: {
    customer_id: '550e8400-e29b-41d4-a716-446655440000',
    items: [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }],
    currency: 'USD',
    idempotency_key: 'client-key-001',
  },
})
if (created.ok && created.envelope) parseEnvelope(created.envelope)
```

Contracts live in `src/contracts.ts` (payload schemas, catalog, envelope create/parse).

## License

MIT
