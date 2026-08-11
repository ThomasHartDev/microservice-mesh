# microservice-mesh

Polyglot order-processing system: independent services talk over a message broker, then ship with docker-compose and Kubernetes.

## What this demonstrates

A realistic microservices mesh for placing an order, reserving inventory, charging payment, and notifying the customer. Services stay independently deployable and share explicit contracts so Go, TypeScript, and Python speak the same wire format.

## Concepts demonstrated

- Microservices topology (API gateway + domain services + worker)
- Message-broker integration (async commands and domain events)
- Contract-first design (shared schemas across languages)
- Polyglot service boundaries
- Containerization and local multi-service orchestration (coming)
- Kubernetes deployment with Helm and HPA (coming)

## What's implemented

- Project scaffold with TypeScript strict mode, Vitest, and CI

## Usage

```bash
pnpm install
pnpm run typecheck
pnpm test
```

## License

MIT
