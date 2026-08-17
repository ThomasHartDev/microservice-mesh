import {
  createEnvelope,
  parseEnvelope,
  type JsonObject,
  type MessageEnvelope,
  type ValidationError,
} from './contracts.js'

export type LineItem = {
  sku: string
  quantity: number
  unit_price_cents: number
}

export type Currency = 'USD' | 'EUR' | 'GBP'

export type Order = {
  order_id: string
  customer_id: string
  items: LineItem[]
  currency: Currency
  total_cents: number
  idempotency_key: string
  status: 'created'
  created_at: string
  correlation_id: string
  source_message_id: string
}

export type HandleOutcome =
  | { kind: 'created'; order: Order; event: MessageEnvelope }
  | { kind: 'replayed'; order: Order; event: MessageEnvelope }
  | { kind: 'duplicate'; order: Order; event: MessageEnvelope }
  | { kind: 'conflict'; order: Order }
  | { kind: 'rejected'; errors: ValidationError[] }
  | { kind: 'publish_failed'; order: Order; event: MessageEnvelope; error: Error }

export type Publisher = {
  publish(routingKey: string, envelope: MessageEnvelope): Promise<void>
}

export type Clock = { now(): Date; newId(): string }

export type OrderRecord = {
  order: Order
  request_hash: string
  event: MessageEnvelope
  published: boolean
}

export const ORDER_CREATED_ROUTING_KEY = 'events.orders.created'

export class MemoryPublisher implements Publisher {
  events: Array<{ routingKey: string; envelope: MessageEnvelope }> = []
  fail: Error | null = null
  async publish(routingKey: string, envelope: MessageEnvelope): Promise<void> {
    if (this.fail) throw this.fail
    this.events.push({ routingKey, envelope })
  }
}

export function defaultClock(): Clock {
  return { now: () => new Date(), newId: () => crypto.randomUUID() }
}

export function totalCents(items: LineItem[]): number | null {
  let sum = 0
  for (const it of items) {
    if (!Number.isSafeInteger(it.quantity) || !Number.isSafeInteger(it.unit_price_cents)) return null
    const line = it.quantity * it.unit_price_cents
    if (!Number.isSafeInteger(line)) return null
    const next = sum + line
    if (!Number.isSafeInteger(next)) return null
    sum = next
  }
  return sum
}

export function requestFingerprint(input: {
  customer_id: string
  items: LineItem[]
  currency: string
}): string {
  return JSON.stringify({
    customer_id: input.customer_id,
    currency: input.currency,
    items: input.items.map((it) => ({
      sku: it.sku,
      quantity: it.quantity,
      unit_price_cents: it.unit_price_cents,
    })),
  })
}

function asPlaceOrder(payload: JsonObject):
  | { ok: true; customer_id: string; items: LineItem[]; currency: Currency; idempotency_key: string }
  | { ok: false } {
  const customer_id = payload['customer_id']
  const items = payload['items']
  const currency = payload['currency']
  const idempotency_key = payload['idempotency_key']
  if (typeof customer_id !== 'string' || typeof idempotency_key !== 'string') return { ok: false }
  if (currency !== 'USD' && currency !== 'EUR' && currency !== 'GBP') return { ok: false }
  if (!Array.isArray(items)) return { ok: false }
  const parsed: LineItem[] = []
  for (const raw of items) {
    if (typeof raw !== 'object' || raw === null) return { ok: false }
    const row = raw as JsonObject
    const sku = row['sku']
    const quantity = row['quantity']
    const unit_price_cents = row['unit_price_cents']
    if (typeof sku !== 'string' || typeof quantity !== 'number' || typeof unit_price_cents !== 'number') {
      return { ok: false }
    }
    parsed.push({ sku, quantity, unit_price_cents })
  }
  return { ok: true, customer_id, items: parsed, currency, idempotency_key }
}

export class MemoryOrderStore {
  private readonly byIdem = new Map<string, OrderRecord>()
  private readonly byMessage = new Map<string, OrderRecord>()
  private readonly byId = new Map<string, OrderRecord>()
  private tail: Promise<void> = Promise.resolve()

  serialize<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.tail.then(fn, fn)
    this.tail = run.then(
      () => undefined,
      () => undefined,
    )
    return run
  }

  getByIdempotency(key: string): OrderRecord | undefined {
    return this.byIdem.get(key)
  }

  getByMessageId(id: string): OrderRecord | undefined {
    return this.byMessage.get(id)
  }

  getById(id: string): OrderRecord | undefined {
    return this.byId.get(id)
  }

  put(rec: OrderRecord): void {
    this.byIdem.set(rec.order.idempotency_key, rec)
    this.byMessage.set(rec.order.source_message_id, rec)
    this.byId.set(rec.order.order_id, rec)
  }

  markPublished(orderId: string): void {
    const rec = this.byId.get(orderId)
    if (rec) rec.published = true
  }

  unpublished(): OrderRecord[] {
    return [...this.byId.values()].filter((r) => !r.published)
  }

  all(): Order[] {
    return [...this.byId.values()].map((r) => r.order)
  }
}

export class OrdersService {
  constructor(
    readonly store: MemoryOrderStore,
    readonly publisher: Publisher,
    readonly clock: Clock,
  ) {}

  handle(raw: unknown): Promise<HandleOutcome> {
    return this.store.serialize(() => this.handleLocked(raw))
  }

  private async handleLocked(raw: unknown): Promise<HandleOutcome> {
    const parsed = parseEnvelope(raw)
    if (!parsed.ok) return { kind: 'rejected', errors: parsed.errors }
    const env = parsed.envelope
    if (env.type !== 'commands.place_order') {
      return { kind: 'rejected', errors: [{ path: 'type', message: 'expected commands.place_order' }] }
    }
    if (env.source !== 'gateway') {
      return { kind: 'rejected', errors: [{ path: 'source', message: 'expected gateway' }] }
    }

    const byMessage = this.store.getByMessageId(env.message_id)
    if (byMessage) return this.finishExisting(byMessage)

    const body = asPlaceOrder(env.payload)
    if (!body.ok) return { kind: 'rejected', errors: [{ path: 'payload', message: 'shape' }] }
    const total = totalCents(body.items)
    if (total === null) {
      return { kind: 'rejected', errors: [{ path: 'payload.items', message: 'overflow' }] }
    }

    const hash = requestFingerprint(body)
    const byKey = this.store.getByIdempotency(body.idempotency_key)
    if (byKey) {
      if (byKey.request_hash !== hash) return { kind: 'conflict', order: byKey.order }
      return this.finishExisting(byKey)
    }

    const createdAt = this.clock.now().toISOString()
    const order: Order = {
      order_id: this.clock.newId(),
      customer_id: body.customer_id,
      items: body.items.map((it) => ({ ...it })),
      currency: body.currency,
      total_cents: total,
      idempotency_key: body.idempotency_key,
      status: 'created',
      created_at: createdAt,
      correlation_id: env.correlation_id,
      source_message_id: env.message_id,
    }
    const built = createEnvelope({
      type: 'events.order_created',
      source: 'orders',
      payload: {
        order_id: order.order_id,
        customer_id: order.customer_id,
        items: order.items.map((it) => ({
          sku: it.sku,
          quantity: it.quantity,
          unit_price_cents: it.unit_price_cents,
        })),
        currency: order.currency,
        total_cents: order.total_cents,
      },
      correlation_id: env.correlation_id,
      message_id: this.clock.newId(),
      causation_id: env.message_id,
      occurred_at: createdAt,
    })
    if (!built.ok || !built.envelope) {
      return { kind: 'rejected', errors: built.ok ? [{ path: '$', message: 'envelope' }] : built.errors }
    }

    const rec: OrderRecord = { order, request_hash: hash, event: built.envelope, published: false }
    this.store.put(rec)
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return { kind: 'created', order, event: rec.event }
  }

  private async finishExisting(rec: OrderRecord): Promise<HandleOutcome> {
    if (rec.published) return { kind: 'duplicate', order: rec.order, event: rec.event }
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return { kind: 'replayed', order: rec.order, event: rec.event }
  }

  private async tryPublish(
    rec: OrderRecord,
  ): Promise<Extract<HandleOutcome, { kind: 'publish_failed' }> | null> {
    try {
      await this.publisher.publish(ORDER_CREATED_ROUTING_KEY, rec.event)
      this.store.markPublished(rec.order.order_id)
      return null
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err))
      return { kind: 'publish_failed', order: rec.order, event: rec.event, error }
    }
  }
}
