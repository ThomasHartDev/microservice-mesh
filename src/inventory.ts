import {
  createEnvelope,
  parseEnvelope,
  type JsonObject,
  type MessageEnvelope,
  type ValidationError,
} from './contracts.js'

export type FailReason = 'out_of_stock' | 'sku_unknown' | 'warehouse_unavailable'
export type ReservationStatus = 'reserved' | 'released' | 'failed'
export type StockLot = { sku: string; warehouse_id: string; on_hand: number; reserved: number }
export type ReservedLine = { sku: string; quantity: number; warehouse_id: string }
export type OrderLine = { sku: string; quantity: number; unit_price_cents: number }
export type Reservation = {
  reservation_id: string
  order_id: string
  items: ReservedLine[]
  status: ReservationStatus
  reason?: FailReason
  failed_skus?: string[]
  created_at: string
  correlation_id: string
  source_message_id: string
}
export type Outcome =
  | { kind: 'reserved'; reservation: Reservation; event: MessageEnvelope }
  | { kind: 'failed'; reservation: Reservation; event: MessageEnvelope }
  | { kind: 'released'; reservation: Reservation }
  | { kind: 'replayed'; reservation: Reservation; event: MessageEnvelope }
  | { kind: 'duplicate'; reservation: Reservation; event?: MessageEnvelope }
  | { kind: 'conflict'; reservation: Reservation }
  | { kind: 'rejected'; errors: ValidationError[] }
  | { kind: 'publish_failed'; reservation: Reservation; event: MessageEnvelope; error: Error }
export type Publisher = { publish(routingKey: string, envelope: MessageEnvelope): Promise<void> }
export type Clock = { now(): Date; newId(): string }
type RecordRow = {
  reservation: Reservation
  request_hash: string
  event: MessageEnvelope | null
  published: boolean
}

export const INVENTORY_RESERVED_ROUTING_KEY = 'events.inventory.reserved'
export const INVENTORY_FAILED_ROUTING_KEY = 'events.inventory.reservation_failed'

export class MemoryPublisher implements Publisher {
  events: Array<{ routingKey: string; envelope: MessageEnvelope }> = []
  fail: Error | null = null
  async publish(routingKey: string, envelope: MessageEnvelope): Promise<void> {
    if (this.fail) throw this.fail
    this.events.push({ routingKey, envelope })
  }
}

export const available = (lot: StockLot) => lot.on_hand - lot.reserved

export function requestFingerprint(items: OrderLine[]): string {
  return JSON.stringify(items.map((it) => ({ sku: it.sku, quantity: it.quantity, unit_price_cents: it.unit_price_cents })))
}

export function aggregateLines(items: OrderLine[]): Map<string, number> | null {
  const qty = new Map<string, number>()
  for (const it of items) {
    if (!Number.isSafeInteger(it.quantity) || it.quantity <= 0) return null
    const next = (qty.get(it.sku) ?? 0) + it.quantity
    if (!Number.isSafeInteger(next)) return null
    qty.set(it.sku, next)
  }
  return qty
}

const lotKey = (sku: string, warehouseId: string) => `${sku}\0${warehouseId}`
const rejected = (path: string, message: string): Outcome => ({ kind: 'rejected', errors: [{ path, message }] })

export class MemoryInventoryStore {
  private readonly lots = new Map<string, StockLot>()
  private readonly byOrder = new Map<string, RecordRow>()
  private readonly byMessage = new Map<string, RecordRow>()
  private readonly cancelled = new Set<string>()
  private readonly down = new Set<string>()
  private tail: Promise<void> = Promise.resolve()

  serialize<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.tail.then(fn, fn)
    this.tail = run.then(() => undefined, () => undefined)
    return run
  }

  seed(sku: string, warehouseId: string, onHand: number): boolean {
    if (!sku || !warehouseId || !Number.isSafeInteger(onHand) || onHand < 0) return false
    this.lots.set(lotKey(sku, warehouseId), { sku, warehouse_id: warehouseId, on_hand: onHand, reserved: 0 })
    return true
  }

  markWarehouseDown(id: string) { this.down.add(id) }
  markCancelled(orderId: string) { this.cancelled.add(orderId) }
  isCancelled(orderId: string) { return this.cancelled.has(orderId) }

  lot(sku: string, warehouseId: string): StockLot | undefined {
    const found = this.lots.get(lotKey(sku, warehouseId))
    return found ? { ...found } : undefined
  }

  hold(sku: string, quantity: number): { ok: true; warehouse_id: string } | { ok: false; reason: FailReason } {
    const lots = [...this.lots.values()].filter((l) => l.sku === sku).sort((a, b) => a.warehouse_id.localeCompare(b.warehouse_id))
    if (lots.length === 0) return { ok: false, reason: 'sku_unknown' }
    const up = lots.filter((l) => !this.down.has(l.warehouse_id))
    if (up.length === 0) return { ok: false, reason: 'warehouse_unavailable' }
    const found = up.find((l) => available(l) >= quantity)
    if (!found) return { ok: false, reason: 'out_of_stock' }
    const next = found.reserved + quantity
    if (!Number.isSafeInteger(next) || next > found.on_hand) return { ok: false, reason: 'out_of_stock' }
    found.reserved = next
    return { ok: true, warehouse_id: found.warehouse_id }
  }

  unhold(sku: string, warehouseId: string, quantity: number): boolean {
    const found = this.lots.get(lotKey(sku, warehouseId))
    if (!found || found.reserved < quantity) return false
    found.reserved -= quantity
    return true
  }

  getByOrder(id: string) { return this.byOrder.get(id) }
  getByMessageId(id: string) { return this.byMessage.get(id) }
  put(rec: RecordRow): void {
    this.byOrder.set(rec.reservation.order_id, rec)
    this.byMessage.set(rec.reservation.source_message_id, rec)
  }

  all() { return [...this.byOrder.values()].map((r) => r.reservation) }
}

function asOrderLines(payload: JsonObject): { order_id: string; items: OrderLine[] } | null {
  const order_id = payload['order_id']
  const items = payload['items']
  if (typeof order_id !== 'string' || !Array.isArray(items)) return null
  const parsed: OrderLine[] = []
  for (const raw of items) {
    if (typeof raw !== 'object' || raw === null) return null
    const row = raw as JsonObject
    const sku = row['sku']
    const quantity = row['quantity']
    const unit_price_cents = row['unit_price_cents']
    if (typeof sku !== 'string' || typeof quantity !== 'number' || typeof unit_price_cents !== 'number') return null
    parsed.push({ sku, quantity, unit_price_cents })
  }
  return { order_id, items: parsed }
}

function asCancel(payload: JsonObject): { order_id: string; compensations: string[] } | null {
  const order_id = payload['order_id']
  const compensations = payload['compensations']
  if (typeof order_id !== 'string' || !Array.isArray(compensations)) return null
  if (!compensations.every((c) => typeof c === 'string')) return null
  return { order_id, compensations: compensations as string[] }
}

export class InventoryService {
  constructor(
    readonly store: MemoryInventoryStore,
    readonly publisher: Publisher,
    readonly clock: Clock,
  ) {}

  handle(raw: unknown): Promise<Outcome> {
    return this.store.serialize(() => this.handleLocked(raw))
  }

  private async handleLocked(raw: unknown): Promise<Outcome> {
    const parsed = parseEnvelope(raw)
    if (!parsed.ok) return { kind: 'rejected', errors: parsed.errors }
    const env = parsed.envelope
    if (env.source !== 'orders') return rejected('source', 'expected orders')
    if (env.type === 'events.order_created') return this.reserveLocked(env)
    if (env.type === 'events.order_cancelled') return this.releaseLocked(env)
    return rejected('type', 'expected events.order_created or events.order_cancelled')
  }

  private async reserveLocked(env: MessageEnvelope): Promise<Outcome> {
    const byMessage = this.store.getByMessageId(env.message_id)
    if (byMessage) return this.finishExisting(byMessage)
    const body = asOrderLines(env.payload)
    if (!body) return rejected('payload', 'shape')
    const needed = aggregateLines(body.items)
    if (!needed) return rejected('payload.items', 'overflow')
    const hash = requestFingerprint(body.items)
    const byOrder = this.store.getByOrder(body.order_id)
    if (byOrder) {
      if (byOrder.reservation.status === 'released') {
        return { kind: 'duplicate', reservation: byOrder.reservation, event: byOrder.event ?? undefined }
      }
      if (byOrder.request_hash !== hash) return { kind: 'conflict', reservation: byOrder.reservation }
      return this.finishExisting(byOrder)
    }
    if (this.store.isCancelled(body.order_id)) return this.tombstone(env, body.order_id, hash)

    const held: ReservedLine[] = []
    for (const [sku, quantity] of needed) {
      const result = this.store.hold(sku, quantity)
      if (!result.ok) {
        for (const h of held) this.store.unhold(h.sku, h.warehouse_id, h.quantity)
        return this.persistOutcome(env, body.order_id, hash, 'failed', result.reason, [sku], [])
      }
      held.push({ sku, quantity, warehouse_id: result.warehouse_id })
    }
    return this.persistOutcome(env, body.order_id, hash, 'reserved', undefined, undefined, held)
  }

  private async releaseLocked(env: MessageEnvelope): Promise<Outcome> {
    const body = asCancel(env.payload)
    if (!body) return rejected('payload', 'shape')
    this.store.markCancelled(body.order_id)
    if (!body.compensations.includes('release_inventory')) {
      const existing = this.store.getByOrder(body.order_id)
      return existing
        ? { kind: 'duplicate', reservation: existing.reservation, event: existing.event ?? undefined }
        : rejected('compensations', 'release_inventory')
    }
    const rec = this.store.getByOrder(body.order_id)
    if (!rec) return this.tombstone(env, body.order_id, '')
    if (rec.reservation.status === 'released') {
      return { kind: 'duplicate', reservation: rec.reservation, event: rec.event ?? undefined }
    }
    if (rec.reservation.status === 'reserved') {
      for (const line of rec.reservation.items) {
        if (!this.store.unhold(line.sku, line.warehouse_id, line.quantity)) return rejected('stock', 'hold_missing')
      }
    }
    rec.reservation.status = 'released'
    this.store.put(rec)
    return { kind: 'released', reservation: rec.reservation }
  }

  private tombstone(env: MessageEnvelope, orderId: string, hash: string): Outcome {
    const reservation: Reservation = {
      reservation_id: this.clock.newId(), order_id: orderId, items: [], status: 'released',
      created_at: this.clock.now().toISOString(), correlation_id: env.correlation_id,
      source_message_id: env.message_id,
    }
    this.store.put({ reservation, request_hash: hash, event: null, published: true })
    return { kind: 'released', reservation }
  }

  private async persistOutcome(
    env: MessageEnvelope,
    orderId: string,
    hash: string,
    status: 'reserved' | 'failed',
    reason: FailReason | undefined,
    failedSkus: string[] | undefined,
    items: ReservedLine[],
  ): Promise<Outcome> {
    const createdAt = this.clock.now().toISOString()
    const reservation: Reservation = {
      reservation_id: this.clock.newId(), order_id: orderId, items: items.map((h) => ({ ...h })),
      status, reason, failed_skus: failedSkus, created_at: createdAt, correlation_id: env.correlation_id,
      source_message_id: env.message_id,
    }
    const payload: JsonObject = status === 'reserved'
      ? {
          order_id: orderId, reservation_id: reservation.reservation_id,
          items: items.map((it) => ({ sku: it.sku, quantity: it.quantity, warehouse_id: it.warehouse_id })),
        }
      : { order_id: orderId, reason: reason ?? 'out_of_stock', failed_skus: failedSkus ?? [] }
    const built = createEnvelope({
      type: status === 'reserved' ? 'events.inventory_reserved' : 'events.inventory_reservation_failed',
      source: 'inventory', payload, correlation_id: env.correlation_id,
      message_id: this.clock.newId(), causation_id: env.message_id, occurred_at: createdAt,
    })
    if (!built.ok || !built.envelope) {
      return { kind: 'rejected', errors: built.ok ? [{ path: '$', message: 'envelope' }] : built.errors }
    }
    const rec: RecordRow = { reservation, request_hash: hash, event: built.envelope, published: false }
    this.store.put(rec)
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return status === 'reserved'
      ? { kind: 'reserved', reservation, event: built.envelope }
      : { kind: 'failed', reservation, event: built.envelope }
  }

  private async finishExisting(rec: RecordRow): Promise<Outcome> {
    if (rec.reservation.status === 'released' || !rec.event) {
      return { kind: 'duplicate', reservation: rec.reservation, event: rec.event ?? undefined }
    }
    if (!rec.published) {
      const failed = await this.tryPublish(rec)
      if (failed) return failed
      if (rec.reservation.status !== 'failed') {
        return { kind: 'replayed', reservation: rec.reservation, event: rec.event }
      }
    }
    return rec.reservation.status === 'failed'
      ? { kind: 'failed', reservation: rec.reservation, event: rec.event }
      : { kind: 'duplicate', reservation: rec.reservation, event: rec.event }
  }

  private async tryPublish(rec: RecordRow): Promise<Extract<Outcome, { kind: 'publish_failed' }> | null> {
    if (!rec.event) return null
    const routingKey = rec.event.type === 'events.inventory_reservation_failed'
      ? INVENTORY_FAILED_ROUTING_KEY : INVENTORY_RESERVED_ROUTING_KEY
    try {
      await this.publisher.publish(routingKey, rec.event)
      rec.published = true
      return null
    } catch (caught) {
      const error = caught instanceof Error ? caught : new Error(String(caught))
      return { kind: 'publish_failed', reservation: rec.reservation, event: rec.event, error }
    }
  }
}
