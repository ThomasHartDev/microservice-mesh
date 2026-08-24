import { describe, expect, it } from 'vitest'
import { createEnvelope, parseEnvelope, type JsonObject } from '../src/index.js'
import {
  InventoryService,
  INVENTORY_FAILED_ROUTING_KEY,
  INVENTORY_RESERVED_ROUTING_KEY,
  MemoryInventoryStore,
  MemoryPublisher,
  aggregateLines,
  available,
  requestFingerprint,
  type Clock,
  type OrderLine,
} from '../src/inventory.js'

const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const C = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'
const D = '6ba7b812-9dad-11d1-80b4-00c04fd430c8'
const E = '6ba7b813-9dad-11d1-80b4-00c04fd430c8'
const F = '6ba7b814-9dad-11d1-80b4-00c04fd430c8'
const G = '6ba7b815-9dad-11d1-80b4-00c04fd430c8'
const AT = '2026-08-24T18:00:00.000Z'
const items: OrderLine[] = [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }]

function clock(ids: string[]): Clock {
  let i = 0
  return {
    now: () => new Date(AT),
    newId: () => {
      const id = ids[i]
      if (!id) throw new Error('clock exhausted')
      i += 1
      return id
    },
  }
}

function created(payload: JsonObject, messageId: string, source: 'orders' | 'gateway' = 'orders') {
  const built = createEnvelope({
    type: 'events.order_created', source, payload, correlation_id: B, message_id: messageId, occurred_at: AT,
  })
  if (!built.ok || !built.envelope) throw new Error('fixture')
  return built.envelope
}

function cancelled(orderId: string, messageId: string) {
  const built = createEnvelope({
    type: 'events.order_cancelled', source: 'orders',
    payload: { order_id: orderId, reason: 'payment_failed', compensations: ['release_inventory'] },
    correlation_id: B, message_id: messageId, occurred_at: AT,
  })
  if (!built.ok || !built.envelope) throw new Error('fixture')
  return built.envelope
}

function orderPayload(extra: Partial<{ order_id: string; items: OrderLine[] }> = {}): JsonObject {
  return { order_id: extra.order_id ?? A, customer_id: A, items: extra.items ?? items, currency: 'USD', total_cents: 3000 }
}

function service(onHand = 10, ids: string[] = [C, D]) {
  const store = new MemoryInventoryStore()
  store.seed('SKU-1', 'wh-east', onHand)
  const publisher = new MemoryPublisher()
  return { store, publisher, svc: new InventoryService(store, publisher, clock(ids)) }
}

describe('InventoryService', () => {
  it('reserves stock and emits inventory_reserved with warehouse assignment', async () => {
    const { store, publisher, svc } = service()
    const out = await svc.handle(created(orderPayload(), A))
    expect(out.kind).toBe('reserved')
    if (out.kind !== 'reserved') return
    expect(out.reservation).toMatchObject({
      reservation_id: C, order_id: A, status: 'reserved',
      items: [{ sku: 'SKU-1', quantity: 2, warehouse_id: 'wh-east' }],
    })
    expect(out.event).toMatchObject({ type: 'events.inventory_reserved', source: 'inventory', causation_id: A, message_id: D })
    expect(parseEnvelope(out.event).ok).toBe(true)
    expect(publisher.events[0]?.routingKey).toBe(INVENTORY_RESERVED_ROUTING_KEY)
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(8)
  })

  it('rolls back earlier holds when a later SKU would oversell', async () => {
    const { store, publisher, svc } = service()
    store.seed('SKU-2', 'wh-east', 1)
    const cart: OrderLine[] = [
      { sku: 'SKU-1', quantity: 4, unit_price_cents: 100 },
      { sku: 'SKU-2', quantity: 2, unit_price_cents: 100 },
    ]
    const out = await svc.handle(created(orderPayload({ items: cart }), A))
    expect(out.kind).toBe('failed')
    if (out.kind !== 'failed') return
    expect(out.reservation).toMatchObject({ status: 'failed', reason: 'out_of_stock', failed_skus: ['SKU-2'] })
    expect(publisher.events[0]?.routingKey).toBe(INVENTORY_FAILED_ROUTING_KEY)
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(10)
    expect(available(store.lot('SKU-2', 'wh-east')!)).toBe(1)
  })

  it('lets one last-unit reservation win and fails the rest without overselling', async () => {
    const { store, publisher, svc } = service(1, [C, D, E, F, G, A])
    const line: OrderLine[] = [{ sku: 'SKU-1', quantity: 1, unit_price_cents: 1500 }]
    const kinds = (await Promise.all([
      svc.handle(created(orderPayload({ order_id: A, items: line }), A)),
      svc.handle(created(orderPayload({ order_id: E, items: line }), E)),
    ])).map((r) => r.kind).sort()
    expect(kinds).toEqual(['failed', 'reserved'])
    expect(store.lot('SKU-1', 'wh-east')!.reserved).toBe(1)
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(0)
    expect(publisher.events).toHaveLength(2)
  })

  it('fails unknown SKUs and downed warehouses without touching other lots', async () => {
    const { store, publisher, svc } = service(10, [C, D, E, F])
    const unknown = await svc.handle(created(orderPayload({ items: [{ sku: 'NOPE', quantity: 1, unit_price_cents: 1 }] }), A))
    expect(unknown.kind).toBe('failed')
    if (unknown.kind === 'failed') expect(unknown.reservation.reason).toBe('sku_unknown')
    store.markWarehouseDown('wh-east')
    const down = await svc.handle(created(orderPayload({ order_id: E }), E))
    expect(down.kind).toBe('failed')
    if (down.kind === 'failed') expect(down.reservation.reason).toBe('warehouse_unavailable')
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(10)
    expect(publisher.events).toHaveLength(2)
  })

  it('releases reserved units on cancel and ignores a late order_created', async () => {
    const { store, publisher, svc } = service(5, [C, D, E, F])
    expect((await svc.handle(created(orderPayload(), A))).kind).toBe('reserved')
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(3)
    expect((await svc.handle(cancelled(A, E))).kind).toBe('released')
    expect(available(store.lot('SKU-1', 'wh-east')!)).toBe(5)
    expect((await svc.handle(cancelled(A, F))).kind).toBe('duplicate')
    expect((await svc.handle(created(orderPayload(), A))).kind).toBe('duplicate')
    const late = service(5, [C, D])
    expect((await late.svc.handle(cancelled(A, E))).kind).toBe('released')
    expect((await late.svc.handle(created(orderPayload(), A))).kind).toBe('duplicate')
    expect(available(late.store.lot('SKU-1', 'wh-east')!)).toBe(5)
    expect(publisher.events).toHaveLength(1)
    expect(late.publisher.events).toHaveLength(0)
  })

  it('replays the same order, conflicts on a different cart, and republishes after broker failure', async () => {
    const { store, publisher, svc } = service()
    publisher.fail = new Error('down')
    const cmd = created(orderPayload(), A)
    expect((await svc.handle(cmd)).kind).toBe('publish_failed')
    expect(store.lot('SKU-1', 'wh-east')!.reserved).toBe(2)
    publisher.fail = null
    const replay = await svc.handle(cmd)
    expect(replay.kind).toBe('replayed')
    if (replay.kind === 'replayed') expect(replay.event.message_id).toBe(D)
    expect((await svc.handle(cmd)).kind).toBe('duplicate')
    const clash = await svc.handle(created(orderPayload({ items: [{ sku: 'SKU-1', quantity: 1, unit_price_cents: 1 }] }), E))
    expect(clash.kind).toBe('conflict')
    expect(publisher.events).toHaveLength(1)
  })

  it('rejects bad envelopes, overflow, empty SKU sums, and bad seed', async () => {
    const { store, publisher, svc } = service()
    expect((await svc.handle(null)).kind).toBe('rejected')
    expect((await svc.handle(created(orderPayload(), A, 'gateway'))).kind).toBe('rejected')
    const huge = await svc.handle(created(orderPayload({
      items: [{ sku: 'SKU-1', quantity: Number.MAX_SAFE_INTEGER, unit_price_cents: 1 }, { sku: 'SKU-1', quantity: 1, unit_price_cents: 1 }],
    }), A))
    expect(huge.kind).toBe('rejected')
    expect(aggregateLines([{ sku: 'A', quantity: 2, unit_price_cents: 1 }, { sku: 'A', quantity: 3, unit_price_cents: 1 }])?.get('A')).toBe(5)
    expect(aggregateLines([{ sku: 'A', quantity: 0, unit_price_cents: 1 }])).toBeNull()
    expect(requestFingerprint(items)).not.toBe(requestFingerprint([{ ...items[0]!, sku: 'SKU-2' }]))
    expect(store.seed('', 'wh-east', 1)).toBe(false)
    expect(store.seed('SKU-1', 'wh-west', -1)).toBe(false)
    expect(publisher.events).toHaveLength(0)
    expect(store.all()).toHaveLength(0)
  })
})
