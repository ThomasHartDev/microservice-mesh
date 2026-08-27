import { describe, expect, it } from 'vitest'
import { createEnvelope, parseEnvelope, type JsonObject, type MessageType, type ServiceName } from '../src/index.js'
import {
  MemoryPublisher,
  MemorySagaStore,
  ORDER_CANCELLED_ROUTING_KEY,
  ORDER_COMPLETED_ROUTING_KEY,
  OrderSagaOrchestrator,
  compensationsFor,
  type Clock,
} from '../src/saga.js'

const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const C = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'
const D = '6ba7b812-9dad-11d1-80b4-00c04fd430c8'
const E = '6ba7b813-9dad-11d1-80b4-00c04fd430c8'
const F = '6ba7b814-9dad-11d1-80b4-00c04fd430c8'
const G = '6ba7b815-9dad-11d1-80b4-00c04fd430c8'
const H = '6ba7b816-9dad-11d1-80b4-00c04fd430c8'
const AT = '2026-08-27T18:00:00.000Z'
const items = [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }]

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

function env(type: MessageType, source: ServiceName, payload: JsonObject, messageId: string) {
  const built = createEnvelope({ type, source, payload, correlation_id: B, message_id: messageId, occurred_at: AT })
  if (!built.ok || !built.envelope) throw new Error('fixture')
  return built.envelope
}
const created = (messageId = A) =>
  env('events.order_created', 'orders', {
    order_id: A, customer_id: A, items, currency: 'USD', total_cents: 3000,
  }, messageId)
const reserved = (messageId: string) =>
  env('events.inventory_reserved', 'inventory', {
    order_id: A, reservation_id: C, items: [{ sku: 'SKU-1', quantity: 2, warehouse_id: 'wh-east' }],
  }, messageId)
const invFailed = (messageId: string) =>
  env('events.inventory_reservation_failed', 'inventory', {
    order_id: A, reason: 'out_of_stock', failed_skus: ['SKU-1'],
  }, messageId)
const charged = (messageId: string, amount = 3000) =>
  env('events.payment_charged', 'payments', {
    order_id: A, payment_id: D, amount_cents: amount, currency: 'USD',
  }, messageId)
const payFailed = (messageId: string) =>
  env('events.payment_failed', 'payments', {
    order_id: A, reason: 'card_declined', retryable: false,
  }, messageId)

function orch(ids: string[] = [E, F]) {
  const publisher = new MemoryPublisher()
  return { publisher, svc: new OrderSagaOrchestrator(new MemorySagaStore(), publisher, clock(ids)) }
}
describe('compensationsFor', () => {
  it('orders refund then release then notify (reverse of reserve then charge)', () => {
    expect(compensationsFor(false, false)).toEqual(['notify_customer'])
    expect(compensationsFor(true, false)).toEqual(['release_inventory', 'notify_customer'])
    expect(compensationsFor(false, true)).toEqual(['refund_payment', 'notify_customer'])
    expect(compensationsFor(true, true)).toEqual(['refund_payment', 'release_inventory', 'notify_customer'])
  })
})

describe('OrderSagaOrchestrator', () => {
  it('completes after reserve then charge and publishes order_completed', async () => {
    const { publisher, svc } = orch()
    expect((await svc.handle(created())).kind).toBe('started')
    const mid = await svc.handle(reserved(F))
    expect(mid.kind).toBe('awaiting')
    if (mid.kind !== 'awaiting') return
    expect(mid.saga.status).toBe('awaiting_payment')
    const out = await svc.handle(charged(G))
    expect(out.kind).toBe('completed')
    if (out.kind !== 'completed') return
    expect(out.event).toMatchObject({
      type: 'events.order_completed', source: 'orders', causation_id: G, message_id: E,
    })
    expect(out.event.payload).toEqual({
      order_id: A, payment_id: D, reservation_id: C, total_cents: 3000,
    })
    expect(parseEnvelope(out.event).ok).toBe(true)
    expect(publisher.events[0]?.routingKey).toBe(ORDER_COMPLETED_ROUTING_KEY)
  })

  it('parks an early charge until inventory reserved, then completes', async () => {
    const { svc } = orch()
    await svc.handle(created())
    const parked = await svc.handle(charged(F))
    expect(parked.kind).toBe('awaiting')
    if (parked.kind !== 'awaiting') return
    expect(parked.saga.status).toBe('awaiting_inventory')
    const out = await svc.handle(reserved(G))
    expect(out.kind).toBe('completed')
  })

  it('cancels with LIFO compensations for each failure mode', async () => {
    const a = orch()
    await a.svc.handle(created())
    const inv = await a.svc.handle(invFailed(F))
    expect(inv.kind).toBe('cancelled')
    if (inv.kind !== 'cancelled') return
    expect(inv.saga.compensations).toEqual(['notify_customer'])
    expect(parseEnvelope(inv.event).ok).toBe(true)
    expect(a.publisher.events[0]?.routingKey).toBe(ORDER_CANCELLED_ROUTING_KEY)

    const b = orch()
    await b.svc.handle(created())
    await b.svc.handle(reserved(F))
    const pay = await b.svc.handle(payFailed(G))
    expect(pay.kind).toBe('cancelled')
    if (pay.kind !== 'cancelled') return
    expect(pay.saga.compensations).toEqual(['release_inventory', 'notify_customer'])

    const c = orch()
    await c.svc.handle(created())
    await c.svc.handle(charged(F))
    const parked = await c.svc.handle(invFailed(G))
    expect(parked.kind).toBe('cancelled')
    if (parked.kind !== 'cancelled') return
    expect(parked.saga.compensations).toEqual(['refund_payment', 'notify_customer'])
    expect(parked.saga.payment_id).toBe(D)

    const d = orch()
    await d.svc.handle(created())
    await d.svc.handle(reserved(F))
    const mismatch = await d.svc.handle(charged(G, 1))
    expect(mismatch.kind).toBe('cancelled')
    if (mismatch.kind !== 'cancelled') return
    expect(mismatch.saga.compensations).toEqual(['refund_payment', 'release_inventory', 'notify_customer'])
  })

  it('refunds a late charge after inventory already cancelled the saga', async () => {
    const { svc } = orch([E, H])
    await svc.handle(created())
    const first = await svc.handle(invFailed(F))
    expect(first.kind).toBe('cancelled')
    const late = await svc.handle(charged(G))
    expect(late.kind).toBe('cancelled')
    if (late.kind !== 'cancelled') return
    expect(late.saga.compensations).toEqual(['refund_payment', 'notify_customer'])
    expect(late.event.causation_id).toBe(G)
    expect(late.event.message_id).toBe(H)
  })

  it('replays unpublished terminals after broker failure', async () => {
    const { publisher, svc } = orch()
    await svc.handle(created())
    await svc.handle(reserved(F))
    publisher.fail = new Error('nats down')
    const failed = await svc.handle(charged(G))
    expect(failed.kind).toBe('publish_failed')
    publisher.fail = null
    const replay = await svc.handle(charged(G))
    expect(replay.kind).toBe('replayed')
    if (replay.kind !== 'replayed') return
    expect(replay.event.message_id).toBe(E)
    expect(publisher.events).toHaveLength(1)
    const dup = await svc.handle(charged(G))
    expect(dup.kind).toBe('duplicate')
  })

  it('rejects empty input, unknown orders, and wrong producers', async () => {
    const { svc } = orch()
    expect((await svc.handle(null)).kind).toBe('rejected')
    expect((await svc.handle({})).kind).toBe('rejected')
    expect((await svc.handle(reserved(F))).kind).toBe('rejected')
    expect((await svc.handle(env('commands.place_order', 'gateway', {
      customer_id: A, items, currency: 'USD', idempotency_key: 'client-key-001',
    }, A))).kind).toBe('rejected')
    await svc.handle(created())
    expect((await svc.handle(env('events.inventory_reserved', 'orders', {
      order_id: A, reservation_id: C, items: [{ sku: 'SKU-1', quantity: 2, warehouse_id: 'wh-east' }],
    }, F))).kind).toBe('rejected')
  })

  it('conflicts a second cart on the same order_id and serializes concurrent replies', async () => {
    const { svc } = orch()
    await svc.handle(created())
    const clash = await svc.handle(env('events.order_created', 'orders', {
      order_id: A, customer_id: A, items, currency: 'EUR', total_cents: 3000,
    }, F))
    expect(clash.kind).toBe('conflict')
    const same = await svc.handle(env('events.order_created', 'orders', {
      order_id: A, customer_id: A, items, currency: 'USD', total_cents: 3000,
    }, G))
    expect(same.kind).toBe('duplicate')

    const { svc: s2 } = orch()
    await s2.handle(created())
    const [a, b] = await Promise.all([s2.handle(reserved(F)), s2.handle(charged(G))])
    const kinds = [a.kind, b.kind].sort()
    expect(kinds).toEqual(['awaiting', 'completed'])
  })
})
