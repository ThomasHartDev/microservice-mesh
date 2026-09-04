import { describe, expect, it } from 'vitest'
import { createEnvelope, parseEnvelope, type JsonObject } from '../src/index.js'
import {
  MemoryOrderStore,
  MemoryPublisher,
  OrdersService,
  ORDER_CREATED_ROUTING_KEY,
  requestFingerprint,
  totalCents,
  type Clock,
  type LineItem,
} from '../src/orders.js'

const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const C = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'
const D = '6ba7b812-9dad-11d1-80b4-00c04fd430c8'
const E = '6ba7b813-9dad-11d1-80b4-00c04fd430c8'
const F = '6ba7b814-9dad-11d1-80b4-00c04fd430c8'
const G = '6ba7b815-9dad-11d1-80b4-00c04fd430c8'
const H = '6ba7b816-9dad-11d1-80b4-00c04fd430c8'
const AT = '2026-08-17T16:00:00.000Z'
const items: LineItem[] = [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }]
const placeOrder: JsonObject = {
  customer_id: A,
  items,
  currency: 'USD',
  idempotency_key: 'checkout-1',
}

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

function command(payload: JsonObject, messageId: string, source: 'gateway' | 'orders' = 'gateway') {
  const built = createEnvelope({
    type: 'commands.place_order',
    source,
    payload,
    correlation_id: B,
    message_id: messageId,
    occurred_at: AT,
  })
  if (!built.ok || !built.envelope) throw new Error('fixture')
  return built.envelope
}

function service(ids: string[] = [C, D]) {
  const store = new MemoryOrderStore()
  const publisher = new MemoryPublisher()
  return { store, publisher, svc: new OrdersService(store, publisher, clock(ids)) }
}

describe('totalCents', () => {
  it('sums lines and rejects unsafe integers', () => {
    expect(totalCents(items)).toBe(3000)
    expect(totalCents([{ sku: 'Z', quantity: 1, unit_price_cents: 0 }])).toBe(0)
    expect(totalCents([])).toBe(0)
    expect(totalCents([{ sku: 'Z', quantity: 3, unit_price_cents: Number.MAX_SAFE_INTEGER }])).toBe(null)
    expect(
      totalCents([
        { sku: 'A', quantity: 1, unit_price_cents: Number.MAX_SAFE_INTEGER },
        { sku: 'B', quantity: 1, unit_price_cents: 1 },
      ]),
    ).toBe(null)
    expect(totalCents([{ sku: 'Z', quantity: 1.5, unit_price_cents: 10 }])).toBe(null)
  })
})

describe('OrdersService', () => {
  it('persists place_order and emits order_created with correlation/causation', async () => {
    const { store, publisher, svc } = service()
    const out = await svc.handle(command(placeOrder, A))
    expect(out.kind).toBe('created')
    if (out.kind !== 'created') return
    expect(out.order).toMatchObject({
      order_id: C,
      customer_id: A,
      currency: 'USD',
      total_cents: 3000,
      idempotency_key: 'checkout-1',
      status: 'created',
      created_at: AT,
      correlation_id: B,
      source_message_id: A,
    })
    expect(out.order.items).toEqual(items)
    expect(out.event).toMatchObject({
      type: 'events.order_created',
      source: 'orders',
      correlation_id: B,
      causation_id: A,
      message_id: D,
      payload: { order_id: C, customer_id: A, items, currency: 'USD', total_cents: 3000 },
    })
    expect(parseEnvelope(out.event).ok).toBe(true)
    expect(publisher.events[0]?.routingKey).toBe(ORDER_CREATED_ROUTING_KEY)
    expect(store.all()).toHaveLength(1)
    expect(store.unpublished()).toHaveLength(0)
  })

  it('rejects bad envelopes, wrong type, non-gateway source, and overflow', async () => {
    const { store, publisher, svc } = service()
    expect((await svc.handle(null)).kind).toBe('rejected')
    expect((await svc.handle({})).kind).toBe('rejected')
    const created = createEnvelope({
      type: 'events.order_created',
      source: 'orders',
      payload: { order_id: A, customer_id: A, items, currency: 'USD', total_cents: 3000 },
      correlation_id: B,
      message_id: A,
      occurred_at: AT,
    })
    if (!created.ok || !created.envelope) return
    expect((await svc.handle(created.envelope)).kind).toBe('rejected')
    expect((await svc.handle(command(placeOrder, A, 'orders'))).kind).toBe('rejected')
    const huge: JsonObject = {
      ...placeOrder,
      items: [{ sku: 'SKU-1', quantity: 3, unit_price_cents: Number.MAX_SAFE_INTEGER }],
    }
    const overflow = await svc.handle(command(huge, A))
    expect(overflow.kind).toBe('rejected')
    if (overflow.kind === 'rejected') expect(overflow.errors[0]?.message).toBe('overflow')
    expect(store.all()).toHaveLength(0)
    expect(publisher.events).toHaveLength(0)
  })

  it('dedups message_id and matching idempotency_key, conflicts on a different body', async () => {
    const { store, publisher, svc } = service()
    const cmd = command(placeOrder, A)
    expect((await svc.handle(cmd)).kind).toBe('created')
    expect((await svc.handle(cmd)).kind).toBe('duplicate')
    const sameKey = await svc.handle(command(placeOrder, E))
    expect(sameKey.kind).toBe('duplicate')
    if (sameKey.kind === 'duplicate') expect(sameKey.order.order_id).toBe(C)
    const other: JsonObject = {
      ...placeOrder,
      items: [{ sku: 'SKU-2', quantity: 1, unit_price_cents: 100 }],
    }
    const clash = await svc.handle(command(other, E))
    expect(clash.kind).toBe('conflict')
    expect(store.all()).toHaveLength(1)
    expect(publisher.events).toHaveLength(1)
    const fpA = requestFingerprint({
      customer_id: A,
      currency: 'USD',
      items: [
        { sku: 'A', quantity: 1, unit_price_cents: 1 },
        { sku: 'B', quantity: 1, unit_price_cents: 1 },
      ],
    })
    const fpB = requestFingerprint({
      customer_id: A,
      currency: 'USD',
      items: [
        { sku: 'B', quantity: 1, unit_price_cents: 1 },
        { sku: 'A', quantity: 1, unit_price_cents: 1 },
      ],
    })
    expect(fpA).not.toBe(fpB)
  })

  it('conflicts when two carts share the old NUL/newline join but differ as JSON', async () => {
    const { store, publisher, svc } = service()
    const mergedSku = 'x\u00001\u00001\ny'
    const firstItems: LineItem[] = [{ sku: mergedSku, quantity: 2, unit_price_cents: 3 }]
    const secondItems: LineItem[] = [
      { sku: 'x', quantity: 1, unit_price_cents: 1 },
      { sku: 'y', quantity: 2, unit_price_cents: 3 },
    ]
    expect(
      requestFingerprint({ customer_id: A, currency: 'USD', items: firstItems }),
    ).not.toBe(requestFingerprint({ customer_id: A, currency: 'USD', items: secondItems }))

    const first = await svc.handle(
      command({ ...placeOrder, items: firstItems }, A),
    )
    expect(first.kind).toBe('created')
    const clash = await svc.handle(
      command({ ...placeOrder, items: secondItems }, F),
    )
    expect(clash.kind).toBe('conflict')
    if (clash.kind === 'conflict') {
      expect(clash.order.order_id).toBe(C)
      expect(clash.order.items).toEqual(firstItems)
    }
    expect(store.all()).toHaveLength(1)
    expect(publisher.events).toHaveLength(1)
  })

  it('conflicts when only customer_id or only currency changes', async () => {
    const { store, publisher, svc } = service()
    expect((await svc.handle(command(placeOrder, A))).kind).toBe('created')

    const otherCustomer = await svc.handle(
      command({ ...placeOrder, customer_id: H }, F),
    )
    expect(otherCustomer.kind).toBe('conflict')
    if (otherCustomer.kind === 'conflict') {
      expect(otherCustomer.order.order_id).toBe(C)
      expect(otherCustomer.order.customer_id).toBe(A)
      expect(otherCustomer.order.items).toEqual(items)
    }

    const otherCurrency = await svc.handle(
      command({ ...placeOrder, currency: 'EUR' }, G),
    )
    expect(otherCurrency.kind).toBe('conflict')
    if (otherCurrency.kind === 'conflict') {
      expect(otherCurrency.order.order_id).toBe(C)
      expect(otherCurrency.order.currency).toBe('USD')
      expect(otherCurrency.order.items).toEqual(items)
    }

    expect(store.all()).toHaveLength(1)
    expect(publisher.events).toHaveLength(1)
  })

  it('persists first, then republishes the same outbox event after broker failure', async () => {
    const { store, publisher, svc } = service()
    publisher.fail = new Error('down')
    const cmd = command(placeOrder, A)
    const first = await svc.handle(cmd)
    expect(first.kind).toBe('publish_failed')
    expect(store.getById(C)?.published).toBe(false)
    expect(publisher.events).toHaveLength(0)
    publisher.fail = null
    const second = await svc.handle(cmd)
    expect(second.kind).toBe('replayed')
    if (second.kind === 'replayed') expect(second.event.message_id).toBe(D)
    expect(publisher.events).toHaveLength(1)
    expect(store.unpublished()).toHaveLength(0)
    publisher.fail = new Error('down')
    const { publisher: p2, svc: s2 } = service()
    p2.fail = new Error('down')
    expect((await s2.handle(command(placeOrder, A))).kind).toBe('publish_failed')
    p2.fail = null
    const retry = await s2.handle(command(placeOrder, E))
    expect(retry.kind).toBe('replayed')
    if (retry.kind === 'replayed') expect(retry.order.source_message_id).toBe(A)
    expect(p2.events).toHaveLength(1)
  })

  it('serializes concurrent handlers and accepts a second distinct order', async () => {
    const { store, publisher, svc } = service()
    const kinds = (await Promise.all([
      svc.handle(command(placeOrder, A)),
      svc.handle(command(placeOrder, A)),
      svc.handle(command(placeOrder, E)),
    ])).map((r) => r.kind).sort()
    expect(kinds).toEqual(['created', 'duplicate', 'duplicate'])
    expect(store.all()).toHaveLength(1)
    expect(publisher.events).toHaveLength(1)

    const next = service([C, D, F, G])
    expect((await next.svc.handle(command(placeOrder, A))).kind).toBe('created')
    const second = await next.svc.handle(command({ ...placeOrder, idempotency_key: 'checkout-2' }, E))
    expect(second.kind).toBe('created')
    if (second.kind === 'created') expect(second.order.order_id).toBe(F)
    expect(next.store.all()).toHaveLength(2)
    expect(next.publisher.events).toHaveLength(2)
  })
})
