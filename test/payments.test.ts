import { describe, expect, it } from 'vitest'
import { parseEnvelope } from '../src/index.js'
import {
  MemoryPaymentStore,
  MemoryPublisher,
  PAYMENT_CHARGED_ROUTING_KEY,
  PAYMENT_FAILED_ROUTING_KEY,
  PaymentsService,
  reserveFingerprint,
  type Clock,
} from '../src/payments.js'

const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const C = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'
const D = '6ba7b812-9dad-11d1-80b4-00c04fd430c8'
const E = '6ba7b813-9dad-11d1-80b4-00c04fd430c8'
const F = '6ba7b814-9dad-11d1-80b4-00c04fd430c8'
const G = '6ba7b815-9dad-11d1-80b4-00c04fd430c8'
const AT = '2026-08-21T16:00:00.000Z'

function clock(ids: string[]): Clock {
  let i = 0
  return {
    now: () => new Date(AT),
    newId: () => {
      const id = ids[i++]
      if (!id) throw new Error('clock exhausted')
      return id
    },
  }
}

function svc(ids: string[] = [C, D], credit = 10_000) {
  const store = new MemoryPaymentStore()
  store.credit(A, credit)
  const publisher = new MemoryPublisher()
  return { store, publisher, pay: new PaymentsService(store, publisher, clock(ids)) }
}

function req(over: Partial<{ order_id: string; amount: number; key: string; customer: string }> = {}) {
  return {
    order_id: over.order_id ?? B, customer_id: over.customer ?? A, amount_cents: over.amount ?? 3000,
    currency: 'USD' as const, idempotency_key: over.key ?? 'reserve-1', correlation_id: A, causation_id: E,
  }
}

describe('PaymentsService', () => {
  it('reserves a hold then charges and emits payment_charged', async () => {
    const { store, publisher, pay } = svc()
    const reserved = await pay.reserve(req())
    expect(reserved.kind).toBe('reserved')
    if (reserved.kind !== 'reserved') return
    expect(reserved.payment).toMatchObject({
      payment_id: C, order_id: B, status: 'reserved', reserved_cents: 3000, captured_cents: 0,
    })
    expect(store.ledger(A)).toEqual({ customer_id: A, available_cents: 7000, held_cents: 3000 })

    const charged = await pay.charge({ order_id: B, idempotency_key: 'charge-1', causation_id: E })
    expect(charged.kind).toBe('charged')
    if (charged.kind !== 'charged') return
    expect(charged.event).toMatchObject({
      type: 'events.payment_charged', source: 'payments', correlation_id: A, causation_id: E, message_id: D,
      payload: { order_id: B, payment_id: C, amount_cents: 3000, currency: 'USD' },
    })
    expect(parseEnvelope(charged.event).ok).toBe(true)
    expect(publisher.events[0]?.routingKey).toBe(PAYMENT_CHARGED_ROUTING_KEY)
    expect(store.ledger(A)).toEqual({ customer_id: A, available_cents: 7000, held_cents: 0 })
  })

  it('rejects zero/overflow amounts, short keys, and charge without a reserve', async () => {
    const { store, pay } = svc()
    expect((await pay.reserve(req({ amount: 0 }))).kind).toBe('rejected')
    expect((await pay.reserve(req({ amount: 1.5 }))).kind).toBe('rejected')
    expect((await pay.reserve(req({ key: 'short' }))).kind).toBe('rejected')
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1' })).kind).toBe('rejected')
    expect(store.all()).toHaveLength(0)
  })

  it('replays matching reserve keys and conflicts on a different body or a second key for the same order', async () => {
    const { store, pay } = svc()
    expect((await pay.reserve(req())).kind).toBe('reserved')
    expect((await pay.reserve(req())).kind).toBe('duplicate')
    expect((await pay.reserve(req({ amount: 1000 }))).kind).toBe('conflict')
    expect((await pay.reserve(req({ key: 'reserve-2' }))).kind).toBe('conflict')
    expect((await pay.reserve(req({ customer: G }))).kind).toBe('conflict')
    expect(store.all()).toHaveLength(1)
    expect(store.ledger(A).held_cents).toBe(3000)
    expect(
      reserveFingerprint({ order_id: B, customer_id: A, amount_cents: 1, currency: 'USD' }),
    ).not.toBe(reserveFingerprint({ order_id: B, customer_id: A, amount_cents: 2, currency: 'USD' }))
  })

  it('occupies the key on insufficient_funds and card_declined; processor_error does not', async () => {
    const low = svc([C, D], 100)
    const failed = await low.pay.reserve(req())
    expect(failed.kind).toBe('failed')
    if (failed.kind !== 'failed') return
    expect(failed.payment).toMatchObject({ reason: 'insufficient_funds', retryable: false })
    expect(failed.event.type).toBe('events.payment_failed')
    expect(low.publisher.events[0]?.routingKey).toBe(PAYMENT_FAILED_ROUTING_KEY)
    low.store.credit(A, 50_000)
    expect((await low.pay.reserve(req())).kind).toBe('duplicate')
    expect(low.store.ledger(A).held_cents).toBe(0)

    const blocked = svc()
    blocked.pay.blocked.add(A)
    const declined = await blocked.pay.reserve(req({ order_id: F, key: 'reserve-b' }))
    expect(declined.kind).toBe('failed')
    if (declined.kind === 'failed') expect(declined.payment.reason).toBe('card_declined')

    const { store, publisher, pay } = svc()
    pay.failNext = 'authorize'
    expect((await pay.reserve(req())).kind).toBe('rejected')
    expect(store.all()).toHaveLength(0)
    expect((await pay.reserve(req())).kind).toBe('reserved')
    pay.failNext = 'capture'
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1' })).kind).toBe('rejected')
    expect(store.getByOrder(B)?.payment.status).toBe('reserved')
    expect(store.ledger(A).held_cents).toBe(3000)
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1' })).kind).toBe('charged')
    expect(publisher.events).toHaveLength(1)
  })

  it('persists the charged event first and republishes the same envelope after broker failure', async () => {
    const { store, publisher, pay } = svc()
    expect((await pay.reserve(req())).kind).toBe('reserved')
    publisher.fail = new Error('down')
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1', causation_id: E })).kind).toBe(
      'publish_failed',
    )
    expect(store.getById(C)?.published).toBe(false)
    expect(publisher.events).toHaveLength(0)
    publisher.fail = null
    const second = await pay.charge({ order_id: B, idempotency_key: 'charge-1' })
    expect(second.kind).toBe('replayed')
    if (second.kind !== 'replayed') return
    expect(second.event.message_id).toBe(D)
    expect(publisher.events).toHaveLength(1)
    expect(store.unpublished()).toHaveLength(0)
  })

  it('serializes concurrent reserve/charge and accepts a second distinct order', async () => {
    const { store, publisher, pay } = svc()
    const kinds = (await Promise.all([pay.reserve(req()), pay.reserve(req()), pay.reserve(req())]))
      .map((r) => r.kind)
      .sort()
    expect(kinds).toEqual(['duplicate', 'duplicate', 'reserved'])
    expect(store.ledger(A).held_cents).toBe(3000)
    const cap = (
      await Promise.all([
        pay.charge({ order_id: B, idempotency_key: 'charge-1' }),
        pay.charge({ order_id: B, idempotency_key: 'charge-1' }),
      ])
    )
      .map((r) => r.kind)
      .sort()
    expect(cap).toEqual(['charged', 'duplicate'])
    expect(publisher.events).toHaveLength(1)

    const next = svc([C, D, F, G])
    expect((await next.pay.reserve(req())).kind).toBe('reserved')
    const other = await next.pay.reserve(req({ order_id: E, key: 'reserve-2' }))
    expect(other.kind).toBe('reserved')
    if (other.kind === 'reserved') expect(other.payment.payment_id).toBe(D)
    expect(next.store.all()).toHaveLength(2)
  })

  it('charges a failed reserve as failed, not duplicate, and leaves the hold at 0', async () => {
    const { store, pay } = svc([C, D], 100)
    const failed = await pay.reserve(req())
    expect(failed.kind).toBe('failed')
    if (failed.kind !== 'failed') return
    const charged = await pay.charge({ order_id: B, idempotency_key: 'charge-1' })
    expect(charged.kind).toBe('failed')
    expect(charged.kind).not.toBe('duplicate')
    if (charged.kind !== 'failed') return
    expect(charged.payment.reason).toBe('insufficient_funds')
    expect(charged.event.message_id).toBe(failed.event.message_id)
    expect(store.ledger(A).held_cents).toBe(0)
    expect(store.getByOrder(B)?.payment.status).toBe('failed')
  })

  it('conflicts when the same charge key is reused for a different order', async () => {
    const { pay } = svc([C, D, F])
    expect((await pay.reserve(req())).kind).toBe('reserved')
    expect((await pay.reserve(req({ order_id: E, key: 'reserve-2' }))).kind).toBe('reserved')
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1' })).kind).toBe('charged')
    const clash = await pay.charge({ order_id: E, idempotency_key: 'charge-1' })
    expect(clash.kind).toBe('conflict')
    if (clash.kind === 'conflict') expect(clash.payment.order_id).toBe(B)
  })

  it('occupies a charge key used against a failed payment so a later order cannot reuse it', async () => {
    const { store, pay } = svc([C, D, F], 100)
    const failed = await pay.reserve(req())
    expect(failed.kind).toBe('failed')
    const first = await pay.charge({ order_id: B, idempotency_key: 'charge-k' })
    expect(first.kind).toBe('failed')
    if (first.kind === 'failed') expect(first.payment.order_id).toBe(B)
    const retry = await pay.charge({ order_id: B, idempotency_key: 'charge-k' })
    expect(retry.kind).toBe('failed')
    expect(retry.kind).not.toBe('duplicate')
    store.credit(A, 50_000)
    expect((await pay.reserve(req({ order_id: E, key: 'reserve-2' }))).kind).toBe('reserved')
    const reuse = await pay.charge({ order_id: E, idempotency_key: 'charge-k' })
    expect(reuse.kind).toBe('conflict')
    if (reuse.kind === 'conflict') expect(reuse.payment.order_id).toBe(B)
  })

  it('occupies a second charge key on an already captured payment so another order cannot reuse it', async () => {
    const { pay } = svc([C, D, F])
    expect((await pay.reserve(req())).kind).toBe('reserved')
    expect((await pay.charge({ order_id: B, idempotency_key: 'charge-1' })).kind).toBe('charged')
    const secondKey = await pay.charge({ order_id: B, idempotency_key: 'charge-2' })
    expect(secondKey.kind).toBe('duplicate')
    expect((await pay.reserve(req({ order_id: E, key: 'reserve-2' }))).kind).toBe('reserved')
    const reuse = await pay.charge({ order_id: E, idempotency_key: 'charge-2' })
    expect(reuse.kind).toBe('conflict')
    if (reuse.kind === 'conflict') expect(reuse.payment.order_id).toBe(B)
  })

  it('republishes the original payment_failed envelope after a broker outage', async () => {
    const { store, publisher, pay } = svc([C, D], 100)
    publisher.fail = new Error('down')
    const first = await pay.reserve(req())
    expect(first.kind).toBe('publish_failed')
    if (first.kind !== 'publish_failed') return
    expect(store.getById(C)?.published).toBe(false)
    expect(publisher.events).toHaveLength(0)
    expect(parseEnvelope(first.event).ok).toBe(true)
    expect(first.event.type).toBe('events.payment_failed')
    publisher.fail = null
    const second = await pay.reserve(req())
    expect(second.kind).toBe('replayed')
    if (second.kind !== 'replayed') return
    expect(second.event.message_id).toBe(D)
    expect(second.event).toBe(first.event)
    expect(parseEnvelope(second.event).ok).toBe(true)
    expect(publisher.events).toHaveLength(1)
    expect(publisher.events[0]?.routingKey).toBe(PAYMENT_FAILED_ROUTING_KEY)
    expect(publisher.events[0]?.envelope.message_id).toBe(D)
    expect(store.unpublished()).toHaveLength(0)
  })

  it('charges an unpublished failed payment by republishing the original envelope', async () => {
    const { store, publisher, pay } = svc([C, D], 100)
    publisher.fail = new Error('down')
    const first = await pay.reserve(req())
    expect(first.kind).toBe('publish_failed')
    if (first.kind !== 'publish_failed') return
    expect(store.unpublished()).toHaveLength(1)
    expect(publisher.events).toHaveLength(0)
    publisher.fail = null
    const charged = await pay.charge({ order_id: B, idempotency_key: 'charge-1' })
    expect(charged.kind).toBe('failed')
    expect(charged.kind).not.toBe('publish_failed')
    if (charged.kind !== 'failed') return
    expect(charged.event.message_id).toBe(first.event.message_id)
    expect(parseEnvelope(charged.event).ok).toBe(true)
    expect(charged.event.type).toBe('events.payment_failed')
    expect(publisher.events).toHaveLength(1)
    expect(publisher.events[0]?.routingKey).toBe(PAYMENT_FAILED_ROUTING_KEY)
    expect(publisher.events[0]?.envelope.message_id).toBe(D)
    expect(store.getById(C)?.published).toBe(true)
    expect(store.unpublished()).toHaveLength(0)
  })

  it('leaves the hold reserved when charge envelope construction fails', async () => {
    const { store, pay } = svc()
    expect((await pay.reserve(req())).kind).toBe('reserved')
    const rec = store.getByOrder(B)
    if (!rec) throw new Error('missing reserve')
    rec.payment.correlation_id = 'not-a-uuid'
    const charged = await pay.charge({ order_id: B, idempotency_key: 'charge-1', causation_id: E })
    expect(charged.kind).toBe('rejected')
    expect(store.getByOrder(B)?.payment.status).toBe('reserved')
    expect(store.getByOrder(B)?.payment.captured_cents).toBe(0)
    expect(store.getByOrder(B)?.payment.charge_key).toBeUndefined()
    expect(store.ledger(A)).toEqual({ customer_id: A, available_cents: 7000, held_cents: 3000 })
    expect(store.getByOrder(B)?.event).toBeNull()
  })
})
