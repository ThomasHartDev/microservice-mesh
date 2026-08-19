import { describe, expect, it } from 'vitest'
import {
  ClosedError,
  MAX_DELIVER,
  SubjectError,
  createMemoryBroker,
  matchSubject,
  publishEnvelope,
  validSubject,
} from '../src/broker.js'
import { CATALOG, createEnvelope } from '../src/index.js'

const enc = new TextEncoder()
const dec = new TextDecoder()
const payload = {
  customer_id: '550e8400-e29b-41d4-a716-446655440000',
  items: [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }],
  currency: 'USD',
  idempotency_key: 'client-key-001',
}

describe('broker', () => {
  it('enforces NATS subjects, queue groups, nack redelivery, and envelope routing', async () => {
    expect(validSubject('commands.place_order', false)).toBe(true)
    expect(validSubject('>', false)).toBe(false)
    expect(validSubject('http.POST /v1/orders', false)).toBe(false)
    expect(matchSubject('orders.created', 'orders.*')).toBe(true)
    expect(matchSubject('foo.bar.baz', 'foo.>')).toBe(true)
    expect(matchSubject('foo', 'foo.>')).toBe(false)
    expect(matchSubject('a.b.c', 'a.*')).toBe(false)
    for (const e of CATALOG) expect(validSubject(e.routing_key, false), e.routing_key).toBe(true)

    const bus = createMemoryBroker()
    const fan: string[] = []
    const q = [0, 0]
    await bus.subscribe('jobs', (d) => {
      fan.push(dec.decode(d.data))
      d.ack()
    })
    await bus.subscribe('jobs', (d) => { q[0]! += 1; d.ack() }, { queue: 'workers' })
    await bus.subscribe('jobs', (d) => { q[1]! += 1; d.ack() }, { queue: 'workers' })
    for (const n of ['1', '2', '3', '4']) await bus.publish('jobs', enc.encode(n))
    expect(fan).toEqual(['1', '2', '3', '4'])
    expect(q).toEqual([2, 2])

    let n = 0
    await bus.subscribe('a.b', (d) => { n += 1; n < 3 ? d.nack() : d.ack() })
    await bus.publish('a.b', enc.encode('x'))
    expect(n).toBe(3)
    let poison = 0
    await bus.subscribe('poison', () => { poison += 1; throw new Error('boom') })
    await bus.publish('poison', enc.encode('p'))
    expect(poison).toBe(MAX_DELIVER)

    await expect(bus.publish('>', enc.encode('x'))).rejects.toBeInstanceOf(SubjectError)
    let hits = 0
    const sub = await bus.subscribe('events.>', (d) => { hits += 1; d.ack() })
    await bus.publish('events.orders.created', enc.encode('ok'))
    sub.unsubscribe()
    await bus.publish('events.orders.created', enc.encode('late'))
    expect(hits).toBe(1)

    const created = createEnvelope({
      type: 'commands.place_order', source: 'gateway', payload,
      correlation_id: '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
      message_id: '550e8400-e29b-41d4-a716-446655440000',
      occurred_at: '2026-08-11T12:00:00.000Z',
    })
    expect(created.ok && created.envelope).toBeTruthy()
    const got: string[] = []
    await bus.subscribe('commands.place_order', (d) => { got.push(dec.decode(d.data)); d.ack() })
    await publishEnvelope(bus, created.envelope!)
    expect(JSON.parse(got[0]!)).toMatchObject({ type: 'commands.place_order' })
    await bus.close()
    await expect(bus.publish('x', enc.encode('x'))).rejects.toBeInstanceOf(ClosedError)
  })
})
