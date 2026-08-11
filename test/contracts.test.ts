import { describe, expect, it } from 'vitest'
import {
  CATALOG,
  createEnvelope,
  getCatalogEntry,
  listMessageTypes,
  parseEnvelope,
  payloadSchemas,
  validate,
  type JsonObject,
  type MessageType,
  type ServiceName,
} from '../src/index.js'

const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const C = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'
const placeOrder: JsonObject = {
  customer_id: A,
  items: [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }],
  currency: 'USD',
  idempotency_key: 'client-key-001',
}

const ALL_MESSAGE_TYPES: MessageType[] = [
  'commands.place_order',
  'events.order_created',
  'events.inventory_reserved',
  'events.inventory_reservation_failed',
  'events.payment_charged',
  'events.payment_failed',
  'events.order_completed',
  'events.order_cancelled',
]

describe('contracts', () => {
  it('catalog covers the full message type set with routing metadata', () => {
    expect(listMessageTypes()).toEqual(ALL_MESSAGE_TYPES)
    expect(CATALOG.map((e) => e.type).sort()).toEqual([...ALL_MESSAGE_TYPES].sort())
    expect(CATALOG).toHaveLength(8)
    expect(CATALOG.every((e) => e.routing_key && e.consumers.length > 0)).toBe(true)
    expect(getCatalogEntry('events.missing')).toBeUndefined()
  })

  it('validates payloads, edges, and compensations', () => {
    expect(validate(placeOrder, payloadSchemas.place_order).ok).toBe(true)
    expect(validate({ ...placeOrder, items: [] }, payloadSchemas.place_order).ok).toBe(false)
    expect(
      validate(
        { ...placeOrder, items: [{ sku: 'X', quantity: 0, unit_price_cents: 1 }] },
        payloadSchemas.place_order,
      ).ok,
    ).toBe(false)
    expect(validate({ ...placeOrder, idempotency_key: 'short' }, payloadSchemas.place_order).ok).toBe(false)
    expect(validate({ ...placeOrder, extra: 1 }, payloadSchemas.place_order).ok).toBe(false)
    expect(validate(null, payloadSchemas.place_order).ok).toBe(false)

    const inv = getCatalogEntry('events.inventory_reservation_failed')!
    expect(validate({ order_id: A, reason: 'out_of_stock', failed_skus: ['SKU-1'] }, inv.schema).ok).toBe(true)
    const cancel = getCatalogEntry('events.order_cancelled')!
    expect(
      validate(
        { order_id: A, reason: 'payment_failed', compensations: ['release_inventory', 'notify_customer'] },
        cancel.schema,
      ).ok,
    ).toBe(true)
    expect(
      validate({ order_id: A, reason: 'payment_failed', compensations: ['delete_db'] }, cancel.schema).ok,
    ).toBe(false)
  })

  it('rejects invalid UUIDs on message_id, correlation_id, and customer_id', () => {
    const bad = 'not-a-uuid'
    expect(
      createEnvelope({
        type: 'commands.place_order',
        source: 'gateway',
        payload: placeOrder,
        correlation_id: B,
        message_id: bad,
        occurred_at: '2026-08-11T12:00:00.000Z',
      }).ok,
    ).toBe(false)
    expect(
      createEnvelope({
        type: 'commands.place_order',
        source: 'gateway',
        payload: placeOrder,
        correlation_id: bad,
        message_id: A,
        occurred_at: '2026-08-11T12:00:00.000Z',
      }).ok,
    ).toBe(false)
    expect(
      createEnvelope({
        type: 'commands.place_order',
        source: 'gateway',
        payload: { ...placeOrder, customer_id: bad },
        correlation_id: B,
        message_id: A,
        occurred_at: '2026-08-11T12:00:00.000Z',
      }).ok,
    ).toBe(false)
    expect(validate({ ...placeOrder, customer_id: bad }, payloadSchemas.place_order).ok).toBe(false)
    expect(validate({ ...placeOrder, customer_id: '550e8400-e29b-41d4-a716-44665544000g' }, payloadSchemas.place_order).ok).toBe(false)
  })

  it('accepts RFC3339 occurred_at and rejects non-ISO forms', () => {
    const base = {
      type: 'commands.place_order' as const,
      source: 'gateway' as const,
      payload: placeOrder,
      correlation_id: B,
      message_id: A,
    }
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11T12:00:00.000Z' }).ok).toBe(true)
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11T12:00:00Z' }).ok).toBe(true)
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11T12:00:00+00:00' }).ok).toBe(true)
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11T05:00:00-07:00' }).ok).toBe(true)

    expect(createEnvelope({ ...base, occurred_at: 'August 11, 2026' }).ok).toBe(false)
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11' }).ok).toBe(false)
    expect(createEnvelope({ ...base, occurred_at: '2026/08/11 12:00:00' }).ok).toBe(false)
    expect(createEnvelope({ ...base, occurred_at: '2026-08-11 12:00:00' }).ok).toBe(false)
    expect(createEnvelope({ ...base, occurred_at: '11-08-2026T12:00:00Z' }).ok).toBe(false)

    const good = createEnvelope({ ...base, occurred_at: '2026-08-11T12:00:00.000Z' })
    expect(good.ok).toBe(true)
    if (!good.ok || !good.envelope) return
    expect(parseEnvelope(good.envelope).ok).toBe(true)
    expect(parseEnvelope({ ...good.envelope, occurred_at: 'August 11, 2026' }).ok).toBe(false)
    expect(parseEnvelope({ ...good.envelope, occurred_at: '2026-08-11' }).ok).toBe(false)
  })

  it('envelope create/parse enforces schema_version and known types', () => {
    const created = createEnvelope({
      type: 'commands.place_order',
      source: 'gateway',
      payload: placeOrder,
      correlation_id: B,
      message_id: A,
      causation_id: C,
      occurred_at: '2026-08-11T12:00:00.000Z',
    })
    expect(created.ok).toBe(true)
    if (!created.ok || !created.envelope) return
    expect(created.envelope.schema_version).toBe('1.0.0')
    expect(parseEnvelope(created.envelope).ok).toBe(true)
    expect(
      createEnvelope({
        type: 'commands.place_order',
        source: 'gateway',
        payload: { ...placeOrder, items: [] },
        correlation_id: B,
        message_id: A,
      }).ok,
    ).toBe(false)
    expect(parseEnvelope({ ...created.envelope, schema_version: '9.0.0' }).ok).toBe(false)
    expect(parseEnvelope({ ...created.envelope, type: 'events.nope' }).ok).toBe(false)
    expect(
      parseEnvelope({
        message_id: A,
        type: 'commands.place_order',
        schema_version: '1.0.0',
        source: 'gateway',
        payload: placeOrder,
      }).ok,
    ).toBe(false)
  })

  it('create/parse failure-path events including order_cancelled compensations', () => {
    const paymentFailed = createEnvelope({
      type: 'events.payment_failed',
      source: 'payments',
      payload: { order_id: A, reason: 'card_declined', retryable: false },
      correlation_id: B,
      message_id: A,
      causation_id: C,
      occurred_at: '2026-08-11T12:00:00.000Z',
    })
    expect(paymentFailed.ok).toBe(true)
    if (!paymentFailed.ok || !paymentFailed.envelope) return
    const parsedPayment = parseEnvelope(paymentFailed.envelope)
    expect(parsedPayment.ok).toBe(true)
    if (!parsedPayment.ok) return
    expect(parsedPayment.envelope.type).toBe('events.payment_failed')
    expect(parsedPayment.envelope.payload).toEqual({
      order_id: A,
      reason: 'card_declined',
      retryable: false,
    })

    const invFailed = createEnvelope({
      type: 'events.inventory_reservation_failed',
      source: 'inventory',
      payload: { order_id: A, reason: 'out_of_stock', failed_skus: ['SKU-1'] },
      correlation_id: B,
      message_id: C,
      occurred_at: '2026-08-11T12:00:01.000Z',
    })
    expect(invFailed.ok).toBe(true)
    if (!invFailed.ok || !invFailed.envelope) return
    expect(parseEnvelope(invFailed.envelope).ok).toBe(true)

    const cancelled = createEnvelope({
      type: 'events.order_cancelled',
      source: 'orders',
      payload: {
        order_id: A,
        reason: 'payment_failed',
        compensations: ['release_inventory', 'notify_customer'],
      },
      correlation_id: B,
      message_id: '6ba7b812-9dad-11d1-80b4-00c04fd430c8',
      causation_id: paymentFailed.envelope.message_id,
      occurred_at: '2026-08-11T12:00:02.000Z',
    })
    expect(cancelled.ok).toBe(true)
    if (!cancelled.ok || !cancelled.envelope) return
    const parsedCancel = parseEnvelope(cancelled.envelope)
    expect(parsedCancel.ok).toBe(true)
    if (!parsedCancel.ok) return
    expect(parsedCancel.envelope.type).toBe('events.order_cancelled')
    expect(parsedCancel.envelope.payload['compensations']).toEqual([
      'release_inventory',
      'notify_customer',
    ])
  })

  it('round-trips the happy-path event sequence', () => {
    const stepIds = [
      '550e8400-e29b-41d4-a716-446655440001',
      '550e8400-e29b-41d4-a716-446655440002',
      '550e8400-e29b-41d4-a716-446655440003',
      '550e8400-e29b-41d4-a716-446655440004',
      '550e8400-e29b-41d4-a716-446655440005',
    ]
    const steps: Array<{ type: MessageType; source: ServiceName; payload: JsonObject }> = [
      { type: 'commands.place_order', source: 'gateway', payload: placeOrder },
      {
        type: 'events.order_created',
        source: 'orders',
        payload: {
          order_id: A, customer_id: A, items: placeOrder['items'] as JsonObject[],
          currency: 'USD', total_cents: 3000,
        },
      },
      {
        type: 'events.inventory_reserved',
        source: 'inventory',
        payload: {
          order_id: A, reservation_id: B,
          items: [{ sku: 'SKU-1', quantity: 2, warehouse_id: 'wh-east' }],
        },
      },
      {
        type: 'events.payment_charged',
        source: 'payments',
        payload: { order_id: A, payment_id: C, amount_cents: 3000, currency: 'USD' },
      },
      {
        type: 'events.order_completed',
        source: 'orders',
        payload: { order_id: A, payment_id: C, reservation_id: B, total_cents: 3000 },
      },
    ]
    let causation: string | undefined
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i]!
      const created = createEnvelope({
        ...step,
        correlation_id: A,
        message_id: stepIds[i]!,
        causation_id: causation,
        occurred_at: '2026-08-11T12:00:00.000Z',
      })
      expect(created.ok, step.type).toBe(true)
      if (!created.ok || !created.envelope) return
      expect(parseEnvelope(created.envelope).ok, step.type).toBe(true)
      if (i > 0) expect(created.envelope.causation_id).toBe(stepIds[i - 1])
      causation = created.envelope.message_id
    }
  })
})
