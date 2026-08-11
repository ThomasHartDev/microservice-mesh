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

describe('contracts', () => {
  it('catalog covers mesh messages with routing metadata', () => {
    expect(listMessageTypes()).toEqual(
      expect.arrayContaining([
        'commands.place_order',
        'events.order_created',
        'events.payment_charged',
        'events.order_cancelled',
      ]),
    )
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

  it('round-trips the happy-path event sequence', () => {
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
    for (const step of steps) {
      const created = createEnvelope({
        ...step, correlation_id: A, message_id: B, causation_id: causation,
        occurred_at: '2026-08-11T12:00:00.000Z',
      })
      expect(created.ok, step.type).toBe(true)
      if (!created.ok || !created.envelope) return
      expect(parseEnvelope(created.envelope).ok, step.type).toBe(true)
      causation = created.envelope.message_id
    }
  })
})
