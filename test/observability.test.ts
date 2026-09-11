import { describe, expect, it } from 'vitest'
import {
  continueFrom,
  createEnvelope,
  extractTraceparent,
  formatLog,
  formatTraceparent,
  handleHealthRequest,
  healthReport,
  parseEnvelope,
  parseTraceparent,
  startSpan,
  type JsonObject,
} from '../src/index.js'

const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
const A = '550e8400-e29b-41d4-a716-446655440000'
const B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
const place: JsonObject = {
  customer_id: A,
  items: [{ sku: 'SKU-1', quantity: 2, unit_price_cents: 1500 }],
  currency: 'USD',
  idempotency_key: 'client-key-001',
}

describe('observability', () => {
  it('parses W3C traceparent, continues a child span, and rejects zeros', () => {
    const ctx = parseTraceparent(` ${TP.toUpperCase()} `)
    expect(ctx).toEqual({
      traceId: '4bf92f3577b34da6a3ce929d0e0e4736', spanId: '00f067aa0ba902b7', sampled: true,
    })
    expect(formatTraceparent(ctx!)).toBe(TP)
    expect(parseTraceparent(TP.replace(/01$/, '00'))?.sampled).toBe(false)
    expect(parseTraceparent('00-00000000000000000000000000000000-00f067aa0ba902b7-01')).toBeNull()
    expect(parseTraceparent('00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01')).toBeNull()
    expect(parseTraceparent('ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01')).toBeNull()
    expect(parseTraceparent(`${TP}-extra`)).toBeNull()
    expect(extractTraceparent({ Traceparent: TP })).toEqual(ctx)
    const root = startSpan(null)
    const child = startSpan(root)
    expect(child.traceId).toBe(root.traceId)
    expect(child.spanId).not.toBe(root.spanId)
    expect(child.parentSpanId).toBe(root.spanId)
    const hop = continueFrom(TP)
    expect(hop.traceId).toBe(ctx!.traceId)
    expect(hop.parentSpanId).toBe(ctx!.spanId)
    expect(continueFrom('nope').traceId).not.toBe(ctx!.traceId)
    expect(startSpan({ ...root, sampled: false }).sampled).toBe(false)
  })

  it('logs trace fields and splits liveness from readiness', () => {
    const span = continueFrom(TP)
    expect(JSON.parse(formatLog({
      ts: '2026-09-11T00:00:00.000Z', level: 'info', service: 'orders', msg: 'order_created',
      span, correlation_id: A, fields: { message_id: B },
    }))).toMatchObject({
      trace_id: span.traceId, span_id: span.spanId, parent_span_id: span.parentSpanId,
      correlation_id: A, message_id: B, msg: 'order_created',
    })
    const down = [{ name: 'process', ok: true }, { name: 'nats', ok: false }]
    expect(healthReport('orders', down)).toMatchObject({ status: 'degraded', live: true, ready: false })
    expect(handleHealthRequest('GET', '/health?x=1', 'orders', down).status).toBe(200)
    expect(handleHealthRequest('GET', '/ready', 'orders', down).status).toBe(503)
    expect(handleHealthRequest('GET', '/ready', 'orders', [{ name: 'process', ok: true }]).status).toBe(200)
    expect(handleHealthRequest('POST', '/health', 'orders', down).status).toBe(404)
  })

  it('stores canonical traceparent on the envelope', () => {
    const built = createEnvelope({
      type: 'commands.place_order', source: 'gateway', payload: place,
      correlation_id: B, message_id: A, traceparent: TP.toUpperCase(),
      occurred_at: '2026-09-11T00:00:00.000Z',
    })
    expect(built.envelope?.traceparent).toBe(TP)
    expect(parseEnvelope(built.envelope).ok).toBe(true)
    expect(createEnvelope({
      type: 'commands.place_order', source: 'gateway', payload: place,
      correlation_id: B, message_id: A,
      traceparent: '00-00000000000000000000000000000000-00f067aa0ba902b7-01',
    }).ok).toBe(false)
  })
})
