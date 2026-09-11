export type SpanContext = {
  traceId: string
  spanId: string
  parentSpanId?: string
  sampled: boolean
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'
export type Check = { name: string; ok: boolean }
export type HealthReport = {
  status: 'ok' | 'degraded'
  live: true
  ready: boolean
  service: string
  checks: Check[]
}

const TRACE_RE =
  /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i
const ZERO_TRACE = '0'.repeat(32)
const ZERO_SPAN = '0'.repeat(16)

export function parseTraceparent(header: string): SpanContext | null {
  const m = TRACE_RE.exec(header.trim())
  if (!m) return null
  const version = m[1]!.toLowerCase()
  const traceId = m[2]!.toLowerCase()
  const spanId = m[3]!.toLowerCase()
  const flags = m[4]!.toLowerCase()
  if (version === 'ff' || traceId === ZERO_TRACE || spanId === ZERO_SPAN) return null
  return { traceId, spanId, sampled: (Number.parseInt(flags, 16) & 1) === 1 }
}

export function formatTraceparent(ctx: SpanContext): string {
  return `00-${ctx.traceId}-${ctx.spanId}-${ctx.sampled ? '01' : '00'}`
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

function randomHex(size: number, zero: string): string {
  const bytes = new Uint8Array(size)
  crypto.getRandomValues(bytes)
  const out = hex(bytes)
  return out === zero ? `${out.slice(0, -1)}1` : out
}

export function startSpan(parent: SpanContext | null): SpanContext {
  const spanId = randomHex(8, ZERO_SPAN)
  if (!parent) return { traceId: randomHex(16, ZERO_TRACE), spanId, sampled: true }
  return { traceId: parent.traceId, spanId, parentSpanId: parent.spanId, sampled: parent.sampled }
}

export function continueFrom(header: string | undefined): SpanContext {
  return startSpan(header ? parseTraceparent(header) : null)
}

export function extractTraceparent(headers: Record<string, string | undefined>): SpanContext | null {
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === 'traceparent' && value) return parseTraceparent(value)
  }
  return null
}

export function formatLog(input: {
  ts?: string
  level: LogLevel
  service: string
  msg: string
  span?: SpanContext
  correlation_id?: string
  fields?: Record<string, string>
}): string {
  const rec: Record<string, string> = {
    ts: input.ts ?? new Date().toISOString(),
    level: input.level,
    service: input.service,
    msg: input.msg,
  }
  if (input.span) {
    rec.trace_id = input.span.traceId
    rec.span_id = input.span.spanId
    if (input.span.parentSpanId) rec.parent_span_id = input.span.parentSpanId
  }
  if (input.correlation_id) rec.correlation_id = input.correlation_id
  if (input.fields) Object.assign(rec, input.fields)
  return JSON.stringify(rec)
}

export function healthReport(service: string, checks: readonly Check[]): HealthReport {
  const ready = checks.every((c) => c.ok)
  return { status: ready ? 'ok' : 'degraded', live: true, ready, service, checks: [...checks] }
}

export function handleHealthRequest(
  method: string,
  url: string,
  service: string,
  checks: readonly Check[],
): { status: number; body: string } {
  const path = (url.split('?')[0] ?? '/') as string
  if (method !== 'GET' || (path !== '/health' && path !== '/ready')) {
    return { status: 404, body: JSON.stringify({ error: 'not_found' }) }
  }
  const report = healthReport(service, checks)
  const status = path === '/health' || report.ready ? 200 : 503
  return { status, body: JSON.stringify(report) }
}
