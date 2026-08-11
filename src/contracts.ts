export type JsonValue = string | number | boolean | null | JsonObject | JsonValue[]
export type JsonObject = { [key: string]: JsonValue }

export type JsonSchema = {
  type?: 'object' | 'array' | 'string' | 'integer' | 'boolean'
  properties?: Record<string, JsonSchema>
  required?: string[]
  additionalProperties?: boolean
  items?: JsonSchema
  minItems?: number
  minLength?: number
  minimum?: number
  exclusiveMinimum?: number
  enum?: JsonValue[]
  format?: 'uuid' | 'date-time'
  pattern?: string
}

export type ValidationError = { path: string; message: string }
export type ValidationResult =
  | { ok: true; value: JsonValue }
  | { ok: false; errors: ValidationError[] }

export type MessageKind = 'command' | 'event'
export type ServiceName = 'gateway' | 'orders' | 'inventory' | 'payments' | 'notifications'
export type MessageType =
  | 'commands.place_order'
  | 'events.order_created'
  | 'events.inventory_reserved'
  | 'events.inventory_reservation_failed'
  | 'events.payment_charged'
  | 'events.payment_failed'
  | 'events.order_completed'
  | 'events.order_cancelled'

export type CatalogEntry = {
  type: MessageType
  kind: MessageKind
  schema_version: string
  schema: JsonSchema
  routing_key: string
  producer: ServiceName
  consumers: ServiceName[]
}

export type MessageEnvelope = {
  message_id: string
  correlation_id: string
  causation_id?: string
  type: MessageType
  schema_version: string
  occurred_at: string
  source: ServiceName
  payload: JsonObject
}

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

// RFC 3339 date-time: full date + time + Z or numeric offset (not date-only / locale).
const DATE_TIME_RE =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})$/

function isRfc3339DateTime(value: string): boolean {
  const m = DATE_TIME_RE.exec(value)
  if (!m) return false
  const year = Number(m[1])
  const month = Number(m[2])
  const day = Number(m[3])
  const hour = Number(m[4])
  const minute = Number(m[5])
  const second = Number(m[6])
  if (month < 1 || month > 12) return false
  if (hour > 23 || minute > 59 || second > 60) return false
  const dim = new Date(Date.UTC(year, month, 0)).getUTCDate()
  if (day < 1 || day > dim) return false
  const offset = m[8]!
  if (offset !== 'Z') {
    const oh = Number(offset.slice(1, 3))
    const om = Number(offset.slice(4, 6))
    if (oh > 23 || om > 59) return false
  }
  return true
}

const u = (): JsonSchema => ({ type: 'string', format: 'uuid' })
const pos = (): JsonSchema => ({ type: 'integer', exclusiveMinimum: 0 })
const nn = (): JsonSchema => ({ type: 'integer', minimum: 0 })
const money = (): JsonSchema => ({ type: 'string', enum: ['USD', 'EUR', 'GBP'] })
const o = (required: string[], properties: Record<string, JsonSchema>): JsonSchema => ({
  type: 'object',
  additionalProperties: false,
  required,
  properties,
})
const a = (items: JsonSchema): JsonSchema => ({ type: 'array', minItems: 1, items })
const line = o(['sku', 'quantity', 'unit_price_cents'], {
  sku: { type: 'string', minLength: 1 },
  quantity: pos(),
  unit_price_cents: nn(),
})

export const payloadSchemas = {
  place_order: o(['customer_id', 'items', 'currency', 'idempotency_key'], {
    customer_id: u(),
    items: a(line),
    currency: money(),
    idempotency_key: { type: 'string', minLength: 8 },
  }),
  order_created: o(['order_id', 'customer_id', 'items', 'currency', 'total_cents'], {
    order_id: u(),
    customer_id: u(),
    items: a(line),
    currency: money(),
    total_cents: nn(),
  }),
  inventory_reserved: o(['order_id', 'reservation_id', 'items'], {
    order_id: u(),
    reservation_id: u(),
    items: a(
      o(['sku', 'quantity', 'warehouse_id'], {
        sku: { type: 'string', minLength: 1 },
        quantity: pos(),
        warehouse_id: { type: 'string', minLength: 1 },
      }),
    ),
  }),
  inventory_reservation_failed: o(['order_id', 'reason', 'failed_skus'], {
    order_id: u(),
    reason: { type: 'string', enum: ['out_of_stock', 'sku_unknown', 'warehouse_unavailable'] },
    failed_skus: a({ type: 'string', minLength: 1 }),
  }),
  payment_charged: o(['order_id', 'payment_id', 'amount_cents', 'currency'], {
    order_id: u(),
    payment_id: u(),
    amount_cents: pos(),
    currency: money(),
  }),
  payment_failed: o(['order_id', 'reason', 'retryable'], {
    order_id: u(),
    reason: { type: 'string', enum: ['insufficient_funds', 'card_declined', 'processor_error'] },
    retryable: { type: 'boolean' },
  }),
  order_completed: o(['order_id', 'payment_id', 'reservation_id', 'total_cents'], {
    order_id: u(),
    payment_id: u(),
    reservation_id: u(),
    total_cents: nn(),
  }),
  order_cancelled: o(['order_id', 'reason', 'compensations'], {
    order_id: u(),
    reason: { type: 'string', enum: ['inventory_failed', 'payment_failed', 'customer_cancelled'] },
    compensations: {
      type: 'array',
      items: { type: 'string', enum: ['release_inventory', 'refund_payment', 'notify_customer'] },
    },
  }),
} as const satisfies Record<string, JsonSchema>

type SchemaKey = keyof typeof payloadSchemas

const envelopeSchema = o(
  ['message_id', 'correlation_id', 'type', 'schema_version', 'occurred_at', 'source', 'payload'],
  {
    message_id: u(),
    correlation_id: u(),
    causation_id: u(),
    type: { type: 'string', minLength: 1 },
    schema_version: { type: 'string', pattern: '^[0-9]+\\.[0-9]+\\.[0-9]+$' },
    occurred_at: { type: 'string', format: 'date-time' },
    source: {
      type: 'string',
      enum: ['gateway', 'orders', 'inventory', 'payments', 'notifications'],
    },
    // Catalog validates the body; the shell only requires an object.
    payload: { type: 'object', additionalProperties: true },
  },
)

const V1 = '1.0.0'
const rows: Array<[MessageType, MessageKind, SchemaKey, string, ServiceName, ServiceName[]]> = [
  ['commands.place_order', 'command', 'place_order', 'http.POST /v1/orders', 'gateway', ['orders']],
  ['events.order_created', 'event', 'order_created', 'events.orders.created', 'orders', [
    'inventory', 'payments', 'notifications',
  ]],
  ['events.inventory_reserved', 'event', 'inventory_reserved', 'events.inventory.reserved', 'inventory', ['orders']],
  ['events.inventory_reservation_failed', 'event', 'inventory_reservation_failed', 'events.inventory.reservation_failed', 'inventory', ['orders']],
  ['events.payment_charged', 'event', 'payment_charged', 'events.payments.charged', 'payments', ['orders']],
  ['events.payment_failed', 'event', 'payment_failed', 'events.payments.failed', 'payments', ['orders']],
  ['events.order_completed', 'event', 'order_completed', 'events.orders.completed', 'orders', ['notifications']],
  ['events.order_cancelled', 'event', 'order_cancelled', 'events.orders.cancelled', 'orders', [
    'inventory', 'payments', 'notifications',
  ]],
]

export const CATALOG: readonly CatalogEntry[] = rows.map(
  ([type, kind, schemaKey, routing_key, producer, consumers]) => ({
    type, kind, schema_version: V1, schema: payloadSchemas[schemaKey], routing_key, producer, consumers,
  }),
)

const byType = new Map(CATALOG.map((e) => [e.type, e]))
export const getCatalogEntry = (type: string) => byType.get(type as MessageType)
export const listMessageTypes = () => CATALOG.map((e) => e.type)

function join(base: string, key: string): string {
  return !base ? key : key.startsWith('[') ? `${base}${key}` : `${base}.${key}`
}

function check(value: unknown, schema: JsonSchema, path: string, errors: ValidationError[]): void {
  if (schema.enum) {
    if (!schema.enum.some((c) => JSON.stringify(c) === JSON.stringify(value))) {
      errors.push({ path, message: `enum ${JSON.stringify(schema.enum)}` })
    }
    return
  }
  if (!schema.type) return
  if (schema.type === 'boolean') {
    if (typeof value !== 'boolean') errors.push({ path, message: 'boolean' })
    return
  }
  if (schema.type === 'string') {
    if (typeof value !== 'string') {
      errors.push({ path, message: 'string' })
      return
    }
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push({ path, message: `minLength ${schema.minLength}` })
    }
    if (schema.pattern && !new RegExp(schema.pattern).test(value)) {
      errors.push({ path, message: 'pattern' })
    }
    if (schema.format === 'uuid' && !UUID_RE.test(value)) errors.push({ path, message: 'uuid' })
    if (schema.format === 'date-time' && !isRfc3339DateTime(value)) {
      errors.push({ path, message: 'date-time' })
    }
    return
  }
  if (schema.type === 'integer') {
    if (typeof value !== 'number' || !Number.isInteger(value)) {
      errors.push({ path, message: 'integer' })
      return
    }
    if (schema.minimum !== undefined && value < schema.minimum) {
      errors.push({ path, message: `>= ${schema.minimum}` })
    }
    if (schema.exclusiveMinimum !== undefined && value <= schema.exclusiveMinimum) {
      errors.push({ path, message: `> ${schema.exclusiveMinimum}` })
    }
    return
  }
  if (schema.type === 'array') {
    if (!Array.isArray(value)) {
      errors.push({ path, message: 'array' })
      return
    }
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      errors.push({ path, message: `minItems ${schema.minItems}` })
    }
    if (schema.items) {
      value.forEach((item, i) => check(item, schema.items!, join(path, `[${i}]`), errors))
    }
    return
  }
  if (schema.type === 'object') {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      errors.push({ path, message: 'object' })
      return
    }
    const record = value as JsonObject
    for (const key of schema.required ?? []) {
      if (!(key in record)) errors.push({ path: join(path, key), message: 'required' })
    }
    const props = schema.properties ?? {}
    for (const [key, child] of Object.entries(record)) {
      if (props[key]) check(child, props[key], join(path, key), errors)
      else if (schema.additionalProperties === false) {
        errors.push({ path: join(path, key), message: 'additional' })
      }
    }
  }
}

export function validate(value: unknown, schema: JsonSchema): ValidationResult {
  const errors: ValidationError[] = []
  check(value, schema, '', errors)
  if (errors.length) {
    return { ok: false, errors: errors.map((e) => ({ path: e.path || '$', message: e.message })) }
  }
  return { ok: true, value: value as JsonValue }
}

const payloadErrs = (errors: ValidationError[]) =>
  errors.map((e) => ({
    path: e.path === '$' ? 'payload' : `payload.${e.path}`,
    message: e.message,
  }))

export function createEnvelope(input: {
  type: MessageType
  source: ServiceName
  payload: JsonObject
  correlation_id: string
  message_id: string
  causation_id?: string
  occurred_at?: string
}): ValidationResult & { envelope?: MessageEnvelope } {
  const entry = getCatalogEntry(input.type)
  if (!entry) return { ok: false, errors: [{ path: 'type', message: 'unknown' }] }
  const payload = validate(input.payload, entry.schema)
  if (!payload.ok) return { ok: false, errors: payloadErrs(payload.errors) }
  const envelope: MessageEnvelope = {
    message_id: input.message_id,
    correlation_id: input.correlation_id,
    type: input.type,
    schema_version: entry.schema_version,
    occurred_at: input.occurred_at ?? new Date().toISOString(),
    source: input.source,
    payload: payload.value as JsonObject,
  }
  if (input.causation_id) envelope.causation_id = input.causation_id
  const shell = validate(envelope as unknown as JsonValue, envelopeSchema)
  if (!shell.ok) return shell
  return { ok: true, value: envelope as unknown as JsonValue, envelope }
}

export function parseEnvelope(
  raw: unknown,
): { ok: true; envelope: MessageEnvelope } | { ok: false; errors: ValidationError[] } {
  const shell = validate(raw, envelopeSchema)
  if (!shell.ok) return shell
  const obj = shell.value as JsonObject
  const type = String(obj['type'] ?? '')
  const entry = getCatalogEntry(type)
  if (!entry) return { ok: false, errors: [{ path: 'type', message: `unknown ${type}` }] }
  if (obj['schema_version'] !== entry.schema_version) {
    return {
      ok: false,
      errors: [{
        path: 'schema_version',
        message: `expected ${entry.schema_version}, got ${String(obj['schema_version'])}`,
      }],
    }
  }
  const payload = validate(obj['payload'], entry.schema)
  if (!payload.ok) return { ok: false, errors: payloadErrs(payload.errors) }
  const envelope: MessageEnvelope = {
    message_id: obj['message_id'] as string,
    correlation_id: obj['correlation_id'] as string,
    type: entry.type,
    schema_version: entry.schema_version,
    occurred_at: obj['occurred_at'] as string,
    source: obj['source'] as ServiceName,
    payload: payload.value as JsonObject,
  }
  if (typeof obj['causation_id'] === 'string') envelope.causation_id = obj['causation_id']
  return { ok: true, envelope }
}
