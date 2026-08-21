import { createEnvelope, type MessageEnvelope, type ValidationError } from './contracts.js'

export type Currency = 'USD' | 'EUR' | 'GBP'
export type FailureReason = 'insufficient_funds' | 'card_declined' | 'processor_error'
export type PaymentStatus = 'reserved' | 'charged' | 'failed'

export type Payment = {
  payment_id: string
  order_id: string
  customer_id: string
  amount_cents: number
  currency: Currency
  status: PaymentStatus
  reserved_cents: number
  captured_cents: number
  reserve_key: string
  charge_key?: string
  reason?: FailureReason
  retryable?: boolean
  created_at: string
  correlation_id: string
}

export type Ledger = { customer_id: string; available_cents: number; held_cents: number }
export type Outcome =
  | { kind: 'reserved'; payment: Payment }
  | { kind: 'charged'; payment: Payment; event: MessageEnvelope }
  | { kind: 'failed'; payment: Payment; event: MessageEnvelope }
  | { kind: 'replayed'; payment: Payment; event: MessageEnvelope }
  | { kind: 'duplicate'; payment: Payment; event?: MessageEnvelope }
  | { kind: 'conflict'; payment: Payment }
  | { kind: 'rejected'; errors: ValidationError[] }
  | { kind: 'publish_failed'; payment: Payment; event: MessageEnvelope; error: Error }

export type ReserveRequest = {
  order_id: string
  customer_id: string
  amount_cents: number
  currency: Currency
  idempotency_key: string
  correlation_id: string
  causation_id?: string
}

export type ChargeRequest = { order_id: string; idempotency_key: string; causation_id?: string }
export type Publisher = { publish(routingKey: string, envelope: MessageEnvelope): Promise<void> }
export type Clock = { now(): Date; newId(): string }
export type PaymentRecord = {
  payment: Payment
  reserve_hash: string
  event: MessageEnvelope | null
  published: boolean
}

export const PAYMENT_CHARGED_ROUTING_KEY = 'events.payments.charged'
export const PAYMENT_FAILED_ROUTING_KEY = 'events.payments.failed'

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

export class MemoryPublisher implements Publisher {
  events: Array<{ routingKey: string; envelope: MessageEnvelope }> = []
  fail: Error | null = null
  async publish(routingKey: string, envelope: MessageEnvelope): Promise<void> {
    if (this.fail) throw this.fail
    this.events.push({ routingKey, envelope })
  }
}

export function reserveFingerprint(r: {
  order_id: string
  customer_id: string
  amount_cents: number
  currency: string
}): string {
  return JSON.stringify({
    order_id: r.order_id, customer_id: r.customer_id, amount_cents: r.amount_cents, currency: r.currency,
  })
}

function rejected(path: string, message: string): Outcome {
  return { kind: 'rejected', errors: [{ path, message }] }
}

export class MemoryPaymentStore {
  private readonly byReserve = new Map<string, PaymentRecord>()
  private readonly byCharge = new Map<string, PaymentRecord>()
  private readonly byOrder = new Map<string, PaymentRecord>()
  private readonly byId = new Map<string, PaymentRecord>()
  private readonly ledgers = new Map<string, Ledger>()
  private tail: Promise<void> = Promise.resolve()

  serialize<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.tail.then(fn, fn)
    this.tail = run.then(() => undefined, () => undefined)
    return run
  }

  getByReserveKey(key: string) { return this.byReserve.get(key) }
  getByChargeKey(key: string) { return this.byCharge.get(key) }
  getByOrder(id: string) { return this.byOrder.get(id) }
  getById(id: string) { return this.byId.get(id) }
  ledger(id: string): Ledger {
    return this.ledgers.get(id) ?? { customer_id: id, available_cents: 0, held_cents: 0 }
  }

  credit(id: string, cents: number): boolean {
    if (!Number.isSafeInteger(cents) || cents < 0) return false
    const cur = this.ledger(id)
    const next = cur.available_cents + cents
    if (!Number.isSafeInteger(next)) return false
    this.ledgers.set(id, { ...cur, available_cents: next })
    return true
  }

  authorize(id: string, cents: number): boolean {
    const cur = this.ledger(id)
    if (cur.available_cents < cents) return false
    const available = cur.available_cents - cents
    const held = cur.held_cents + cents
    if (!Number.isSafeInteger(available) || !Number.isSafeInteger(held)) return false
    this.ledgers.set(id, { customer_id: id, available_cents: available, held_cents: held })
    return true
  }

  capture(id: string, cents: number): boolean {
    const cur = this.ledger(id)
    if (cur.held_cents < cents) return false
    const held = cur.held_cents - cents
    if (!Number.isSafeInteger(held)) return false
    this.ledgers.set(id, { ...cur, held_cents: held })
    return true
  }

  put(rec: PaymentRecord): void {
    this.byReserve.set(rec.payment.reserve_key, rec)
    this.byOrder.set(rec.payment.order_id, rec)
    this.byId.set(rec.payment.payment_id, rec)
    if (rec.payment.charge_key) this.byCharge.set(rec.payment.charge_key, rec)
  }

  bindCharge(key: string, rec: PaymentRecord): void {
    this.byCharge.set(key, rec)
    if (!rec.payment.charge_key) rec.payment.charge_key = key
  }

  unpublished() { return [...this.byId.values()].filter((r) => r.event && !r.published) }
  all() { return [...this.byId.values()].map((r) => r.payment) }
}

export class PaymentsService {
  failNext: 'authorize' | 'capture' | null = null
  blocked = new Set<string>()

  constructor(
    readonly store: MemoryPaymentStore,
    readonly publisher: Publisher,
    readonly clock: Clock,
  ) {}

  reserve(req: ReserveRequest) {
    return this.store.serialize(() => this.reserveLocked(req))
  }
  charge(req: ChargeRequest) {
    return this.store.serialize(() => this.chargeLocked(req))
  }

  private async reserveLocked(req: ReserveRequest): Promise<Outcome> {
    const errors: ValidationError[] = []
    if (!UUID_RE.test(req.order_id)) errors.push({ path: 'order_id', message: 'uuid' })
    if (!UUID_RE.test(req.customer_id)) errors.push({ path: 'customer_id', message: 'uuid' })
    if (!UUID_RE.test(req.correlation_id)) errors.push({ path: 'correlation_id', message: 'uuid' })
    if (!Number.isSafeInteger(req.amount_cents) || req.amount_cents <= 0) {
      errors.push({ path: 'amount_cents', message: '> 0' })
    }
    if (req.currency !== 'USD' && req.currency !== 'EUR' && req.currency !== 'GBP') {
      errors.push({ path: 'currency', message: 'enum' })
    }
    if (req.idempotency_key.length < 8) errors.push({ path: 'idempotency_key', message: 'minLength 8' })
    if (errors.length) return { kind: 'rejected', errors }

    const hash = reserveFingerprint(req)
    const byKey = this.store.getByReserveKey(req.idempotency_key)
    if (byKey) {
      if (byKey.reserve_hash !== hash) return { kind: 'conflict', payment: byKey.payment }
      return this.finishExisting(byKey)
    }
    const byOrder = this.store.getByOrder(req.order_id)
    if (byOrder) return { kind: 'conflict', payment: byOrder.payment }
    if (this.failNext === 'authorize') {
      this.failNext = null
      return rejected('processor', 'processor_error')
    }

    const payment: Payment = {
      payment_id: this.clock.newId(), order_id: req.order_id, customer_id: req.customer_id,
      amount_cents: req.amount_cents, currency: req.currency, status: 'reserved',
      reserved_cents: 0, captured_cents: 0, reserve_key: req.idempotency_key,
      created_at: this.clock.now().toISOString(), correlation_id: req.correlation_id,
    }
    if (this.blocked.has(req.customer_id)) return this.failReserve(payment, hash, 'card_declined', req.causation_id)
    if (!this.store.authorize(req.customer_id, req.amount_cents)) {
      return this.failReserve(payment, hash, 'insufficient_funds', req.causation_id)
    }
    payment.reserved_cents = req.amount_cents
    this.store.put({ payment, reserve_hash: hash, event: null, published: false })
    return { kind: 'reserved', payment }
  }

  private async chargeLocked(req: ChargeRequest): Promise<Outcome> {
    if (req.idempotency_key.length < 8) return rejected('idempotency_key', 'minLength 8')
    if (!UUID_RE.test(req.order_id)) return rejected('order_id', 'uuid')
    const byCharge = this.store.getByChargeKey(req.idempotency_key)
    if (byCharge) {
      if (byCharge.payment.order_id !== req.order_id) return { kind: 'conflict', payment: byCharge.payment }
      if (byCharge.payment.status === 'failed') return this.finishFailedCharge(byCharge)
      return this.finishExisting(byCharge)
    }
    const rec = this.store.getByOrder(req.order_id)
    if (!rec) return rejected('order_id', 'not_found')
    if (rec.payment.status === 'failed') {
      this.store.bindCharge(req.idempotency_key, rec)
      return this.finishFailedCharge(rec)
    }
    if (rec.payment.status === 'charged') {
      this.store.bindCharge(req.idempotency_key, rec)
      return this.finishExisting(rec)
    }
    if (rec.payment.status !== 'reserved') return rejected('status', 'not_reserved')
    if (this.failNext === 'capture') {
      this.failNext = null
      return rejected('processor', 'processor_error')
    }
    const event = this.envelope(
      'events.payment_charged',
      {
        order_id: rec.payment.order_id, payment_id: rec.payment.payment_id,
        amount_cents: rec.payment.reserved_cents, currency: rec.payment.currency,
      },
      rec.payment.correlation_id, req.causation_id, this.clock.now().toISOString(),
    )
    if (!event.ok) return event.outcome
    if (!this.store.capture(rec.payment.customer_id, rec.payment.reserved_cents)) {
      return rejected('ledger', 'hold_missing')
    }
    rec.payment.status = 'charged'
    rec.payment.captured_cents = rec.payment.reserved_cents
    rec.event = event.envelope
    this.store.bindCharge(req.idempotency_key, rec)
    this.store.put(rec)
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return { kind: 'charged', payment: rec.payment, event: event.envelope }
  }

  private async finishFailedCharge(rec: PaymentRecord): Promise<Outcome> {
    if (!rec.event) return rejected('event', 'missing')
    if (!rec.published) {
      const failed = await this.tryPublish(rec)
      if (failed) return failed
    }
    return { kind: 'failed', payment: rec.payment, event: rec.event }
  }

  private async failReserve(
    payment: Payment,
    hash: string,
    reason: Exclude<FailureReason, 'processor_error'>,
    causation_id: string | undefined,
  ): Promise<Outcome> {
    payment.status = 'failed'
    payment.reason = reason
    payment.retryable = false
    const event = this.envelope(
      'events.payment_failed',
      { order_id: payment.order_id, reason, retryable: false },
      payment.correlation_id, causation_id, payment.created_at,
    )
    if (!event.ok) return event.outcome
    const rec: PaymentRecord = { payment, reserve_hash: hash, event: event.envelope, published: false }
    this.store.put(rec)
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return { kind: 'failed', payment, event: event.envelope }
  }

  private envelope(
    type: 'events.payment_charged' | 'events.payment_failed',
    payload: Record<string, string | number | boolean>,
    correlation_id: string,
    causation_id: string | undefined,
    occurred_at: string,
  ): { ok: true; envelope: MessageEnvelope } | { ok: false; outcome: Outcome } {
    const built = createEnvelope({
      type, source: 'payments', payload, correlation_id,
      message_id: this.clock.newId(), causation_id, occurred_at,
    })
    if (!built.ok || !built.envelope) {
      return {
        ok: false,
        outcome: { kind: 'rejected', errors: built.ok ? [{ path: '$', message: 'envelope' }] : built.errors },
      }
    }
    return { ok: true, envelope: built.envelope }
  }

  private async finishExisting(rec: PaymentRecord): Promise<Outcome> {
    if (rec.payment.status === 'reserved' || !rec.event) return { kind: 'duplicate', payment: rec.payment }
    if (rec.published) return { kind: 'duplicate', payment: rec.payment, event: rec.event }
    const failed = await this.tryPublish(rec)
    if (failed) return failed
    return { kind: 'replayed', payment: rec.payment, event: rec.event }
  }

  private async tryPublish(
    rec: PaymentRecord,
  ): Promise<Extract<Outcome, { kind: 'publish_failed' }> | null> {
    if (!rec.event) return null
    const routingKey =
      rec.event.type === 'events.payment_failed' ? PAYMENT_FAILED_ROUTING_KEY : PAYMENT_CHARGED_ROUTING_KEY
    try {
      await this.publisher.publish(routingKey, rec.event)
      rec.published = true
      return null
    } catch (caught) {
      const error = caught instanceof Error ? caught : new Error(String(caught))
      return { kind: 'publish_failed', payment: rec.payment, event: rec.event, error }
    }
  }
}
