import {
  createEnvelope,
  parseEnvelope,
  type JsonObject,
  type MessageEnvelope,
  type ValidationError,
} from './contracts.js'

export type Currency = 'USD' | 'EUR' | 'GBP'
export type SagaStatus = 'awaiting_inventory' | 'awaiting_payment' | 'completed' | 'cancelled'
export type Compensation = 'release_inventory' | 'refund_payment' | 'notify_customer'
export type CancelReason = 'inventory_failed' | 'payment_failed' | 'customer_cancelled'
export type ParkedPayment =
  | { outcome: 'charged'; payment_id: string; amount_cents: number; currency: Currency }
  | { outcome: 'failed' }

export type SagaInstance = {
  order_id: string; customer_id: string; currency: Currency; total_cents: number
  correlation_id: string; status: SagaStatus
  reservation_id?: string; payment_id?: string; parked?: ParkedPayment
  cancel_reason?: CancelReason; compensations: Compensation[]
  request_hash: string; terminal?: MessageEnvelope; published: boolean
}

export type Outcome =
  | { kind: 'started' | 'awaiting'; saga: SagaInstance }
  | { kind: 'completed' | 'cancelled' | 'replayed'; saga: SagaInstance; event: MessageEnvelope }
  | { kind: 'duplicate'; saga: SagaInstance; event?: MessageEnvelope }
  | { kind: 'conflict'; saga: SagaInstance }
  | { kind: 'rejected'; errors: ValidationError[] }
  | { kind: 'publish_failed'; saga: SagaInstance; event: MessageEnvelope; error: Error }

export type Publisher = { publish(routingKey: string, envelope: MessageEnvelope): Promise<void> }
export type Clock = { now(): Date; newId(): string }

export const ORDER_COMPLETED_ROUTING_KEY = 'events.orders.completed'
export const ORDER_CANCELLED_ROUTING_KEY = 'events.orders.cancelled'

export class MemoryPublisher implements Publisher {
  events: Array<{ routingKey: string; envelope: MessageEnvelope }> = []
  fail: Error | null = null
  async publish(routingKey: string, envelope: MessageEnvelope): Promise<void> {
    if (this.fail) throw this.fail
    this.events.push({ routingKey, envelope })
  }
}

export function compensationsFor(reserved: boolean, charged: boolean): Compensation[] {
  const steps: Compensation[] = []
  if (charged) steps.push('refund_payment')
  if (reserved) steps.push('release_inventory')
  steps.push('notify_customer')
  return steps
}

const reject = (path: string, message: string): Outcome => ({ kind: 'rejected', errors: [{ path, message }] })

function money(v: unknown): Currency | undefined {
  return v === 'USD' || v === 'EUR' || v === 'GBP' ? v : undefined
}

export class MemorySagaStore {
  private readonly byOrder = new Map<string, SagaInstance>()
  private readonly byMessage = new Map<string, SagaInstance>()
  private tail: Promise<void> = Promise.resolve()

  serialize<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.tail.then(fn, fn)
    this.tail = run.then(() => undefined, () => undefined)
    return run
  }

  getByOrder(id: string) { return this.byOrder.get(id) }
  getByMessage(id: string) { return this.byMessage.get(id) }

  put(saga: SagaInstance, messageId: string): void {
    this.byOrder.set(saga.order_id, saga)
    this.byMessage.set(messageId, saga)
  }
  index(messageId: string, saga: SagaInstance): void { this.byMessage.set(messageId, saga) }
}

export class OrderSagaOrchestrator {
  constructor(readonly store: MemorySagaStore, readonly publisher: Publisher, readonly clock: Clock) {}
  handle(raw: unknown): Promise<Outcome> {
    return this.store.serialize(() => this.handleLocked(raw))
  }
  private async handleLocked(raw: unknown): Promise<Outcome> {
    const parsed = parseEnvelope(raw)
    if (!parsed.ok) return { kind: 'rejected', errors: parsed.errors }
    const env = parsed.envelope
    const seen = this.store.getByMessage(env.message_id)
    if (seen) return this.finishExisting(seen)
    if (env.type === 'events.order_created') return this.start(env)
    if (env.type === 'events.inventory_reserved' || env.type === 'events.inventory_reservation_failed') return this.onInventory(env)
    if (env.type === 'events.payment_charged' || env.type === 'events.payment_failed') return this.onPayment(env)
    return reject('type', `unexpected ${env.type}`)
  }
  private start(env: MessageEnvelope): Promise<Outcome> | Outcome {
    if (env.source !== 'orders') return reject('source', 'expected orders')
    const p = env.payload
    const order_id = p['order_id']; const customer_id = p['customer_id']
    const total_cents = p['total_cents']; const currency = money(p['currency'])
    if (typeof order_id !== 'string' || typeof customer_id !== 'string' || typeof total_cents !== 'number' || !currency) {
      return reject('payload', 'shape')
    }
    const hash = JSON.stringify({ customer_id, currency, total_cents, items: p['items'] })
    const existing = this.store.getByOrder(order_id)
    if (existing) {
      if (existing.request_hash !== hash) return { kind: 'conflict', saga: existing }
      this.store.index(env.message_id, existing)
      return this.finishExisting(existing)
    }
    const saga: SagaInstance = {
      order_id, customer_id, currency, total_cents, correlation_id: env.correlation_id,
      status: 'awaiting_inventory', compensations: [], request_hash: hash, published: false,
    }
    this.store.put(saga, env.message_id)
    return { kind: 'started', saga }
  }
  private async onInventory(env: MessageEnvelope): Promise<Outcome> {
    if (env.source !== 'inventory') return reject('source', 'expected inventory')
    const saga = this.lookup(env)
    if (!saga) return reject('payload.order_id', 'unknown')
    if (saga.status === 'completed') return { kind: 'conflict', saga }
    if (saga.status === 'cancelled') return this.lateEffect(saga, env)
    if (saga.reservation_id !== undefined || saga.cancel_reason === 'inventory_failed') {
      return { kind: 'conflict', saga }
    }
    if (env.type === 'events.inventory_reservation_failed') {
      if (saga.parked?.outcome === 'charged') saga.payment_id = saga.parked.payment_id
      return this.emitCancel(saga, env, 'inventory_failed')
    }
    const reservation_id = env.payload['reservation_id']
    if (typeof reservation_id !== 'string') return reject('payload.reservation_id', 'required')
    saga.reservation_id = reservation_id
    if (saga.parked?.outcome === 'charged') {
      return this.settleCharge(saga, env, saga.parked.payment_id, saga.parked.amount_cents, saga.parked.currency)
    }
    if (saga.parked?.outcome === 'failed') return this.emitCancel(saga, env, 'payment_failed')
    saga.status = 'awaiting_payment'
    return { kind: 'awaiting', saga }
  }
  private async onPayment(env: MessageEnvelope): Promise<Outcome> {
    if (env.source !== 'payments') return reject('source', 'expected payments')
    const saga = this.lookup(env)
    if (!saga) return reject('payload.order_id', 'unknown')
    if (saga.status === 'completed') {
      if (env.type === 'events.payment_charged' && env.payload['payment_id'] === saga.payment_id) {
        return { kind: 'duplicate', saga, event: saga.terminal }
      }
      return { kind: 'conflict', saga }
    }
    if (saga.status === 'cancelled') return this.lateEffect(saga, env)
    if (env.type === 'events.payment_failed') {
      if (saga.parked?.outcome === 'charged' || saga.payment_id !== undefined) {
        return { kind: 'awaiting', saga }
      }
      if (saga.parked !== undefined) return { kind: 'conflict', saga }
      if (saga.status === 'awaiting_inventory') {
        saga.parked = { outcome: 'failed' }
        return { kind: 'awaiting', saga }
      }
      return this.emitCancel(saga, env, 'payment_failed')
    }
    if (saga.parked?.outcome === 'charged' || saga.payment_id !== undefined) {
      return { kind: 'conflict', saga }
    }
    const payment_id = env.payload['payment_id']
    const amount_cents = env.payload['amount_cents']
    const currency = money(env.payload['currency'])
    if (typeof payment_id !== 'string' || typeof amount_cents !== 'number' || !currency) {
      return reject('payload', 'shape')
    }
    if (saga.status === 'awaiting_inventory') {
      saga.parked = { outcome: 'charged', payment_id, amount_cents, currency }
      return { kind: 'awaiting', saga }
    }
    return this.settleCharge(saga, env, payment_id, amount_cents, currency)
  }
  private lookup(env: MessageEnvelope): SagaInstance | undefined {
    const order_id = env.payload['order_id']
    if (typeof order_id !== 'string') return undefined
    const saga = this.store.getByOrder(order_id)
    if (saga) this.store.index(env.message_id, saga)
    return saga
  }
  private settleCharge(
    saga: SagaInstance, cause: MessageEnvelope, payment_id: string, amount_cents: number, currency: Currency,
  ): Promise<Outcome> {
    saga.payment_id = payment_id
    saga.parked = undefined
    if (amount_cents !== saga.total_cents || currency !== saga.currency) {
      return this.emitCancel(saga, cause, 'payment_failed')
    }
    return this.emitComplete(saga, cause)
  }
  private lateEffect(saga: SagaInstance, env: MessageEnvelope): Promise<Outcome> | Outcome {
    if (env.type === 'events.payment_charged' && saga.payment_id === undefined) {
      const payment_id = env.payload['payment_id']
      if (typeof payment_id !== 'string') return reject('payload.payment_id', 'required')
      saga.payment_id = payment_id
      saga.parked = undefined
      return this.emitLateWork(saga, env, 'refund_payment')
    }
    if (env.type === 'events.inventory_reserved' && saga.reservation_id === undefined) {
      const reservation_id = env.payload['reservation_id']
      if (typeof reservation_id !== 'string') return reject('payload.reservation_id', 'required')
      saga.reservation_id = reservation_id
      return this.emitLateWork(saga, env, 'release_inventory')
    }
    return { kind: 'duplicate', saga, event: saga.terminal }
  }
  private emitLateWork(saga: SagaInstance, cause: MessageEnvelope, step: Compensation): Promise<Outcome> {
    const reason = saga.cancel_reason ?? (step === 'refund_payment' ? 'payment_failed' : 'inventory_failed')
    if (!saga.published || !saga.terminal) {
      return this.emitCancel(saga, cause, reason)
    }
    saga.compensations = [step]
    return this.publishTerminal(saga, cause, {
      type: 'events.order_cancelled',
      payload: { order_id: saga.order_id, reason, compensations: [step] },
      ok: 'cancelled',
    })
  }
  private emitComplete(saga: SagaInstance, cause: MessageEnvelope): Promise<Outcome> {
    if (!saga.reservation_id || !saga.payment_id) return Promise.resolve(reject('$', 'incomplete'))
    saga.status = 'completed'
    saga.compensations = []
    return this.publishTerminal(saga, cause, {
      type: 'events.order_completed',
      payload: {
        order_id: saga.order_id, payment_id: saga.payment_id,
        reservation_id: saga.reservation_id, total_cents: saga.total_cents,
      },
      ok: 'completed',
    })
  }
  private emitCancel(saga: SagaInstance, cause: MessageEnvelope, reason: CancelReason): Promise<Outcome> {
    saga.status = 'cancelled'
    saga.cancel_reason = reason
    saga.parked = undefined
    saga.compensations = compensationsFor(saga.reservation_id !== undefined, saga.payment_id !== undefined)
    return this.publishTerminal(saga, cause, {
      type: 'events.order_cancelled',
      payload: { order_id: saga.order_id, reason, compensations: [...saga.compensations] },
      ok: 'cancelled',
    })
  }
  private async publishTerminal(
    saga: SagaInstance, cause: MessageEnvelope,
    spec: { type: 'events.order_completed' | 'events.order_cancelled'; payload: JsonObject; ok: 'completed' | 'cancelled' },
  ): Promise<Outcome> {
    const built = createEnvelope({
      type: spec.type, source: 'orders', payload: spec.payload,
      correlation_id: saga.correlation_id, message_id: this.clock.newId(),
      causation_id: cause.message_id, occurred_at: this.clock.now().toISOString(),
    })
    if (!built.ok || !built.envelope) {
      return { kind: 'rejected', errors: built.ok ? [{ path: '$', message: 'envelope' }] : built.errors }
    }
    saga.terminal = built.envelope
    saga.published = false
    this.store.index(built.envelope.message_id, saga)
    const failed = await this.tryPublish(saga)
    if (failed) return failed
    return { kind: spec.ok, saga, event: built.envelope }
  }
  private async finishExisting(saga: SagaInstance): Promise<Outcome> {
    if (!saga.terminal) return { kind: 'duplicate', saga }
    if (saga.published) return { kind: 'duplicate', saga, event: saga.terminal }
    const failed = await this.tryPublish(saga)
    if (failed) return failed
    return { kind: 'replayed', saga, event: saga.terminal }
  }
  private async tryPublish(saga: SagaInstance): Promise<Outcome | null> {
    const event = saga.terminal
    if (!event) return null
    const key = event.type === 'events.order_completed' ? ORDER_COMPLETED_ROUTING_KEY : ORDER_CANCELLED_ROUTING_KEY
    try {
      await this.publisher.publish(key, event)
      saga.published = true
      return null
    } catch (err) {
      return { kind: 'publish_failed', saga, event, error: err instanceof Error ? err : new Error(String(err)) }
    }
  }
}
