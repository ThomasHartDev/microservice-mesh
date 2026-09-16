import { getCatalogEntry, parseEnvelope, type MessageEnvelope } from './contracts.js'

export const MAX_DELIVER = 3

export class SubjectError extends Error {
  override readonly name = 'SubjectError'
  constructor(message: string) {
    super(message)
  }
}

export class ClosedError extends Error {
  override readonly name = 'ClosedError'
  constructor() {
    super('broker closed')
  }
}

export type Delivery = { subject: string; data: Uint8Array; ack: () => void; nack: () => void }
export type Broker = {
  publish(subject: string, data: Uint8Array): Promise<void>
  subscribe(pattern: string, handler: (d: Delivery) => void | Promise<void>, opts?: { queue?: string }): Promise<{ unsubscribe: () => void }>
  close(): Promise<void>
}

export type CircuitState = 'closed' | 'open' | 'half-open'
export type BackoffOptions = { baseMs: number; maxMs: number; factor: number }
export type CircuitBreakerOptions = { failureThreshold: number; cooldownMs: number }
export type DeadLetter = { subject: string; reason: 'max_deliver_exhausted' | 'circuit_open'; attempts: number; failedAt: string; data: number[] }
export type BrokerOptions = {
  maxDeliver?: number
  backoff?: BackoffOptions
  circuitBreaker?: CircuitBreakerOptions
  deadLetterSubject?: (subject: string) => string
  now?: () => number
  sleep?: (ms: number) => Promise<void>
}

const DEFAULT_BACKOFF: BackoffOptions = { baseMs: 0, maxMs: 0, factor: 2 }

function defaultDeadLetterSubject(subject: string): string {
  return `dlq.${subject}`
}

function defaultSleep(ms: number): Promise<void> {
  return ms <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms))
}

function backoffDelayMs(backoff: BackoffOptions, attempt: number): number {
  const delay = backoff.baseMs * backoff.factor ** (attempt - 1)
  return Math.min(delay, backoff.maxMs)
}

type Breaker = { state: CircuitState; failures: number; openedAt: number }
type Sub = { id: number; pattern: string; queue: string | undefined; handler: (d: Delivery) => void | Promise<void>; breaker: Breaker }

export function validSubject(value: string, wildcards: boolean): boolean {
  if (!value) return false
  const tokens = value.split('.')
  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i]!
    if (tok === '') return false
    if (tok === '>') return wildcards && i === tokens.length - 1
    if (tok === '*') {
      if (!wildcards) return false
      continue
    }
    if (/[*>\s]/.test(tok)) return false
  }
  return true
}

export function matchSubject(subject: string, pattern: string): boolean {
  if (!validSubject(subject, false) || !validSubject(pattern, true)) return false
  const s = subject.split('.')
  const p = pattern.split('.')
  let i = 0
  for (const tok of p) {
    if (tok === '>') return i < s.length
    if (i >= s.length || (tok !== '*' && tok !== s[i])) return false
    i++
  }
  return i === s.length
}

export function createMemoryBroker(opts: BrokerOptions = {}): Broker {
  const maxDeliver = opts.maxDeliver ?? MAX_DELIVER
  const backoff = opts.backoff ?? DEFAULT_BACKOFF
  const breakerOpts = opts.circuitBreaker
  const deadLetterSubject = opts.deadLetterSubject ?? defaultDeadLetterSubject
  const now = opts.now ?? Date.now
  const sleep = opts.sleep ?? defaultSleep

  let closed = false
  let nextId = 1
  const subs: Sub[] = []
  const rr = new Map<string, number>()

  const pick = (subject: string, wantPattern?: string, wantQueue?: string): Sub[] => {
    const groups = new Map<string, Sub[]>()
    const fanout: Sub[] = []
    for (const sub of subs) {
      if (wantPattern && (sub.pattern !== wantPattern || sub.queue !== wantQueue)) continue
      if (!matchSubject(subject, sub.pattern)) continue
      if (sub.queue === undefined) {
        fanout.push(sub)
        continue
      }
      const key = `${sub.pattern}\0${sub.queue}`
      const members = groups.get(key)
      if (members) members.push(sub)
      else groups.set(key, [sub])
    }
    const targets = [...fanout]
    for (const [key, members] of groups) {
      const i = (rr.get(key) ?? 0) % members.length
      rr.set(key, i + 1)
      const chosen = members[i]
      if (chosen) targets.push(chosen)
    }
    return targets
  }

  const breakerAllows = (sub: Sub): boolean => {
    if (!breakerOpts) return true
    if (sub.breaker.state === 'open') {
      if (now() - sub.breaker.openedAt < breakerOpts.cooldownMs) return false
      sub.breaker.state = 'half-open'
    }
    return true
  }

  const recordSuccess = (sub: Sub): void => {
    sub.breaker.failures = 0
    sub.breaker.state = 'closed'
  }

  const recordFailure = (sub: Sub): void => {
    if (!breakerOpts) return
    if (sub.breaker.state === 'half-open') {
      sub.breaker.state = 'open'
      sub.breaker.openedAt = now()
      sub.breaker.failures = 0
      return
    }
    sub.breaker.failures += 1
    if (sub.breaker.failures >= breakerOpts.failureThreshold) {
      sub.breaker.state = 'open'
      sub.breaker.openedAt = now()
      sub.breaker.failures = 0
    }
  }

  const deadLetter = async (subject: string, data: Uint8Array, reason: DeadLetter['reason'], attempts: number): Promise<void> => {
    const letter: DeadLetter = { subject, reason, attempts, failedAt: new Date(now()).toISOString(), data: Array.from(data) }
    const dlqSubject = deadLetterSubject(subject)
    const encoded = new TextEncoder().encode(JSON.stringify(letter))
    for (const target of pick(dlqSubject)) {
      try {
        await target.handler({ subject: dlqSubject, data: encoded.slice(), ack: () => {}, nack: () => {} })
      } catch {
        /* dead-letter delivery is best-effort */
      }
    }
  }

  const deliver = async (sub: Sub, subject: string, data: Uint8Array, attempt: number): Promise<void> => {
    if (!breakerAllows(sub)) {
      await deadLetter(subject, data, 'circuit_open', attempt)
      return
    }
    let nacked = false
    try {
      await sub.handler({ subject, data: data.slice(), ack: () => {}, nack: () => { nacked = true } })
    } catch {
      nacked = true
    }
    if (!nacked) {
      recordSuccess(sub)
      return
    }
    recordFailure(sub)
    if (attempt >= maxDeliver) {
      await deadLetter(subject, data, 'max_deliver_exhausted', attempt)
      return
    }
    await sleep(backoffDelayMs(backoff, attempt))
    const next = sub.queue === undefined ? [sub] : pick(subject, sub.pattern, sub.queue)
    await deliver(next[0] ?? sub, subject, data, attempt + 1)
  }

  return {
    async publish(subject, data) {
      if (closed) throw new ClosedError()
      if (!validSubject(subject, false)) throw new SubjectError('invalid subject')
      const payload = data.slice()
      for (const sub of pick(subject)) await deliver(sub, subject, payload, 1)
    },
    async subscribe(pattern, handler, opts) {
      if (closed) throw new ClosedError()
      if (!validSubject(pattern, true)) throw new SubjectError('invalid pattern')
      const queue = opts?.queue
      if (queue !== undefined && queue.trim() === '') throw new SubjectError('invalid queue')
      const sub: Sub = { id: nextId++, pattern, queue, handler, breaker: { state: 'closed', failures: 0, openedAt: 0 } }
      subs.push(sub)
      return {
        unsubscribe() {
          const i = subs.findIndex((s) => s.id === sub.id)
          if (i >= 0) subs.splice(i, 1)
        },
      }
    },
    async close() {
      closed = true
      subs.length = 0
    },
  }
}

export async function publishEnvelope(broker: Broker, env: MessageEnvelope): Promise<void> {
  const parsed = parseEnvelope(env)
  if (!parsed.ok) throw new SubjectError('invalid envelope')
  const entry = getCatalogEntry(parsed.envelope.type)
  if (!entry) throw new SubjectError('unknown type')
  await broker.publish(entry.routing_key, new TextEncoder().encode(JSON.stringify(parsed.envelope)))
}
