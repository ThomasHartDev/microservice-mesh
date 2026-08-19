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
type Sub = { id: number; pattern: string; queue: string | undefined; handler: (d: Delivery) => void | Promise<void> }

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

export function createMemoryBroker(): Broker {
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

  const deliver = async (sub: Sub, subject: string, data: Uint8Array, attempt: number): Promise<void> => {
    let nacked = false
    try {
      await sub.handler({ subject, data, ack: () => {}, nack: () => { nacked = true } })
    } catch {
      nacked = true
    }
    if (nacked && attempt < MAX_DELIVER) {
      const next = sub.queue === undefined ? [sub] : pick(subject, sub.pattern, sub.queue)
      await deliver(next[0] ?? sub, subject, data, attempt + 1)
    }
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
      if (queue !== undefined && queue.length === 0) throw new SubjectError('invalid queue')
      const sub: Sub = { id: nextId++, pattern, queue, handler }
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
