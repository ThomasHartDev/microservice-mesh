import { describe, expect, it } from 'vitest'
import { createMemoryBroker, type DeadLetter } from '../src/broker.js'

const enc = new TextEncoder()
const dec = new TextDecoder()

function fakeClock(start = 0) {
  let t = start
  return { now: () => t, advance: (ms: number) => { t += ms } }
}

function recordingSleep(): { sleep: (ms: number) => Promise<void>; delays: number[] } {
  const delays: number[] = []
  return { sleep: async (ms: number) => { delays.push(ms) }, delays }
}

function decodeLetter(data: Uint8Array): DeadLetter {
  return JSON.parse(dec.decode(data)) as DeadLetter
}

describe('retry backoff', () => {
  it('doubles the delay per attempt up to maxMs, and skips the final attempt', async () => {
    const { sleep, delays } = recordingSleep()
    const bus = createMemoryBroker({ maxDeliver: 4, backoff: { baseMs: 10, maxMs: 35, factor: 2 }, sleep })
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; throw new Error('down') })
    await bus.publish('a.b', enc.encode('x'))
    expect(calls).toBe(4)
    expect(delays).toEqual([10, 20, 35])
  })

  it('never sleeps when maxDeliver is 1', async () => {
    const { sleep, delays } = recordingSleep()
    const bus = createMemoryBroker({ maxDeliver: 1, backoff: { baseMs: 50, maxMs: 50, factor: 2 }, sleep })
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; throw new Error('down') })
    await bus.publish('a.b', enc.encode('x'))
    expect(calls).toBe(1)
    expect(delays).toEqual([])
  })
})

describe('circuit breaker', () => {
  it('opens after the failure threshold and short-circuits the handler', async () => {
    const clock = fakeClock()
    const bus = createMemoryBroker({
      maxDeliver: 1,
      circuitBreaker: { failureThreshold: 2, cooldownMs: 1000 },
      now: clock.now,
    })
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; throw new Error('down') })
    await bus.publish('a.b', enc.encode('1'))
    await bus.publish('a.b', enc.encode('2'))
    expect(calls).toBe(2)
    await bus.publish('a.b', enc.encode('3'))
    expect(calls).toBe(2)
  })

  it('half-opens after the cooldown, closes on a successful trial', async () => {
    const clock = fakeClock()
    const bus = createMemoryBroker({
      maxDeliver: 1,
      circuitBreaker: { failureThreshold: 1, cooldownMs: 100 },
      now: clock.now,
    })
    let succeed = false
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; if (!succeed) throw new Error('down') })
    await bus.publish('a.b', enc.encode('1'))
    expect(calls).toBe(1)
    await bus.publish('a.b', enc.encode('2'))
    expect(calls).toBe(1)
    clock.advance(100)
    succeed = true
    await bus.publish('a.b', enc.encode('3'))
    expect(calls).toBe(2)
    await bus.publish('a.b', enc.encode('4'))
    expect(calls).toBe(3)
  })

  it('reopens immediately when the half-open trial fails', async () => {
    const clock = fakeClock()
    const bus = createMemoryBroker({
      maxDeliver: 1,
      circuitBreaker: { failureThreshold: 1, cooldownMs: 100 },
      now: clock.now,
    })
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; throw new Error('down') })
    await bus.publish('a.b', enc.encode('1'))
    clock.advance(100)
    await bus.publish('a.b', enc.encode('2'))
    expect(calls).toBe(2)
    await bus.publish('a.b', enc.encode('3'))
    expect(calls).toBe(2)
    clock.advance(50)
    await bus.publish('a.b', enc.encode('4'))
    expect(calls).toBe(2)
    clock.advance(100)
    await bus.publish('a.b', enc.encode('5'))
    expect(calls).toBe(3)
  })

  it('tracks queue members independently', async () => {
    const clock = fakeClock()
    const bus = createMemoryBroker({
      maxDeliver: 1,
      circuitBreaker: { failureThreshold: 1, cooldownMs: 1000 },
      now: clock.now,
    })
    const hits: number[] = []
    await bus.subscribe('jobs', () => { hits.push(1); throw new Error('down') }, { queue: 'w' })
    await bus.subscribe('jobs', () => { hits.push(2) }, { queue: 'w' })
    await bus.publish('jobs', enc.encode('1'))
    await bus.publish('jobs', enc.encode('2'))
    expect(hits).toEqual([1, 2])
  })
})

describe('dead-letter path', () => {
  it('routes an exhausted message to dlq.<subject> with the original bytes intact', async () => {
    const bus = createMemoryBroker({ maxDeliver: 2, backoff: { baseMs: 0, maxMs: 0, factor: 1 } })
    await bus.subscribe('orders.created', () => { throw new Error('handler exploded') })
    const letters: DeadLetter[] = []
    await bus.subscribe('dlq.orders.created', (d) => { letters.push(decodeLetter(d.data)) })
    await bus.publish('orders.created', enc.encode('payload'))
    expect(letters).toHaveLength(1)
    expect(letters[0]).toMatchObject({ subject: 'orders.created', reason: 'max_deliver_exhausted', attempts: 2 })
    expect(dec.decode(new Uint8Array(letters[0]!.data))).toBe('payload')
  })

  it('routes to a custom dead-letter subject and tags circuit_open as the reason', async () => {
    const clock = fakeClock()
    const bus = createMemoryBroker({
      maxDeliver: 1,
      circuitBreaker: { failureThreshold: 1, cooldownMs: 1000 },
      now: clock.now,
      deadLetterSubject: (subject) => `graveyard.${subject}`,
    })
    let calls = 0
    await bus.subscribe('a.b', () => { calls += 1; throw new Error('down') })
    const letters: DeadLetter[] = []
    await bus.subscribe('graveyard.a.b', (d) => { letters.push(decodeLetter(d.data)) })
    await bus.publish('a.b', enc.encode('1'))
    await bus.publish('a.b', enc.encode('2'))
    expect(calls).toBe(1)
    expect(letters.map((l) => l.reason)).toEqual(['max_deliver_exhausted', 'circuit_open'])
  })

  it('never crashes publish when nothing subscribes to the dead-letter subject', async () => {
    const bus = createMemoryBroker({ maxDeliver: 1 })
    await bus.subscribe('a.b', () => { throw new Error('down') })
    await expect(bus.publish('a.b', enc.encode('x'))).resolves.toBeUndefined()
  })
})
