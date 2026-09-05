import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { checkComposePolicy, parseCompose, parseDurationMs, parseYaml, startupOrder, type ComposeFile } from '../src/index.js'

const clone = (): ComposeFile => structuredClone(parseCompose(readFileSync('docker-compose.yml', 'utf8')))
const codes = (file: ComposeFile) => checkComposePolicy(file).map((f) => f.code)

describe('mesh compose file', () => {
  it('passes policy, matches image builds, and starts infra first', () => {
    const file = clone()
    expect(checkComposePolicy(file)).toEqual([])
    const order = startupOrder(file)!
    const idx = (n: string) => order.indexOf(n)
    expect(idx('nats')).toBeLessThan(idx('gateway'))
    expect(idx('nats')).toBeLessThan(idx('orders'))
    expect(idx('postgres')).toBeLessThan(idx('inventory'))
    expect(idx('redis')).toBeLessThan(idx('notifications'))
    expect(file.services.gateway?.dependsOn).toEqual({ nats: 'service_healthy' })
    expect(file.services.postgres?.ports).toEqual([])
    expect(file.services.gateway?.build).toEqual({ context: 'services/gateway', dockerfile: 'Dockerfile' })
    expect(file.services.orders?.build?.dockerfile).toBe('services/orders/Dockerfile')
  })

  it('rejects started-only edges, published stores, cycles, and CMD-SHELL on gateway', () => {
    const bad = clone()
    delete bad.services.orders!.healthcheck
    bad.services.orders!.dependsOn.nats = 'service_started'
    bad.services.postgres!.ports = ['5432:5432']
    expect(codes(bad)).toEqual(expect.arrayContaining(['healthcheck', 'started-only', 'broker-edge', 'publish-store']))
    const cyclic = clone()
    cyclic.services.nats!.dependsOn = { gateway: 'service_healthy' }
    expect(codes(cyclic)).toContain('cycle')
    expect(startupOrder(cyclic)).toBeNull()
    const dangling = clone()
    dangling.services.gateway!.dependsOn.missing = 'service_healthy'
    expect(codes(dangling)).toContain('dangling')
    const shell = clone()
    shell.services.gateway!.healthcheck!.test = ['CMD-SHELL', 'true']
    expect(codes(shell)).toEqual(expect.arrayContaining(['cmd-shell', 'probe']))
  })

  it('treats short-form depends_on as service_started and rejects zero probes', () => {
    const file = parseCompose('name: mesh\nnetworks:\n  mesh:\nservices:\n  nats:\n    image: nats:2.10-alpine\n    depends_on:\n      - gateway\n  gateway:\n    image: alpine\n')
    expect(file.services.nats?.dependsOn).toEqual({ gateway: 'service_started' })
    expect(codes(file)).toEqual(expect.arrayContaining(['started-only', 'missing']))
    const zero = clone()
    zero.services.nats!.healthcheck!.interval = '0s'
    zero.services.redis!.healthcheck!.disable = true
    zero.services.postgres!.healthcheck!.retries = 0
    expect(codes(zero)).toEqual(expect.arrayContaining(['interval', 'healthcheck', 'retries']))
  })

  it('parses flow lists, env lists, durations, and rejects tabs', () => {
    expect(parseYaml('ports: ["8080:8080", \'4222:4222\']')).toEqual({ ports: ['8080:8080', '4222:4222'] })
    expect(() => parseYaml('\tservices:\n')).toThrow(/tab/)
    expect(parseDurationMs('5s')).toBe(5000)
    expect(parseDurationMs('0s')).toBe(0)
    expect(parseDurationMs('-1s')).toBeNull()
    const env = parseCompose('services:\n  gateway:\n    environment:\n      - GATEWAY_ADDR=:8080\n      - NATS_URL=nats://nats:4222\n    build: services/gateway\n')
    expect(env.services.gateway?.environment.GATEWAY_ADDR).toBe(':8080')
    expect(env.services.gateway?.build).toEqual({ context: 'services/gateway', dockerfile: 'Dockerfile' })
  })
})
