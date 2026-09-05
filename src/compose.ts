import { SERVICE_IMAGES } from './images.js'

export type YamlValue = null | boolean | number | string | YamlValue[] | { [k: string]: YamlValue }
export type DependsCondition = 'service_started' | 'service_healthy' | 'service_completed_successfully'
export type ComposeFinding = { code: string; message: string }
export type ComposeHealthcheck = { test: string[]; interval?: string; timeout?: string; retries?: number; start_period?: string; disable?: boolean }
export type ComposeService = {
  image?: string; build?: { context: string; dockerfile: string }; environment: Record<string, string>
  ports: string[]; volumes: string[]; dependsOn: Record<string, DependsCondition>
  healthcheck?: ComposeHealthcheck; networks: string[]; restart?: string
}
export type ComposeFile = { name?: string; services: Record<string, ComposeService>; volumes: string[]; networks: string[] }

const SQL = new Set(['orders', 'payments', 'inventory'])
const MESH = ['gateway', 'orders', 'payments', 'inventory', 'notifications'] as const
const DUR = /^(0|[1-9]\d*)(ns|us|ms|s|m|h)$/
const COND = new Set<DependsCondition>(['service_started', 'service_healthy', 'service_completed_successfully'])

export function parseYaml(source: string): YamlValue {
  const rows: { n: number; i: number; s: string }[] = []
  for (const [n, raw] of source.split(/\r?\n/).entries()) {
    if (raw.includes('\t') && raw.trim()) throw new Error(`tab at line ${n + 1}`)
    const t = raw.trim()
    if (!t || t.startsWith('#')) continue
    rows.push({ n: n + 1, i: raw.length - raw.trimStart().length, s: t })
  }
  let p = 0
  const parse = (ind: number): YamlValue => {
    const row = rows[p]
    if (!row || row.i < ind) return {}
    if (row.s === '-' || row.s.startsWith('- ')) {
      const arr: YamlValue[] = []
      while (rows[p] && rows[p]!.i === ind && (rows[p]!.s === '-' || rows[p]!.s.startsWith('- '))) {
        const rest = rows[p]!.s === '-' ? '' : rows[p]!.s.slice(2)
        p++
        arr.push(rest ? inline(rest) : rows[p] && rows[p]!.i > ind ? parse(rows[p]!.i) : null)
      }
      return arr
    }
    const obj: Record<string, YamlValue> = {}
    while (rows[p] && rows[p]!.i === ind && rows[p]!.s !== '-' && !rows[p]!.s.startsWith('- ')) {
      if (rows[p]!.i > ind) throw new Error(`indent at line ${rows[p]!.n}`)
      const { key, rest } = splitKey(rows[p]!.s)
      p++
      obj[key] = rest ? inline(rest) : rows[p] && rows[p]!.i > ind ? parse(rows[p]!.i) : null
    }
    return obj
  }
  return rows.length === 0 ? {} : parse(rows[0]!.i)
}

export function parseCompose(source: string): ComposeFile {
  const root = parseYaml(source)
  if (!isMap(root)) throw new Error('compose root must be a mapping')
  const services: Record<string, ComposeService> = {}
  for (const [name, raw] of Object.entries(asMap(root.services))) services[name] = readService(raw)
  return { name: typeof root.name === 'string' ? root.name : undefined, services, volumes: Object.keys(asMap(root.volumes)), networks: Object.keys(asMap(root.networks)) }
}

export function parseDurationMs(value: string): number | null {
  const m = DUR.exec(value)
  if (!m) return null
  const u = m[2]!
  return Number(m[1]) * (u === 'ns' ? 1e-6 : u === 'us' ? 0.001 : u === 'ms' ? 1 : u === 's' ? 1000 : u === 'm' ? 60_000 : 3_600_000)
}

export function startupOrder(file: ComposeFile): string[] | null {
  const names = Object.keys(file.services)
  const indeg: Record<string, number> = Object.fromEntries(names.map((n) => [n, 0]))
  const adj: Record<string, string[]> = Object.fromEntries(names.map((n) => [n, [] as string[]]))
  for (const [svc, spec] of Object.entries(file.services)) {
    for (const dep of Object.keys(spec.dependsOn)) {
      if (dep in indeg) {
        adj[dep]!.push(svc)
        indeg[svc]!++
      }
    }
  }
  const q = names.filter((n) => indeg[n] === 0).sort()
  const out: string[] = []
  while (q.length) {
    const n = q.shift()!
    out.push(n)
    for (const m of [...adj[n]!].sort()) {
      if (--indeg[m]! === 0) {
        q.push(m)
        q.sort()
      }
    }
  }
  return out.length === names.length ? out : null
}

export function checkComposePolicy(file: ComposeFile): ComposeFinding[] {
  const out: ComposeFinding[] = []
  const push = (code: string, message: string) => out.push({ code, message })
  if (file.name !== 'mesh') push('name', 'compose project name must be mesh')
  if (!file.networks.includes('mesh')) push('network-def', 'missing mesh network')
  for (const vol of ['nats-data', 'postgres-data', 'redis-data']) if (!file.volumes.includes(vol)) push('volume-def', `missing volume ${vol}`)
  for (const name of [...MESH, 'nats', 'postgres', 'redis']) if (!file.services[name]) push('missing', `missing service ${name}`)
  const { nats, postgres, redis } = file.services
  if (nats && !nats.image?.includes('nats')) push('broker-image', 'nats image')
  if (postgres && !postgres.image?.includes('postgres')) push('store-image', 'postgres image')
  if (redis && !redis.image?.includes('redis')) push('store-image', 'redis image')
  if (postgres?.ports.length) push('publish-store', 'postgres must stay on the mesh network')
  if (redis?.ports.length) push('publish-store', 'redis must stay on the mesh network')
  if (nats && !nats.volumes.some((v) => v.startsWith('nats-data:'))) push('volume-mount', 'nats-data')
  if (postgres && !postgres.volumes.some((v) => v.startsWith('postgres-data:'))) push('volume-mount', 'postgres-data')
  if (redis && !redis.volumes.some((v) => v.startsWith('redis-data:'))) push('volume-mount', 'redis-data')
  probe(nats, 'nats', ['CMD', 'healthz'], true, push)
  probe(postgres, 'postgres', ['CMD-SHELL', 'pg_isready'], true, push)
  probe(redis, 'redis', ['CMD', 'redis-cli', 'ping'], true, push)
  for (const img of SERVICE_IMAGES) {
    const svc = file.services[img.service]
    if (!svc) continue
    const ctx = img.context === 'service' ? 'services/gateway' : '.'
    const df = img.context === 'service' ? 'Dockerfile' : img.dockerfile
    if (!svc.build) push('build', `${img.service} must build`)
    else {
      if (norm(svc.build.context) !== ctx) push('build-context', `${img.service} context`)
      if (svc.build.dockerfile !== df) push('build-file', `${img.service} dockerfile`)
    }
    if (svc.dependsOn.nats !== 'service_healthy') push('broker-edge', `${img.service} must wait for healthy nats`)
    if (svc.environment.NATS_URL !== 'nats://nats:4222') push('nats-url', `${img.service} NATS_URL`)
    if (!svc.networks.includes('mesh')) push('network', `${img.service} must join mesh`)
    if (svc.restart !== 'unless-stopped' && svc.restart !== 'always') push('restart', `${img.service} restart`)
    if (SQL.has(img.service)) {
      if (svc.dependsOn.postgres !== 'service_healthy') push('store-edge', `${img.service} must wait for healthy postgres`)
      if (!svc.environment.DATABASE_URL?.includes('postgres:5432')) push('db-url', `${img.service} DATABASE_URL`)
    }
    if (img.service === 'notifications') {
      if (svc.dependsOn.redis !== 'service_healthy') push('store-edge', 'notifications must wait for healthy redis')
      if (!svc.environment.REDIS_URL?.includes('redis:6379')) push('redis-url', 'REDIS_URL')
    }
    if (img.service === 'gateway') {
      probe(svc, 'gateway', ['CMD', '/app/gateway', '-health'], false, push)
      if (!svc.ports.includes('8080:8080')) push('ingress', 'gateway must publish 8080:8080')
      if (svc.healthcheck?.test[0] === 'CMD-SHELL') push('cmd-shell', 'distroless gateway cannot use CMD-SHELL')
    } else probe(svc, img.service, ['CMD-SHELL'], false, push)
  }
  for (const [name, svc] of Object.entries(file.services)) {
    for (const dep of Object.keys(svc.dependsOn)) {
      if (!file.services[dep]) push('dangling', `${name} depends on unknown ${dep}`)
      if (svc.dependsOn[dep] === 'service_started') push('started-only', `${name} -> ${dep} is service_started`)
    }
  }
  if (startupOrder(file) === null) push('cycle', 'depends_on graph has a cycle')
  return out
}

function probe(svc: ComposeService | undefined, name: string, tokens: string[], infra: boolean, push: (c: string, m: string) => void): void {
  const hc = svc?.healthcheck
  if (!svc || !hc || hc.disable || hc.test.length === 0) return push('healthcheck', `${name} needs a healthcheck`)
  for (const tok of tokens) if (!hc.test.some((t) => t === tok || t.includes(tok))) push('probe', `${name} healthcheck must include ${tok}`)
  if (!parseDurationMs(hc.interval ?? '')) push('interval', `${name} interval`)
  if (!parseDurationMs(hc.timeout ?? '')) push('timeout', `${name} timeout`)
  if (typeof hc.retries !== 'number' || hc.retries < 1 || !Number.isInteger(hc.retries)) push('retries', `${name} retries`)
  if (infra && parseDurationMs(hc.start_period ?? '') === null) push('start-period', `${name} start_period`)
}

function readService(raw: YamlValue): ComposeService {
  const m = asMap(raw)
  const h = m.healthcheck == null ? undefined : asMap(m.healthcheck)
  const dep = m.depends_on
  const dependsOn: Record<string, DependsCondition> = {}
  if (Array.isArray(dep)) {
    for (const item of dep) if (typeof item === 'string') dependsOn[item] = 'service_started'
  } else if (isMap(dep)) {
    for (const [k, v] of Object.entries(dep)) {
      const c = isMap(v) && typeof v.condition === 'string' ? v.condition : 'service_started'
      dependsOn[k] = COND.has(c as DependsCondition) ? (c as DependsCondition) : 'service_started'
    }
  }
  const b = m.build
  const build = typeof b === 'string' ? { context: b, dockerfile: 'Dockerfile' } : isMap(b) ? { context: str(b.context) ?? '.', dockerfile: str(b.dockerfile) ?? 'Dockerfile' } : undefined
  return {
    image: str(m.image),
    build,
    environment: envMap(m.environment),
    ports: list(m.ports),
    volumes: list(m.volumes),
    dependsOn,
    healthcheck: h ? { test: Array.isArray(h.test) ? h.test.map(String) : typeof h.test === 'string' ? ['CMD-SHELL', h.test] : [], interval: str(h.interval), timeout: str(h.timeout), retries: typeof h.retries === 'number' ? h.retries : undefined, start_period: str(h.start_period), disable: h.disable === true } : undefined,
    networks: Array.isArray(m.networks) ? m.networks.map(String) : isMap(m.networks) ? Object.keys(m.networks) : [],
    restart: str(m.restart),
  }
}

function envMap(raw: YamlValue | undefined): Record<string, string> {
  if (Array.isArray(raw)) {
    const out: Record<string, string> = {}
    for (const item of raw) {
      if (typeof item !== 'string') continue
      const eq = item.indexOf('=')
      out[eq < 0 ? item : item.slice(0, eq)] = eq < 0 ? '' : item.slice(eq + 1)
    }
    return out
  }
  return isMap(raw) ? Object.fromEntries(Object.entries(raw).map(([k, v]) => [k, v == null ? '' : String(v)])) : {}
}

function list(v: YamlValue | undefined): string[] { return Array.isArray(v) ? v.map(String) : [] }
function str(v: YamlValue | undefined): string | undefined { return typeof v === 'string' ? v : undefined }
function asMap(v: YamlValue | undefined): Record<string, YamlValue> { return isMap(v) ? v : {} }
function isMap(v: YamlValue | undefined): v is { [k: string]: YamlValue } { return v !== null && v !== undefined && typeof v === 'object' && !Array.isArray(v) }
function norm(context: string): string { return context.replace(/^\.\//, '').replace(/\/$/, '') || '.' }

function splitKey(text: string): { key: string; rest: string } {
  const m = /^(.+?):\s*(.*)$/.exec(text)
  if (!m) throw new Error(`missing ':' in ${text}`)
  return { key: unquote(m[1]!.trim()), rest: m[2] ?? '' }
}
function inline(input: string): YamlValue {
  const s = input.trim()
  return s.startsWith('[') ? flow(s) : scalar(s)
}
function flow(s: string): YamlValue[] {
  if (!s.endsWith(']')) throw new Error('unclosed [')
  const body = s.slice(1, -1).trim()
  if (body === '') return []
  const arr: YamlValue[] = []
  let cur = ''
  let q: string | null = null
  const push = () => {
    const t = cur.trim()
    if (t !== '' || q !== null) arr.push(scalar(t))
    cur = ''
    q = null
  }
  for (let i = 0; i < body.length; i++) {
    const c = body[i]!
    if (q) {
      if (c === q) q = null
      else cur += c
      continue
    }
    if (c === '"' || c === "'") q = c
    else if (c === ',') push()
    else cur += c
  }
  push()
  return arr
}
function scalar(s: string): YamlValue {
  if (s === '~' || s === 'null' || s === '') return null
  if (s === 'true' || s === 'yes') return true
  if (s === 'false' || s === 'no') return false
  if (/^-?\d+$/.test(s)) return Number(s)
  return unquote(s)
}
function unquote(s: string): string {
  return (s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'")) ? s.slice(1, -1) : s
}
