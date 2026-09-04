import type { ServiceName } from './contracts.js'

export type Language = 'go' | 'typescript' | 'python'
export type ServiceImage = {
  service: ServiceName
  language: Language
  dockerfile: string
  dockerignore: string
  context: 'service' | 'repo'
  http: boolean
}

export const SERVICE_IMAGES: readonly ServiceImage[] = [
  { service: 'gateway', language: 'go', dockerfile: 'services/gateway/Dockerfile', dockerignore: 'services/gateway/.dockerignore', context: 'service', http: true },
  { service: 'orders', language: 'typescript', dockerfile: 'services/orders/Dockerfile', dockerignore: '.dockerignore', context: 'repo', http: false },
  { service: 'payments', language: 'typescript', dockerfile: 'services/payments/Dockerfile', dockerignore: '.dockerignore', context: 'repo', http: false },
  { service: 'inventory', language: 'typescript', dockerfile: 'services/inventory/Dockerfile', dockerignore: '.dockerignore', context: 'repo', http: false },
  { service: 'notifications', language: 'python', dockerfile: 'services/notifications/Dockerfile', dockerignore: '.dockerignore', context: 'repo', http: false },
]

export type Instruction = { keyword: string; args: string; line: number }
export type Stage = { index: number; name: string | null; image: string; instructions: Instruction[] }
export type ParsedDockerfile = { instructions: Instruction[]; preamble: Instruction[]; stages: Stage[] }
export type IgnorePattern = { negated: boolean; regex: RegExp }
export type Finding = { code: string; message: string }

export function parseDockerfile(source: string): ParsedDockerfile {
  const instructions = splitInstructions(source)
  const preamble: Instruction[] = []
  const stages: Stage[] = []
  for (const ins of instructions) {
    if (ins.keyword === 'FROM') {
      stages.push({ index: stages.length, ...parseFrom(ins.args), instructions: [] })
    } else if (stages.length === 0) preamble.push(ins)
    else stages[stages.length - 1]!.instructions.push(ins)
  }
  return { instructions, preamble, stages }
}

export function parseDockerignore(text: string): IgnorePattern[] {
  const out: IgnorePattern[] = []
  for (const line0 of text.split(/\r?\n/)) {
    const line = line0.trim()
    if (!line || line.startsWith('#')) continue
    let body = line
    let negated = false
    if (body.startsWith('!')) {
      negated = true
      body = body.slice(1)
    }
    if (body.endsWith('/')) body = body.slice(0, -1)
    out.push({ negated, regex: globToRegExp(body) })
  }
  return out
}

export function isDockerignored(relPath: string, patterns: IgnorePattern[]): boolean {
  const path = relPath.replaceAll('\\', '/').replace(/^\.\//, '')
  if (path === '' || path === '.') return false
  let ignored = false
  for (const p of patterns) {
    if (p.regex.test(path)) ignored = !p.negated
  }
  return ignored
}

export function checkImagePolicy(source: string, opts: { language: Language; http: boolean }): Finding[] {
  const parsed = parseDockerfile(source)
  const findings: Finding[] = []
  if (parsed.stages.length < 2) findings.push({ code: 'multi-stage', message: 'need a build stage and a runtime stage' })
  const final = parsed.stages.at(-1)
  if (!final) {
    findings.push({ code: 'from', message: 'missing FROM' })
    return findings
  }
  const user = resolveUser(parsed, final)
  if (user === null || user === '') findings.push({ code: 'user', message: 'final stage must set USER' })
  else if (isRootUser(user)) findings.push({ code: 'user', message: `final USER ${user} is root` })
  if (parsed.instructions.some((i) => i.keyword === 'ADD')) findings.push({ code: 'add', message: 'use COPY instead of ADD' })
  if (!final.instructions.some((i) => i.keyword === 'COPY' && /(^|\s)--from=/.test(i.args))) {
    findings.push({ code: 'copy-from', message: 'runtime stage must COPY --from a build stage' })
  }
  if (opts.http && ![...parsed.preamble, ...final.instructions].some((i) => i.keyword === 'EXPOSE')) {
    findings.push({ code: 'expose', message: 'HTTP image must EXPOSE a port' })
  }
  if (opts.language === 'go' && /^golang(?:$|:)/i.test(final.image)) {
    findings.push({ code: 'runtime-base', message: 'Go runtime stage still uses the compiler image' })
  }
  return findings
}

export function checkDockerignore(text: string, samples: readonly string[]): Finding[] {
  const patterns = parseDockerignore(text)
  const findings: Finding[] = []
  if (patterns.length === 0) findings.push({ code: 'empty', message: '.dockerignore is empty' })
  for (const sample of samples) {
    if (!isDockerignored(sample, patterns)) {
      findings.push({ code: 'include', message: `${sample} should be excluded from the build context` })
    }
  }
  return findings
}

function splitInstructions(source: string): Instruction[] {
  const lines = source.split(/\r?\n/)
  const logical: { text: string; line: number }[] = []
  let buf = ''
  let start = 1
  for (let i = 0; i < lines.length; i++) {
    const n = i + 1
    let line = lines[i] ?? ''
    const trimmed = line.trim()
    if (!buf && (trimmed === '' || trimmed.startsWith('#'))) continue
    if (!buf) start = n
    const cont = /\\[ \t]*$/.test(line)
    if (cont) line = line.replace(/\\[ \t]*$/, '')
    buf += (buf ? ' ' : '') + line.trim()
    if (!cont) {
      logical.push({ text: buf, line: start })
      buf = ''
    }
  }
  if (buf) logical.push({ text: buf, line: start })
  const out: Instruction[] = []
  for (const { text, line } of logical) {
    const sp = text.search(/\s/)
    const keyword = (sp < 0 ? text : text.slice(0, sp)).toUpperCase()
    if (!keyword) continue
    out.push({ keyword, args: sp < 0 ? '' : text.slice(sp).trim(), line })
  }
  return out
}

function parseFrom(args: string): { name: string | null; image: string } {
  const tokens = args.split(/\s+/).filter((t) => t && !t.startsWith('--'))
  const asIdx = tokens.findIndex((t) => t.toUpperCase() === 'AS')
  return { image: tokens[0] ?? '', name: asIdx >= 0 ? (tokens[asIdx + 1] ?? null) : null }
}

function resolveUser(parsed: ParsedDockerfile, final: Stage): string | null {
  const vars: Record<string, string> = {}
  applyArgs(vars, parsed.preamble, false)
  applyArgs(vars, final.instructions, true)
  let user: string | null = null
  for (const ins of final.instructions) {
    if (ins.keyword === 'USER') user = expand(ins.args.split(/\s+/)[0] ?? '', vars)
  }
  return user
}

function applyArgs(vars: Record<string, string>, inst: Instruction[], inheritEmpty: boolean): void {
  for (const ins of inst) {
    if (ins.keyword !== 'ARG' && ins.keyword !== 'ENV') continue
    const eq = ins.args.indexOf('=')
    if (eq < 0) {
      const name = ins.args.trim()
      if (inheritEmpty && name && vars[name] === undefined) vars[name] = ''
      continue
    }
    const name = ins.args.slice(0, eq).trim()
    const value = stripQuotes(ins.args.slice(eq + 1).trim())
    if (name) vars[name] = expand(value, vars)
  }
}

function expand(value: string, vars: Record<string, string>): string {
  return value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (_, brace: string | undefined, bare: string | undefined) => vars[brace ?? bare ?? ''] ?? '')
}

function stripQuotes(value: string): string {
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) return value.slice(1, -1)
  return value
}

function isRootUser(user: string): boolean {
  const id = (user.split(':')[0] ?? user).toLowerCase()
  return id === '0' || id === 'root'
}

function globToRegExp(glob: string): RegExp {
  const anchored = glob.startsWith('/')
  const g = anchored ? glob.slice(1) : glob
  let re = anchored || g.includes('/') ? '^' : '(?:^|.*/)'
  for (let i = 0; i < g.length; ) {
    if (g.startsWith('**/', i)) {
      re += '(?:.*/)?'
      i += 3
      continue
    }
    if (g.startsWith('**', i) && i + 2 === g.length) {
      re += '.*'
      i += 2
      continue
    }
    const c = g[i]!
    if (c === '*') re += '[^/]*'
    else if (c === '?') re += '[^/]'
    else if ('\\^$+{}[]()|.'.includes(c)) re += `\\${c}`
    else re += c
    i++
  }
  return new RegExp(`${re}(?:/.*)?$`)
}
