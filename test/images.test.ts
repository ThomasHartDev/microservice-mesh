import { existsSync, readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import {
  SERVICE_IMAGES,
  checkDockerignore,
  checkImagePolicy,
  isDockerignored,
  parseDockerfile,
  parseDockerignore,
  type ServiceName,
} from '../src/index.js'

const ROOT_IGNORE = [
  '.git/config',
  'node_modules/vitest/index.js',
  'test/images.test.ts',
  'src/images.test.ts',
  'coverage/out.json',
  'dist/index.js',
  '.env',
  '.env.local',
  'services/gateway/gateway_test.go',
]

const load = (rel: string) => readFileSync(rel, 'utf8')
const ignored = (text: string, path: string) => isDockerignored(path, parseDockerignore(text))
const codes = (src: string, language: 'go' | 'typescript' | 'python', http: boolean) =>
  checkImagePolicy(src, { language, http }).map((f) => f.code)

function copyContextSources(args: string): string[] {
  const tokens = args.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) ?? []
  const paths: string[] = []
  for (const raw of tokens) {
    const token = raw.replace(/^['"]|['"]$/g, '')
    if (token === '--from' || token.startsWith('--from=')) return []
    if (token.startsWith('--')) continue
    paths.push(token)
  }
  return paths.slice(0, -1)
}

function contextPath(contextDir: string, src: string): string {
  const cleaned = src.replace(/^\.\//, '')
  if (cleaned === '' || cleaned === '.') return contextDir
  return contextDir === '.' ? cleaned : `${contextDir}/${cleaned}`
}

describe('image catalog', () => {
  it('covers every mesh service with a Dockerfile on disk', () => {
    const names: ServiceName[] = ['gateway', 'orders', 'inventory', 'payments', 'notifications']
    expect(SERVICE_IMAGES.map((s) => s.service).sort()).toEqual([...names].sort())
    expect(new Set(SERVICE_IMAGES.map((s) => s.dockerfile)).size).toBe(5)
    for (const img of SERVICE_IMAGES) {
      expect(checkImagePolicy(load(img.dockerfile), { language: img.language, http: img.http })).toEqual([])
      const samples = img.context === 'service' ? ['gateway_test.go', '.git/HEAD'] : ROOT_IGNORE
      expect(checkDockerignore(load(img.dockerignore), samples)).toEqual([])
      const parsed = parseDockerfile(load(img.dockerfile))
      expect(parsed.stages.length).toBeGreaterThanOrEqual(2)
      expect(parsed.stages[0]?.name).toBe('build')
      expect(parsed.stages.at(-1)?.instructions.find((i) => i.keyword === 'USER')?.args).toMatch(/^65532/)
      if (img.service === 'gateway') expect(load(img.dockerfile)).not.toContain('MESH_SERVICE')
      else expect(load(img.dockerfile)).toContain(`MESH_SERVICE=${img.service}`)
    }
  })

  it('COPY sources exist relative to each image build context', () => {
    for (const img of SERVICE_IMAGES) {
      const contextDir = img.context === 'service' ? img.dockerfile.replace(/\/Dockerfile$/, '') : '.'
      const parsed = parseDockerfile(load(img.dockerfile))
      const sources = parsed.instructions
        .filter((i) => i.keyword === 'COPY')
        .flatMap((i) => copyContextSources(i.args))
      expect(sources.length).toBeGreaterThan(0)
      for (const src of sources) {
        expect(existsSync(contextPath(contextDir, src)), `${img.dockerfile} COPY ${src}`).toBe(true)
      }
    }
  })
})

describe('Dockerfile parser and policy', () => {
  it('rejects empty, single-stage, root, ADD, and missing COPY --from', () => {
    expect(codes('', 'go', false)).toEqual(expect.arrayContaining(['from', 'multi-stage']))
    expect(codes('FROM golang:1.23\nUSER 65532\n', 'go', false).sort()).toEqual([
      'copy-from',
      'multi-stage',
      'runtime-base',
    ])
    const root = 'FROM golang:1.23 AS build\nFROM alpine\nCOPY --from=build /out/app /app\nUSER root'
    expect(codes(root, 'go', true).sort()).toEqual(['expose', 'user'])
    const zero = 'FROM golang:1.23 AS build\nFROM gcr.io/distroless/static-debian12\nCOPY --from=build /out/app /app\nUSER 0:0'
    expect(codes(zero, 'go', false)).toContain('user')
    const add = 'FROM node:20-alpine AS build\nFROM node:20-alpine\nADD app.tar /app\nCOPY --from=build /src/dist /app/dist\nUSER 65532'
    expect(codes(add, 'typescript', false)).toEqual(['add'])
  })

  it('expands ARG in USER and joins continued lines', () => {
    const src = [
      'ARG UID=65532',
      'FROM golang:1.23-alpine AS build',
      'RUN CGO_ENABLED=0 go build \\',
      '  -o /out/app .',
      'FROM gcr.io/distroless/static-debian12:nonroot',
      'ARG UID',
      'COPY --from=build /out/app /app',
      'USER ${UID}:65532',
      'EXPOSE 8080',
    ].join('\n')
    const parsed = parseDockerfile(src)
    expect(parsed.stages).toHaveLength(2)
    expect(parsed.stages[0]?.name).toBe('build')
    expect(parsed.stages[0]?.instructions[0]?.args).toContain('-o /out/app')
    expect(checkImagePolicy(src, { language: 'go', http: true })).toEqual([])
  })

  it('treats missing USER as a failure and ignores parser directives', () => {
    const src = '# syntax=docker/dockerfile:1\nFROM python:3.12-alpine AS build\nFROM python:3.12-alpine\nCOPY --from=build /src/worker.py /app/worker.py'
    expect(codes(src, 'python', false)).toEqual(['user'])
    expect(parseDockerfile(src).instructions.every((i) => i.keyword !== '#')).toBe(true)
  })

  it('does not leak ARG from a sibling stage into the runtime USER', () => {
    const src = 'FROM golang:1.23 AS build\nARG UID=65532\nFROM alpine\nCOPY --from=build /out/app /app\nUSER $UID'
    expect(codes(src, 'go', false)).toContain('user')
  })
})

describe('dockerignore matching', () => {
  it('applies last-match-wins negation and globstar', () => {
    const text = ['*.md', '!README.md', '**/*.test.ts', 'tmp/', '.git', 'secret'].join('\n')
    expect(ignored(text, 'docs/guide.md')).toBe(true)
    expect(ignored(text, 'README.md')).toBe(false)
    expect(ignored(text, 'src/foo.test.ts')).toBe(true)
    expect(ignored(text, 'src/foo.ts')).toBe(false)
    expect(ignored(text, 'tmp/cache')).toBe(true)
    expect(ignored(text, '.git/objects/pack')).toBe(true)
    expect(ignored(text, 'secret')).toBe(true)
    expect(ignored(text, 'src/secret')).toBe(true)
    expect(ignored('', 'anything')).toBe(false)
    expect(ignored('# only a comment\n', 'x')).toBe(false)
    expect(ignored('*.log\n', 'dir/app.log')).toBe(true)
    expect(ignored('.env.*\n!.env.example\n', '.env.local')).toBe(true)
    expect(ignored('.env.*\n!.env.example\n', '.env.example')).toBe(false)
    expect(checkDockerignore('', ['node_modules/x']).map((f) => f.code)).toEqual(['empty', 'include'])
  })

  it('anchors patterns that contain a slash to the context root', () => {
    const text = '/node_modules\nsrc/*.map\n'
    expect(ignored(text, 'node_modules/pkg')).toBe(true)
    expect(ignored(text, 'vendor/node_modules/pkg')).toBe(false)
    expect(ignored(text, 'src/index.js.map')).toBe(true)
    expect(ignored(text, 'lib/src/index.js.map')).toBe(false)
  })

  it('keeps gateway sources while ignoring the root binary, tests, and git', () => {
    const text = load('services/gateway/.dockerignore')
    expect(ignored(text, 'cmd/gateway/main.go')).toBe(false)
    expect(ignored(text, 'gateway.go')).toBe(false)
    expect(ignored(text, 'gateway')).toBe(true)
    expect(ignored(text, 'cmd/gateway/gateway')).toBe(true)
    expect(ignored(text, 'gateway_test.go')).toBe(true)
    expect(ignored(text, '.git')).toBe(true)
    expect(ignored(text, '.git/HEAD')).toBe(true)
  })
})
