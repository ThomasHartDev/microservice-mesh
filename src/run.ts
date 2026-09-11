import { createServer } from 'node:http'
import { handleHealthRequest } from './observability.js'

const service = process.env.MESH_SERVICE ?? 'unknown'
const port = Number(process.env.HEALTH_PORT ?? '8081')
const nats = process.env.NATS_URL
const checks = [
  { name: 'process', ok: true },
  ...(nats !== undefined ? [{ name: 'nats', ok: nats.length > 0 }] : []),
]

const server = createServer((req, res) => {
  const out = handleHealthRequest(req.method ?? 'GET', req.url ?? '/', service, checks)
  res.writeHead(out.status, { 'content-type': 'application/json' })
  res.end(out.body)
})
server.listen(Number.isFinite(port) && port > 0 ? port : 8081)

process.stdout.write(`${service} started\n`)

const keepAlive = setInterval(() => {}, 60_000)

const onStop = (signal: string): void => {
  process.stdout.write(`${service} stopping ${signal}\n`)
  clearInterval(keepAlive)
  server.close()
  process.exit(0)
}

process.on('SIGTERM', onStop)
process.on('SIGINT', onStop)
