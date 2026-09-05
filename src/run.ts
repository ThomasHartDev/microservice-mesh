const service = process.env.MESH_SERVICE ?? 'unknown'
process.stdout.write(`${service} started\n`)

const keepAlive = setInterval(() => {}, 60_000)

const onStop = (signal: string): void => {
  process.stdout.write(`${service} stopping ${signal}\n`)
  clearInterval(keepAlive)
  process.exit(0)
}

process.on('SIGTERM', onStop)
process.on('SIGINT', onStop)

