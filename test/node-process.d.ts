declare const process: {
  env: Record<string, string | undefined>
  stdout: { write(chunk: string): boolean }
  exit(code?: number): void
  on(event: string, listener: (signal: string) => void): void
}

declare function setInterval(handler: () => void, timeout: number): unknown
declare function clearInterval(id: unknown): void
