declare module 'node:fs' {
  export function existsSync(path: string): boolean
  export function readFileSync(path: string, encoding: string): string
  export function writeFileSync(path: string, data: string): void
  export function unlinkSync(path: string): void
}
