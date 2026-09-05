declare module 'node:child_process' {
  export function spawnSync(
    command: string,
    args: string[],
    options: { encoding: string },
  ): { status: number | null; stdout: string; stderr: string }
}