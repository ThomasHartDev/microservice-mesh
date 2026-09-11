declare module 'node:http' {
  type IncomingMessage = { method?: string; url?: string }
  type ServerResponse = {
    writeHead(statusCode: number, headers?: Record<string, string>): void
    end(body?: string): void
  }
  type Server = {
    listen(port: number, listeningListener?: () => void): Server
    close(callback?: () => void): void
  }
  export function createServer(
    listener: (req: IncomingMessage, res: ServerResponse) => void,
  ): Server
}
