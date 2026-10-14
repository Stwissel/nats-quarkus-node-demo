import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { readFile } from 'node:fs/promises'
import { extname, join, normalize } from 'node:path'
import { fileURLToPath } from 'node:url'
import { NatsBridge, type ReceivedMessage } from './nats.js'

const PORT = Number(process.env['PORT'] ?? 8080)
const NATS_URL = process.env['NATS_URL'] ?? 'nats://localhost:4222'
const PUBLIC_DIR = join(fileURLToPath(new URL('.', import.meta.url)), '..', 'public')

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
}

const bridge = new NatsBridge()
const sseClients = new Set<ServerResponse>()

bridge.on('message', (event: ReceivedMessage) => {
  broadcast(event)
})

function broadcast(event: object): void {
  const frame = `data: ${JSON.stringify(event)}\n\n`
  for (const res of sseClients) res.write(frame)
}

/** A client mistake; the request handler turns this into a 400 rather than a 500. */
class BadRequestError extends Error {}

async function readBody(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  const raw = Buffer.concat(chunks).toString('utf8')
  if (!raw) return {}
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    throw new BadRequestError('malformed JSON body')
  }
  // 'null' parses fine but the handlers index into the result, so it threw a
  // TypeError and surfaced as a 500. Anything that is not an object is a client
  // mistake; quarkus-service already answers 400 for these.
  if (parsed === null || typeof parsed !== 'object') {
    throw new BadRequestError('request body must be a JSON object')
  }
  return parsed as Record<string, unknown>
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const data = JSON.stringify(body)
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' })
  res.end(data)
}

async function serveStatic(res: ServerResponse, urlPath: string): Promise<void> {
  const safePath = normalize(urlPath === '/' ? '/index.html' : urlPath).replace(/^(\.\.[/\\])+/, '')
  try {
    const file = await readFile(join(PUBLIC_DIR, safePath))
    res.writeHead(200, { 'content-type': MIME[extname(safePath)] ?? 'application/octet-stream' })
    res.end(file)
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain' })
    res.end('not found')
  }
}

async function handleApi(req: IncomingMessage, res: ServerResponse, path: string): Promise<void> {
  const method = req.method ?? 'GET'

  if (path === '/api/health' && method === 'GET') {
    sendJson(res, 200, { status: 'UP', service: 'node-service', nats: bridge.status() })
    return
  }

  if (path === '/api/events' && method === 'GET') {
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    })
    res.write(`data: ${JSON.stringify({ kind: 'hello', at: new Date().toISOString() })}\n\n`)
    sseClients.add(res)
    const heartbeat = setInterval(() => res.write(': keep-alive\n\n'), 15_000)
    req.on('close', () => {
      clearInterval(heartbeat)
      sseClients.delete(res)
    })
    return
  }

  if (path === '/api/subscriptions' && method === 'GET') {
    sendJson(res, 200, bridge.listSubscriptions())
    return
  }

  if (path === '/api/subscriptions' && method === 'POST') {
    const body = await readBody(req)
    const subject = String(body['subject'] ?? '').trim()
    const queue = body['queue'] ? String(body['queue']).trim() : undefined
    if (!subject) {
      sendJson(res, 400, { error: 'subject is required' })
      return
    }
    const info = bridge.subscribe(subject, queue)
    broadcast({ kind: 'subscribed', ...info })
    sendJson(res, 201, info)
    return
  }

  const unsubMatch = path.match(/^\/api\/subscriptions\/([^/]+)$/)
  if (unsubMatch && method === 'DELETE') {
    const id = unsubMatch[1] ?? ''
    if (bridge.unsubscribe(id)) {
      broadcast({ kind: 'unsubscribed', id })
      sendJson(res, 200, { id, unsubscribed: true })
    } else {
      sendJson(res, 404, { error: `no subscription with id ${id}` })
    }
    return
  }

  if (path === '/api/publish' && method === 'POST') {
    const body = await readBody(req)
    const subject = String(body['subject'] ?? '').trim()
    const payload = String(body['payload'] ?? '')
    const jetstream = Boolean(body['jetstream'])
    if (!subject) {
      sendJson(res, 400, { error: 'subject is required' })
      return
    }
    try {
      const result = await bridge.publish(subject, payload, jetstream)
      sendJson(res, 200, { subject, jetstream, ...result })
    } catch (err) {
      sendJson(res, 502, { error: String(err) })
    }
    return
  }

  if (path === '/api/request' && method === 'POST') {
    const body = await readBody(req)
    const subject = String(body['subject'] ?? '').trim()
    const payload = String(body['payload'] ?? '')
    if (!subject) {
      sendJson(res, 400, { error: 'subject is required' })
      return
    }
    const started = performance.now()
    try {
      const reply = await bridge.request(subject, payload)
      sendJson(res, 200, { subject, reply, latencyMs: Math.round(performance.now() - started) })
    } catch (err) {
      sendJson(res, 504, { error: `no reply: ${String(err)}` })
    }
    return
  }

  if (path === '/api/jetstream' && method === 'GET') {
    try {
      sendJson(res, 200, await bridge.streamInfo())
    } catch (err) {
      sendJson(res, 502, { error: String(err) })
    }
    return
  }

  if (path === '/api/jetstream/messages' && method === 'GET') {
    const url = new URL(path + (req.url?.includes('?') ? req.url.slice(req.url.indexOf('?')) : ''), 'http://x')
    const limit = Math.min(Number(url.searchParams.get('limit') ?? 10), 50)
    try {
      sendJson(res, 200, await bridge.lastMessages(limit))
    } catch (err) {
      sendJson(res, 502, { error: String(err) })
    }
    return
  }

  sendJson(res, 404, { error: 'unknown endpoint' })
}

async function main(): Promise<void> {
  await bridge.start(NATS_URL)

  const server = createServer((req, res) => {
    const path = (req.url ?? '/').split('?')[0] ?? '/'
    if (path.startsWith('/api/')) {
      handleApi(req, res, path).catch((err) => {
        console.error('api error:', err)
        if (res.headersSent) return
        if (err instanceof BadRequestError) sendJson(res, 400, { error: err.message })
        else sendJson(res, 500, { error: String(err) })
      })
    } else {
      void serveStatic(res, path)
    }
  })

  server.listen(PORT, () => console.log(`node-service listening on :${PORT}, UI at /`))

  const shutdown = async (): Promise<void> => {
    server.close()
    await bridge.stop()
    process.exit(0)
  }
  process.on('SIGTERM', () => void shutdown())
  process.on('SIGINT', () => void shutdown())
}

main().catch((err) => {
  console.error('fatal:', err)
  process.exit(1)
})
