import {
  connect,
  RetentionPolicy,
  StorageType,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
  type Subscription,
} from 'nats'
import { EventEmitter } from 'node:events'

const STREAM = 'DEMO'
const STREAM_SUBJECTS = ['demo.events.>', 'demo.quarkus.ticks']

const te = new TextEncoder()
const td = new TextDecoder()

export interface ReceivedMessage {
  kind: 'message'
  subscriptionId: string
  subject: string
  queue?: string
  replyTo?: string
  payload: string
  receivedAt: string
}

export interface SubscriptionInfo {
  id: string
  subject: string
  queue?: string
  createdAt: string
  received: number
}

interface ManagedSubscription extends SubscriptionInfo {
  sub: Subscription
}

/** Holds the NATS connection, the dynamic subscription registry and JetStream helpers. */
export class NatsBridge extends EventEmitter {
  private nc!: NatsConnection
  private js!: JetStreamClient
  private jsm!: JetStreamManager
  private readonly subscriptions = new Map<string, ManagedSubscription>()
  private nextId = 1

  async start(url: string): Promise<void> {
    this.nc = await connect({ servers: url, maxReconnectAttempts: -1 })
    console.log(`connected to NATS at ${url}`)

    this.jsm = await this.nc.jetstreamManager()
    this.js = this.nc.jetstream()
    await this.ensureStream()
    this.registerResponders()
  }

  /** Idempotently create the DEMO stream (file storage, limits retention). */
  private async ensureStream(): Promise<void> {
    try {
      await this.jsm.streams.info(STREAM)
      console.log(`stream ${STREAM} already exists`)
    } catch {
      await this.jsm.streams.add({
        name: STREAM,
        subjects: STREAM_SUBJECTS,
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        max_msgs: 10_000,
      })
      console.log(`created stream ${STREAM} (subjects: ${STREAM_SUBJECTS.join(', ')})`)
    }
  }

  /** Core-NATS responders this service contributes to the demo. */
  private registerResponders(): void {
    // Request/reply: uppercase echo.
    const echo = this.nc.subscribe('demo.echo')
    void (async () => {
      for await (const msg of echo) {
        const text = td.decode(msg.data)
        msg.respond(te.encode(`ECHO: ${text.toUpperCase()} — from node-service`))
      }
    })()

    // Queue group "workers" on demo.work — load-balanced with quarkus-service.
    const work = this.nc.subscribe('demo.work', { queue: 'workers' })
    void (async () => {
      for await (const msg of work) {
        const text = td.decode(msg.data)
        const reply = `work item '${text}' processed by node-service at ${new Date().toISOString()}`
        if (msg.reply) msg.respond(te.encode(reply))
        console.log(`processed work item from queue group: ${text}`)
      }
    })()
  }

  /** Dynamic subscription requested from the UI; messages stream out via the 'message' event. */
  subscribe(subject: string, queue?: string): SubscriptionInfo {
    const id = String(this.nextId++)
    const sub = this.nc.subscribe(subject, queue ? { queue } : {})
    const managed: ManagedSubscription = {
      id,
      subject,
      sub,
      createdAt: new Date().toISOString(),
      received: 0,
      ...(queue ? { queue } : {}),
    }
    this.subscriptions.set(id, managed)

    void (async () => {
      for await (const msg of sub) {
        managed.received++
        const event: ReceivedMessage = {
          kind: 'message',
          subscriptionId: id,
          subject: msg.subject,
          payload: td.decode(msg.data),
          receivedAt: new Date().toISOString(),
          ...(managed.queue ? { queue: managed.queue } : {}),
          ...(msg.reply ? { replyTo: msg.reply } : {}),
        }
        this.emit('message', event)
      }
    })()

    return toInfo(managed)
  }

  unsubscribe(id: string): boolean {
    const managed = this.subscriptions.get(id)
    if (!managed) return false
    managed.sub.unsubscribe()
    this.subscriptions.delete(id)
    return true
  }

  listSubscriptions(): SubscriptionInfo[] {
    return [...this.subscriptions.values()].map(toInfo)
  }

  /** Publish on core NATS, or through JetStream (persisted, acked) when requested. */
  async publish(subject: string, payload: string, jetstream: boolean): Promise<{ seq?: number }> {
    if (jetstream) {
      const ack = await this.js.publish(subject, te.encode(payload))
      return { seq: ack.seq }
    }
    this.nc.publish(subject, te.encode(payload))
    return {}
  }

  /** Request/reply with a timeout; returns the reply payload. */
  async request(subject: string, payload: string, timeoutMs = 5000): Promise<string> {
    const reply = await this.nc.request(subject, te.encode(payload), { timeout: timeoutMs })
    return td.decode(reply.data)
  }

  async streamInfo(): Promise<unknown> {
    const info = await this.jsm.streams.info(STREAM)
    const consumers = await this.jsm.consumers.list(STREAM).next()
    return {
      stream: info.config.name,
      subjects: info.config.subjects,
      storage: info.config.storage,
      messages: info.state.messages,
      firstSeq: info.state.first_seq,
      lastSeq: info.state.last_seq,
      consumers: consumers.map((c) => ({
        name: c.name,
        durable: c.config.durable_name,
        delivered: c.delivered.stream_seq,
        pending: c.num_pending,
      })),
    }
  }

  /** Read back the most recent persisted messages straight from the stream. */
  async lastMessages(limit: number): Promise<Array<{ seq: number; subject: string; payload: string; time: string }>> {
    const info = await this.jsm.streams.info(STREAM)
    const { first_seq: first, last_seq: last } = info.state
    const out: Array<{ seq: number; subject: string; payload: string; time: string }> = []
    for (let seq = last; seq >= first && out.length < limit; seq--) {
      try {
        const m = await this.jsm.streams.getMessage(STREAM, { seq })
        out.push({ seq: m.seq, subject: m.subject, payload: td.decode(m.data), time: m.time.toISOString() })
      } catch {
        // sequence gap (deleted message) — skip
      }
    }
    return out
  }

  status(): { connected: boolean; server: string } {
    return {
      connected: !this.nc.isClosed(),
      server: this.nc.getServer(),
    }
  }

  async stop(): Promise<void> {
    await this.nc.drain()
  }
}

function toInfo(m: ManagedSubscription): SubscriptionInfo {
  const { sub: _sub, ...info } = m
  return info
}
