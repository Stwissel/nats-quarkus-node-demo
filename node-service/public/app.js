// Web UI logic: talks to the node-service REST API and listens on its SSE feed.
const $ = (id) => document.getElementById(id);

const api = async (path, options) => {
  const res = await fetch(path, options);
  const body = await res.json();
  if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
  return body;
};

const post = (path, body) =>
  api(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });

// --- Live log -------------------------------------------------------------
let logCount = 0;

function tagVariantFor(subject) {
  if (subject.startsWith('demo.quarkus.')) return 'success';
  if (subject.startsWith('demo.events.')) return 'brand';
  return 'neutral';
}

function addLogEntry({ subject, payload, queue, note }) {
  const entry = document.createElement('div');
  entry.className = 'log-entry wa-cluster wa-gap-xs';

  const time = document.createElement('time');
  time.dateTime = new Date().toISOString();
  time.textContent = new Date().toLocaleTimeString();
  entry.append(time);

  const tag = document.createElement('wa-tag');
  tag.size = 'small';
  tag.variant = note ? 'warning' : tagVariantFor(subject ?? '');
  tag.textContent = note ? 'info' : subject;
  entry.append(tag);

  if (queue) {
    const q = document.createElement('wa-tag');
    q.size = 'small';
    q.appearance = 'outlined';
    q.textContent = `queue: ${queue}`;
    entry.append(q);
  }

  const text = document.createElement('span');
  text.className = 'mono';
  text.textContent = note ?? payload;
  entry.append(text);

  const log = $('log');
  log.prepend(entry);
  while (log.children.length > 200) log.lastElementChild.remove();
  $('log-count').textContent = String(++logCount);
}

// --- SSE feed --------------------------------------------------------------
const events = new EventSource('/api/events');
events.onmessage = (e) => {
  const event = JSON.parse(e.data);
  if (event.kind === 'message') {
    addLogEntry(event);
  } else if (event.kind === 'subscribed') {
    addLogEntry({ note: `subscribed #${event.id} to ${event.subject}${event.queue ? ` (queue ${event.queue})` : ''}` });
    void refreshSubscriptions();
  } else if (event.kind === 'unsubscribed') {
    addLogEntry({ note: `unsubscribed #${event.id}` });
    void refreshSubscriptions();
  }
};
events.onopen = () => setConnBadge(true);
events.onerror = () => setConnBadge(false);

function setConnBadge(up) {
  const badge = $('conn-badge');
  badge.variant = up ? 'success' : 'danger';
  badge.textContent = up ? 'connected' : 'disconnected';
}

// --- Subscriptions ---------------------------------------------------------
async function refreshSubscriptions() {
  const subs = await api('/api/subscriptions');
  const list = $('sub-list');
  list.replaceChildren();
  if (subs.length === 0) {
    const empty = document.createElement('small');
    empty.className = 'wa-color-text-quiet';
    empty.textContent = 'No active subscriptions.';
    list.append(empty);
    return;
  }
  for (const sub of subs) {
    const tag = document.createElement('wa-tag');
    tag.variant = 'brand';
    tag.withRemove = true;
    tag.textContent = `#${sub.id} ${sub.subject}${sub.queue ? ` (${sub.queue})` : ''} · ${sub.received}`;
    tag.addEventListener('wa-remove', () => {
      void api(`/api/subscriptions/${sub.id}`, { method: 'DELETE' }).then(refreshSubscriptions);
    });
    list.append(tag);
  }
}

$('sub-add').addEventListener('click', async () => {
  const subject = $('sub-subject').value.trim();
  if (!subject) return;
  const queue = $('sub-queue').value.trim();
  await post('/api/subscriptions', { subject, ...(queue ? { queue } : {}) });
  await refreshSubscriptions();
});

// --- Publish ---------------------------------------------------------------
$('pub-send').addEventListener('click', async () => {
  const out = $('pub-result');
  try {
    const result = await post('/api/publish', {
      subject: $('pub-subject').value.trim(),
      payload: $('pub-payload').value,
      jetstream: $('pub-js').checked,
    });
    out.textContent = result.seq !== undefined ? `persisted as sequence ${result.seq}` : 'published (core NATS, fire-and-forget)';
  } catch (err) {
    out.textContent = String(err.message ?? err);
  }
});

// --- Request / reply -------------------------------------------------------
function replyCallout(variant, text) {
  const callout = document.createElement('wa-callout');
  callout.variant = variant;
  callout.size = 'small';
  const span = document.createElement('span');
  span.className = 'mono';
  span.textContent = text;
  callout.append(span);
  return callout;
}

$('req-send').addEventListener('click', async () => {
  const out = $('req-result');
  out.replaceChildren();
  try {
    const result = await post('/api/request', {
      subject: $('req-subject').value,
      payload: $('req-payload').value,
    });
    out.append(replyCallout('success', `${result.reply} (${result.latencyMs} ms)`));
  } catch (err) {
    out.append(replyCallout('danger', String(err.message ?? err)));
  }
});

$('req-burst').addEventListener('click', async () => {
  const out = $('req-result');
  out.replaceChildren();
  const results = await Promise.all(
    Array.from({ length: 6 }, (_, i) =>
      post('/api/request', { subject: 'demo.work', payload: `job-${i + 1}` }).catch((err) => ({ reply: String(err.message ?? err) })),
    ),
  );
  const byService = { 'quarkus-service': 0, 'node-service': 0 };
  for (const r of results) {
    if (r.reply.includes('quarkus-service')) byService['quarkus-service']++;
    if (r.reply.includes('node-service')) byService['node-service']++;
    out.append(replyCallout('neutral', r.reply));
  }
  out.prepend(
    replyCallout('success', `queue group split — quarkus-service: ${byService['quarkus-service']}, node-service: ${byService['node-service']}`),
  );
});

// --- JetStream -------------------------------------------------------------
async function refreshStream() {
  try {
    const info = await api('/api/jetstream');
    const stats = $('js-stats');
    stats.replaceChildren();
    const rows = [
      ['Subjects', info.subjects.join(', ')],
      ['Messages', String(info.messages)],
      ['Sequence range', `${info.firstSeq} – ${info.lastSeq}`],
      ['Consumers', info.consumers.map((c) => c.durable ?? c.name).join(', ') || 'none'],
    ];
    for (const [label, value] of rows) {
      const line = document.createElement('div');
      line.className = 'stat-line wa-split';
      const dt = document.createElement('dt');
      dt.textContent = label;
      const dd = document.createElement('dd');
      dd.textContent = value;
      line.append(dt, dd);
      stats.append(line);
    }
  } catch {
    // stream not ready yet
  }
}

$('js-refresh').addEventListener('click', refreshStream);

$('js-replay').addEventListener('click', async () => {
  const out = $('js-replay-out');
  out.replaceChildren();
  const messages = await api('/api/jetstream/messages?limit=10');
  if (messages.length === 0) {
    out.append(replyCallout('neutral', 'The stream is empty — publish something with JetStream enabled.'));
    return;
  }
  for (const m of messages) {
    out.append(replyCallout('neutral', `[seq ${m.seq}] ${m.subject}: ${m.payload}`));
  }
});

// --- Init ------------------------------------------------------------------
void refreshSubscriptions();
void refreshStream();
setInterval(refreshStream, 10_000);
