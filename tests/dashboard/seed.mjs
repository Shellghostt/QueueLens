// Synthetic browser fixture. Requires an explicitly disposable local broker.
import { createRequire } from 'node:module'
import { performance } from 'node:perf_hooks'
import { setTimeout as sleep } from 'node:timers/promises'

const require = createRequire(import.meta.url)
const { Queue, Worker } = require('../integration/bullmq/node_modules/bullmq')
const Redis = require('../integration/bullmq/node_modules/ioredis')
const endpoint = process.env.QUEUELENS_DEMO_URL
const redisURL = process.env.QUEUELENS_DEMO_REDIS_URL
const token = process.env.QUEUELENS_ADMIN_TOKEN
if (!endpoint || !redisURL || !token || process.env.QUEUELENS_DEMO_CONFIRM_DISPOSABLE !== '1') {
  throw new Error('Set QUEUELENS_DEMO_URL, QUEUELENS_DEMO_REDIS_URL, QUEUELENS_ADMIN_TOKEN and QUEUELENS_DEMO_CONFIRM_DISPOSABLE=1 for a disposable local fixture.')
}
if (![endpoint, redisURL].every(value => ['127.0.0.1', 'localhost', '[::1]'].includes(new URL(value).hostname))) throw new Error('Browser fixture requires loopback services.')
const connection = new Redis(redisURL, { maxRetriesPerRequest: null })
const events = []
const run = String(Date.now())
const names = ['emails', 'reports', 'thumbnails', 'invoices', 'jobs/failed % 雪']
let sequence = 0
function fact(queue, job, attempt, state, details = {}, at = new Date()) {
  events.push({ v: 1, id: `browser-${run}-${sequence++}`, ts: at.toISOString(), queue, job_id: job, attempt, state, ...details })
}
async function post(body) {
  const response = await fetch(`${endpoint}/v1/events`, { method: 'POST', signal: AbortSignal.timeout(10000),
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  const ack = await response.json()
  if (response.status !== 202 || ack.accepted !== body.length || ack.dropped || ack.rejected.length) throw new Error('Synthetic events were not fully accepted.')
}
try {
  for (let q = 0; q < names.length; q++) {
    const name = names[q]
    const queue = new Queue(name, { connection })
    const workerID = `browser-worker-${q + 1}`
    const worker = new Worker(name, async job => {
      const started = performance.now()
      fact(name, job.id, 1, 'started', { worker_id: workerID, job_name: job.name })
      await sleep(3)
      const duration_ms = Math.round(performance.now() - started)
      if (job.data.fail) {
        fact(name, job.id, 1, 'failed', { worker_id: workerID, duration_ms, error: 'Synthetic SMTP timeout: <img src=x onerror=alert(1)>', job_name: job.name })
        throw new Error('Synthetic SMTP timeout')
      }
      fact(name, job.id, 1, 'completed', { worker_id: workerID, duration_ms, job_name: job.name })
      return 'fixture completed'
    }, { connection, concurrency: 4 })
    const terminal = await queue.addBulk(Array.from({ length: 24 }, (_, i) => {
      const id = `processed-${run}-${q}-${i}`
      fact(name, id, 1, 'enqueued', { job_name: 'send-notification' })
      return { name: 'send-notification', data: { fail: i % 8 === 0 }, opts: { jobId: id } }
    }))
    for (let i = 0; i < 100; i++) {
      const states = await Promise.all(terminal.map(job => job.getState()))
      if (states.every(state => state === 'completed' || state === 'failed')) break
      if (i === 99) throw new Error('Synthetic worker did not finish.')
      await sleep(100)
    }
    await worker.close()
    await queue.addBulk(Array.from({ length: 140 - q * 21 }, (_, i) => {
      const id = `waiting-${run}-${q}-${String(i).padStart(3, '0')}`
      fact(name, id, 1, 'enqueued', { job_name: 'pending-notification' })
      return { name: 'pending-notification', data: {}, opts: { jobId: id, delay: i % 9 === 0 ? 3600000 : undefined } }
    }))
    const heartbeat = await fetch(`${endpoint}/v1/workers/heartbeat`, { method: 'POST', signal: AbortSignal.timeout(10000),
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ v: 1, worker_id: workerID, pool: 'synthetic-browser-fixture' }) })
    if (heartbeat.status !== 202) throw new Error('Synthetic heartbeat not accepted.')
    await queue.close()
  }
  const at = new Date(Date.now() - 60000)
  for (let attempt = 1; attempt <= 205; attempt++) fact('invoices', 'invoice-retry-history', attempt, 'failed',
    { error: 'Synthetic gateway timeout — duration unknown', job_name: 'settle-invoice' }, at)
  fact('invoices', 'invoice-retry-history', 205, 'completed', { duration_ms: 42, job_name: 'settle-invoice' }, new Date(at.getTime() + 1000))
  fact('invoices', 'invoice-retry-history', 205, 'enqueued', { job_name: 'settle-invoice' }, new Date(at.getTime() + 2000))
  fact('jobs/failed % 雪', '<img src=x onerror=alert(1)>', 1, 'failed', { error: '<script>window.injected=true</script>', job_name: '<svg onload=alert(1)>' }, at)
  for (let i = 0; i < events.length; i += 400) await post(events.slice(i, i + 400))
  for (let i = 0; i < 60; i++) {
    const response = await fetch(`${endpoint}/v1/dashboard/job?queue=invoices&job_id=invoice-retry-history`, { headers: { Authorization: `Bearer ${token}` } })
    const data = await response.json()
    if (data.job?.summary?.observed_attempts === 205) {
      console.log(`Seeded ${names.length} actual BullMQ queues and ${events.length} stored synthetic lifecycle facts. Timeline: invoices / invoice-retry-history (205 attempts).`)
      break
    }
    if (i === 59) throw new Error('Synthetic history did not project before deadline.')
    await sleep(250)
  }
} finally { await connection.quit() }
