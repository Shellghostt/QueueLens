import type { DashboardData, Route, Session } from './types'

export const MAX_RESPONSE_BYTES = 2 * 1024 * 1024
export class APIError extends Error {
  readonly status: number
  constructor(status: number) { super(status === 401 ? 'Your dashboard session has expired.' : `Read unavailable (HTTP ${status}).`); this.status = status }
}

async function withDeadline<T>(parent: AbortSignal | undefined, action: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController()
  function cleanup() { clearTimeout(timer); parent?.removeEventListener('abort', abortParent) }
  function abortParent() { controller.abort(parent?.reason); cleanup() }
  const timer = setTimeout(() => { controller.abort(new DOMException('Dashboard request timed out.', 'TimeoutError')); cleanup() }, 10000)
  parent?.addEventListener('abort', abortParent, { once: true })
  if (parent?.aborted) abortParent()
  try { return await action(controller.signal) } finally { cleanup() }
}

export async function boundedJSON<T>(response: Response): Promise<T> {
  if (!response.ok) { await response.body?.cancel(); throw new APIError(response.status) }
  const declared = response.headers.get('Content-Length')
  if (declared && Number(declared) > MAX_RESPONSE_BYTES) { await response.body?.cancel(); throw new Error('Response exceeds the dashboard size limit.') }
  if (!response.body) throw new Error('Response body unavailable.')
  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let total = 0
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      total += value.byteLength
      if (total > MAX_RESPONSE_BYTES) { await reader.cancel(); throw new Error('Response exceeds the dashboard size limit.') }
      chunks.push(value)
    }
  } finally { reader.releaseLock() }
  const body = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength }
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body)) as T
}

export function queryPath(route: Route): string {
  const q = new URLSearchParams({ limit: '100' })
  if (route.cursor) q.set('cursor', route.cursor)
  if (route.view === 'overview' || route.view === 'queue') q.set('window', route.window)
  if (['queue', 'jobs', 'job'].includes(route.view) && route.queue) q.set('queue', route.queue)
  if (route.view === 'queue' || route.view === 'jobs') q.set('state', route.state)
  if (route.view === 'job') q.set('job_id', route.jobID)
  if (route.view === 'workers') q.set('status', route.status)
  return `/dashboard/api/${route.view}?${q.toString()}`
}

export async function readDashboard(path: string, signal: AbortSignal): Promise<DashboardData> {
  const data = await withDeadline(signal, async boundedSignal => boundedJSON<DashboardData>(await fetch(path, { signal: boundedSignal, credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' } })))
  if (data === null || typeof data !== 'object') throw new Error('Dashboard response is not an object.')
  for (const items of [data.queues?.items, data.jobs?.items, data.job?.attempts, data.items]) {
    if (items !== undefined && (!Array.isArray(items) || items.length > 200)) throw new Error('Dashboard page exceeds the item limit or has invalid items.')
  }
  return data
}
export async function sessionRequest(method: 'GET' | 'POST', signal: AbortSignal, password?: string): Promise<Session> {
  return withDeadline(signal, async boundedSignal => {
    const response = await fetch('/dashboard/session', { method, signal: boundedSignal, credentials: 'same-origin', cache: 'no-store',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: method === 'POST' ? JSON.stringify({ password }) : undefined })
    if (method === 'POST' && response.status === 401) { await response.body?.cancel(); throw new Error('Incorrect dashboard password.') }
    return boundedJSON<Session>(response)
  })
}
export async function revokeSession(): Promise<void> {
  return withDeadline(undefined, async signal => {
    const response = await fetch('/dashboard/session', { method: 'DELETE', signal, credentials: 'same-origin', cache: 'no-store' })
    if (!response.ok) throw new APIError(response.status)
  })
}
