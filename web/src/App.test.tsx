import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { App } from './App'

afterEach(() => { vi.unstubAllGlobals(); window.history.replaceState(null, '', '/'); localStorage.clear(); sessionStorage.clear() })
const evidence = { evaluated_at: '2026-11-21T08:00:00Z', history_from: '2026-10-22T08:00:00Z', consistency: 'live_row_keyset',
  projection_lag: { known: false, reason: 'bounded lag unavailable' }, limitations: ['Best-effort telemetry cannot prove complete history.'] }

it('renders hostile metadata as text, preserves identifiers in links and displays pruned evidence', async () => {
  const queue = 'jobs/failed % 雪'
  const id = '<img src=x onerror=alert(1)>'
  window.history.replaceState(null, '', '/?view=jobs')
  const fetcher = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input)
    if (url === '/dashboard/session') return new Response(JSON.stringify({ authenticated: true, expires_at: new Date(Date.now() + 3600000).toISOString() }))
    if (url.includes('/job?')) return new Response(JSON.stringify({ ...evidence, job: { queue, job_id: id, availability: 'pruned', summary: null, attempts: [], pruned_at: '2026-11-21T07:00:00Z' } }))
    return new Response(JSON.stringify({ ...evidence, jobs: { items: [{ queue, job_id: id, attempt: 3, observed_attempts: 3, state: 'failed', duration_ms: null,
      suspicion: null, history: { missing_started: true, incomplete_attempts: 3 } }] } }))
  })
  vi.stubGlobal('fetch', fetcher)
  render(<App />)
  const link = await screen.findByRole('link', { name: id })
  expect(document.querySelector('img')).toBeNull()
  const parsed = new URL(link.getAttribute('href')!, 'http://localhost')
  expect(parsed.searchParams.get('queue')).toBe(queue)
  expect(parsed.searchParams.get('job_id')).toBe(id)
  expect(screen.getByText('Unknown')).toBeTruthy()
  expect(screen.getByText('Missing start')).toBeTruthy()
  fireEvent.click(link)
  await screen.findByText('Retained history was pruned')
  expect(screen.getByText(/does not establish that the production job is absent/)).toBeTruthy()
  expect(screen.getByText(/Projection lag: unknown/)).toBeTruthy()
})

it('keeps login credentials out of browser storage and aborts the view on sign out', async () => {
  const requests: { url: string; options?: RequestInit }[] = []
  window.history.replaceState(null, '', '/?view=workers')
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, options?: RequestInit) => {
    const url = String(input)
    requests.push({ url, options })
    if (options?.method === 'DELETE') return new Response(null, { status: 204 })
    if (url === '/dashboard/session') return new Response(JSON.stringify({ authenticated: options?.method === 'POST', expires_at: new Date(Date.now() + 3600000).toISOString() }))
    return new Response(JSON.stringify({ ...evidence, items: [] }))
  }))
  render(<App />)
  const password = await screen.findByLabelText('Dashboard password')
  fireEvent.change(password, { target: { value: 'm4-synthetic-viewer-password' } })
  fireEvent.submit(password.closest('form')!)
  await screen.findByText('Worker receipts')
  expect(localStorage.length).toBe(0)
  expect(sessionStorage.length).toBe(0)
  expect(requests.every(request => !request.url.includes('password') && request.options?.credentials === 'same-origin')).toBe(true)
  fireEvent.click(screen.getByRole('button', { name: 'Sign out' }))
  await screen.findByLabelText('Dashboard password')
  await waitFor(() => expect(requests.some(request => request.options?.method === 'DELETE')).toBe(true))
  expect(screen.queryByText('Worker receipts')).toBeNull()
})

it('returns to login and removes the active view at the fixed session expiry', async () => {
  vi.useFakeTimers()
  try {
    window.history.replaceState(null, '', '/?view=workers')
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => new Response(JSON.stringify(String(input) === '/dashboard/session'
      ? { authenticated: true, expires_at: new Date(Date.now() + 30000).toISOString() }
      : { ...evidence, items: [] }))))
    const view = render(<App />)
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(screen.getByText('Worker receipts')).toBeTruthy()
    await act(async () => { await vi.advanceTimersByTimeAsync(30000) })
    expect(screen.getByLabelText('Dashboard password')).toBeTruthy()
    expect(screen.queryByText('Worker receipts')).toBeNull()
    view.unmount()
    // Let jsdom's pending animation frame finish after React's DOM teardown.
    await act(async () => { await vi.advanceTimersByTimeAsync(20) })
    expect(vi.getTimerCount()).toBe(0)
  } finally { vi.useRealTimers() }
})
