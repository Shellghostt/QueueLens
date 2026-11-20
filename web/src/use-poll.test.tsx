import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { usePoll } from './use-poll'

const response = () => new Response(JSON.stringify({ items: ['retained'], evaluated_at: '2026-11-21T00:00:00Z' }))
async function settle() { await act(async () => { await vi.advanceTimersByTimeAsync(0) }) }
function visible(hidden: boolean) { Object.defineProperty(document, 'hidden', { configurable: true, value: hidden }); document.dispatchEvent(new Event('visibilitychange')) }

describe('bounded polling lifecycle', () => {
  beforeEach(() => { vi.useFakeTimers(); Object.defineProperty(document, 'hidden', { configurable: true, value: false }) })
  afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals() })
  it('keeps one active request, aborts route changes and discards older responses', async () => {
    const signals: AbortSignal[] = []
    const resolves: ((response: Response) => void)[] = []
    const fetcher = vi.fn((_input: RequestInfo | URL, options?: RequestInit) => {
      signals.push(options!.signal!)
      return new Promise<Response>(resolve => resolves.push(resolve))
    })
    vi.stubGlobal('fetch', fetcher)
    const unauthorized = vi.fn()
    const hook = renderHook(({ path }) => usePoll(path, unauthorized), { initialProps: { path: '/dashboard/api/jobs?queue=first' } })
    await act(async () => { await vi.advanceTimersByTimeAsync(20000) })
    expect(fetcher).toHaveBeenCalledTimes(1)
    hook.rerender({ path: '/dashboard/api/jobs?queue=second' })
    expect(signals[0].aborted).toBe(true)
    expect(fetcher).toHaveBeenCalledTimes(2)
    await act(async () => { resolves[1](response()); await vi.advanceTimersByTimeAsync(0) })
    expect(hook.result.current.data).not.toBeNull()
    const retained = hook.result.current.data
    await act(async () => { resolves[0](new Response('{"items":["old"]}')); await vi.advanceTimersByTimeAsync(0) })
    expect(hook.result.current.data).toBe(retained)
    hook.unmount()
    expect(vi.getTimerCount()).toBe(0)
  })
  it('pauses hidden tabs, aborts active reads and refreshes once on return', async () => {
    const signals: AbortSignal[] = []
    const fetcher = vi.fn((_input: RequestInfo | URL, options?: RequestInit) => { signals.push(options!.signal!); return new Promise<Response>(() => {}) })
    vi.stubGlobal('fetch', fetcher)
    const unauthorized = vi.fn()
    const hook = renderHook(() => usePoll('/dashboard/api/workers', unauthorized))
    act(() => visible(true))
    expect(signals[0].aborted).toBe(true)
    await act(async () => { await vi.advanceTimersByTimeAsync(60000) })
    expect(fetcher).toHaveBeenCalledTimes(1)
    act(() => visible(false))
    act(() => visible(false))
    expect(fetcher).toHaveBeenCalledTimes(2)
    hook.unmount()
    expect(signals[1].aborted).toBe(true)
    act(() => visible(true)); act(() => visible(false))
    expect(fetcher).toHaveBeenCalledTimes(2)
  })
  it('retains stale data and follows 5/10/20/40/60-second backoff without page accumulation', async () => {
    const fetcher = vi.fn().mockResolvedValue(response())
    vi.stubGlobal('fetch', fetcher)
    const unauthorized = vi.fn()
    const hook = renderHook(({ path }) => usePoll(path, unauthorized), { initialProps: { path: '/dashboard/api/queues' } })
    await settle()
    const retained = hook.result.current.data
    fetcher.mockRejectedValue(new TypeError('Network unavailable'))
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    expect(hook.result.current.stale).toBe(true)
    expect(hook.result.current.data).toBe(retained)
    let calls = 2
    for (const delay of [5000, 10000, 20000, 40000, 60000, 60000]) {
      await act(async () => { await vi.advanceTimersByTimeAsync(delay - 1) })
      expect(fetcher).toHaveBeenCalledTimes(calls)
      await act(async () => { await vi.advanceTimersByTimeAsync(1) })
      expect(fetcher).toHaveBeenCalledTimes(++calls)
    }
    hook.rerender({ path: '/dashboard/api/queues?cursor=next' })
    expect(hook.result.current.data).toBeNull()
    hook.unmount()
    expect(vi.getTimerCount()).toBe(0)
  })
  it('clears polling on unauthorized without retrying or keeping the old view mounted', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response('{}', { status: 401 }))
    vi.stubGlobal('fetch', fetcher)
    const unauthorized = vi.fn()
    const hook = renderHook(() => usePoll('/dashboard/api/jobs', unauthorized))
    await settle()
    expect(unauthorized).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(0)
    await act(async () => { await vi.advanceTimersByTimeAsync(60000) })
    expect(fetcher).toHaveBeenCalledTimes(1)
    hook.unmount()
  })
})
