import { describe, expect, it, vi } from 'vitest'
import { APIError, boundedJSON, MAX_RESPONSE_BYTES, queryPath, readDashboard, sessionRequest } from './api'
import { firstRoute } from './navigation'

describe('bounded same-origin client', () => {
  it('preserves exact Unicode and reserved identifiers, decoded once', () => {
    const queue = 'jobs/failed % \"雪'
    const jobID = "workers/'?%&é"
    const url = new URL(queryPath({ ...firstRoute, view: 'job', queue, jobID }), 'http://localhost')
    expect(url.pathname).toBe('/dashboard/api/job')
    expect(url.searchParams.get('queue')).toBe(queue)
    expect(url.searchParams.get('job_id')).toBe(jobID)
    expect(url.searchParams.has('window')).toBe(false)
  })
  it('enforces byte bounds with or without a content-length header', async () => {
    await expect(boundedJSON(new Response('{}', { headers: { 'Content-Length': String(MAX_RESPONSE_BYTES + 1) } }))).rejects.toThrow('size limit')
    let cancelled = false
    const stream = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array(MAX_RESPONSE_BYTES)); controller.enqueue(new Uint8Array(1)) },
      cancel() { cancelled = true },
    })
    await expect(boundedJSON(new Response(stream))).rejects.toThrow('size limit')
    expect(cancelled).toBe(true)
    await expect(boundedJSON(new Response('{"valid":true}'))).resolves.toEqual({ valid: true })
  })
  it('distinguishes unauthorized and unavailable reads without echoing response input', async () => {
    await expect(boundedJSON(new Response('secret-input', { status: 401 }))).rejects.toMatchObject({ status: 401 })
    await expect(boundedJSON(new Response('secret-input', { status: 503 }))).rejects.toBeInstanceOf(APIError)
    await expect(boundedJSON(new Response('<html>oops</html>'))).rejects.toThrow()
  })
  it('caps every cached list even if a malformed upstream exceeds the server page contract', async () => {
    const controller = new AbortController()
    try {
      for (const data of [{ queues: { items: Array(201).fill({}) } }, { jobs: { items: Array(201).fill({}) } },
        { job: { attempts: Array(201).fill({}) } }, { items: Array(201).fill({}) }]) {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(data))))
        await expect(readDashboard('/dashboard/api/jobs', controller.signal)).rejects.toThrow('item limit')
      }
    } finally { vi.unstubAllGlobals() }
  })
  it('identifies an incorrect login password without calling it an expired session', async () => {
    try {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}', { status: 401 })))
      await expect(sessionRequest('POST', new AbortController().signal, 'wrong-password-123')).rejects.toThrow('Incorrect dashboard password.')
    } finally { vi.unstubAllGlobals() }
  })
})
