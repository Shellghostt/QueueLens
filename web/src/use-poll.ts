import { useEffect, useState } from 'react'
import { APIError, readDashboard } from './api'
import type { DashboardData } from './types'

interface PollState { key: string; data: DashboardData | null; error: string | null; stale: boolean; updatedAt: Date | null }
const delays = [5000, 10000, 20000, 40000, 60000]

export function usePoll(path: string, onUnauthorized: () => void) {
  const [state, setState] = useState<PollState>({ key: path, data: null, error: null, stale: false, updatedAt: null })
  useEffect(() => {
    let active = true
    let generation = 0
    let failures = 0
    let timer: ReturnType<typeof setTimeout> | undefined
    let controller: AbortController | undefined
    let running = false
    let hidden = document.hidden
    function clear() {
      generation++
      if (timer !== undefined) clearTimeout(timer)
      timer = undefined
      controller?.abort()
      controller = undefined
      running = false
    }
    async function refresh() {
      if (!active || document.hidden || running) return
      running = true
      const current = ++generation
      controller = new AbortController()
      try {
        const data = await readDashboard(path, controller.signal)
        if (!active || current !== generation) return
        failures = 0
        setState({ key: path, data, stale: false, error: null, updatedAt: new Date() })
      } catch (error) {
        if (!active || current !== generation) return
        if (error instanceof APIError && error.status === 401) { clear(); onUnauthorized(); return }
        failures++
        const message = error instanceof Error ? error.message : 'Dashboard read unavailable.'
        setState(previous => ({ key: path, data: previous.key === path ? previous.data : null,
          updatedAt: previous.key === path ? previous.updatedAt : null, stale: true, error: message }))
      } finally {
        if (active && current === generation) {
          running = false
          controller = undefined
          timer = setTimeout(() => { void refresh() }, delays[Math.min(Math.max(failures - 1, 0), delays.length - 1)])
        }
      }
    }
    function visibilityChanged() {
      if (hidden === document.hidden) return
      hidden = document.hidden
      clear()
      if (!hidden) void refresh()
    }
    document.addEventListener('visibilitychange', visibilityChanged)
    void refresh()
    return () => { active = false; clear(); document.removeEventListener('visibilitychange', visibilityChanged) }
  }, [path, onUnauthorized])
  return state.key === path ? state : { key: path, data: null, error: null, stale: false, updatedAt: null }
}
