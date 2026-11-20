import { useCallback, useEffect, useState } from 'react'
import type { Route, View, WindowName } from './types'

const views: View[] = ['overview', 'queues', 'queue', 'jobs', 'job', 'workers']
const windows: WindowName[] = ['1h', '24h', '7d', '30d']
export const firstRoute: Route = { view: 'overview', queue: '', jobID: '', state: 'all', window: '1h', status: 'all', cursor: '' }

export function routeURL(route: Partial<Route>): string {
  const merged = { ...firstRoute, ...route }
  const q = new URLSearchParams({ view: merged.view })
  if (merged.queue) q.set('queue', merged.queue)
  if (merged.jobID) q.set('job_id', merged.jobID)
  if (merged.state !== 'all') q.set('state', merged.state)
  if (merged.window !== '1h') q.set('window', merged.window)
  if (merged.status !== 'all') q.set('status', merged.status)
  if (merged.cursor) q.set('cursor', merged.cursor)
  return `/?${q.toString()}`
}
function currentRoute(): Route {
  const q = new URLSearchParams(window.location.search)
  const view = q.get('view') as View
  const windowName = q.get('window') as WindowName
  return { ...firstRoute, view: views.includes(view) ? view : 'overview', queue: q.get('queue') ?? '', jobID: q.get('job_id') ?? '',
    state: q.get('state') ?? 'all', window: windows.includes(windowName) ? windowName : '1h', status: q.get('status') ?? 'all', cursor: q.get('cursor') ?? '' }
}
export function useNavigation() {
  const [route, setRoute] = useState(currentRoute)
  useEffect(() => {
    const changed = () => setRoute(currentRoute())
    window.addEventListener('popstate', changed)
    return () => window.removeEventListener('popstate', changed)
  }, [])
  const navigate = useCallback((next: Partial<Route>) => { window.history.pushState(null, '', routeURL(next)); setRoute(currentRoute()) }, [])
  return { route, navigate }
}
