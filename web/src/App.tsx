import { useCallback, useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Activity, ArrowLeft, ArrowRight, Check, ChevronRight, Clock3, Database, Eye, Layers3, ListFilter, LogOut, Menu, Search, ShieldCheck, TriangleAlert, Users, X } from 'lucide-react'
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Button } from './components/ui/button'
import { queryPath, revokeSession, sessionRequest } from './api'
import { routeURL, useNavigation } from './navigation'
import { usePoll } from './use-poll'
import type { BrokerQueue, DashboardData, Evidence, History, JobDetail, JobItem, Metric, Route, Session, State, View } from './types'

const number = (value: number) => new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)
const date = (value: string | null) => value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' }) : 'Unknown'
const dateUTC = (value: string) => new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium', timeZone: 'UTC' })
const stateLabels: Record<State, string> = { enqueued: 'Enqueued', started: 'Started', completed: 'Completed', failed: 'Failed', retried: 'Retried', stalled: 'Stalled · observed' }
const titles: Record<View, string> = { overview: 'Overview', queues: 'Queues', queue: 'Queue detail', jobs: 'Observed jobs', job: 'Job timeline', workers: 'Workers' }

function Logo() { return <span className="brand"><span className="brand-icon"><Layers3 size={20} /></span>QueueLens<span className="brand-dot">.</span></span> }
function Notice({ children, tone = 'info' }: { children: ReactNode; tone?: 'info' | 'warning' }) {
  return <div className={`notice notice-${tone}`}><TriangleAlert size={17} aria-hidden="true" /><div>{children}</div></div>
}
function Empty({ title, children }: { title: string; children: ReactNode }) {
  return <div className="empty"><Database size={26} aria-hidden="true" /><h3>{title}</h3><p>{children}</p></div>
}
function Link({ to, navigate, children, className }: { to: Partial<Route>; navigate: (route: Partial<Route>) => void; children: ReactNode; className?: string }) {
  return <a href={routeURL(to)} className={className} onClick={event => { if (!event.ctrlKey && !event.metaKey && !event.shiftKey) { event.preventDefault(); navigate(to) } }}>{children}</a>
}
function Login({ pending, error, onLogin }: { pending: boolean; error: string | null; onLogin: (password: string) => void }) {
  const [password, setPassword] = useState('')
  function submit(event: FormEvent) { event.preventDefault(); const input = password; setPassword(''); onLogin(input) }
  return <main className="login-page"><div className="login-card"><Logo /><div className="login-symbol"><Eye size={30} /></div>
    <p className="eyebrow">QUEUE OBSERVABILITY</p><h1>A clear view of your queues.</h1><p className="muted">Sign in to inspect broker state and retained job evidence.</p>
    <form onSubmit={submit}><label htmlFor="password">Dashboard password</label><input autoFocus id="password" name="password" type="password" autoComplete="current-password" required minLength={12} maxLength={256} value={password} onChange={e => setPassword(e.target.value)} />
      <Button disabled={pending} type="submit">{pending ? 'Signing in…' : 'Open dashboard'}<ArrowRight size={16} /></Button></form>
    {error && <p role="alert" className="form-error">{error}</p>}<p className="login-note"><ShieldCheck size={14} />Read access · four-hour session</p></div></main>
}

export function App() {
  const [session, setSession] = useState<Session | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  const loginController = useRef<AbortController | null>(null)
  const unauthorized = useCallback(() => { setSession({ authenticated: false, expires_at: null }) }, [])
  useEffect(() => {
    const controller = new AbortController()
    void sessionRequest('GET', controller.signal).then(result => { if (!controller.signal.aborted) setSession(result) }).catch(error => {
      if (!controller.signal.aborted) { setError(error instanceof Error ? error.message : 'Session bootstrap unavailable.'); unauthorized() }
    })
    return () => { controller.abort(); loginController.current?.abort() }
  }, [unauthorized])
  useEffect(() => {
    if (!session?.authenticated || !session.expires_at) return
    const timer = setTimeout(unauthorized, Math.max(0, new Date(session.expires_at).getTime() - Date.now()))
    return () => clearTimeout(timer)
  }, [session, unauthorized])
  async function login(password: string) {
    loginController.current?.abort()
    const controller = new AbortController()
    loginController.current = controller
    setPending(true); setError(null)
    try { const result = await sessionRequest('POST', controller.signal, password); if (!controller.signal.aborted) setSession(result) }
    catch (error) { if (!controller.signal.aborted) setError(error instanceof Error ? error.message : 'Sign-in unavailable.') }
    finally { if (!controller.signal.aborted) setPending(false) }
  }
  async function logout() {
    unauthorized(); setError(null)
    try { await revokeSession() } catch { setError('Logout could not reach the server. Close this tab or try signing in and logging out again to revoke the session.') }
  }
  if (session === null) return <main className="login-page"><div role="status" className="loading"><Logo /><p>Checking dashboard access…</p></div></main>
  if (!session.authenticated) return <Login pending={pending} error={error} onLogin={password => { void login(password) }} />
  return <Dashboard onUnauthorized={unauthorized} onLogout={() => { void logout() }} />
}

function Dashboard({ onUnauthorized, onLogout }: { onUnauthorized: () => void; onLogout: () => void }) {
  const { route, navigate } = useNavigation()
  const poll = usePoll(queryPath(route), onUnauthorized)
  const [mobileOpen, setMobileOpen] = useState(false)
  const go = (next: Partial<Route>) => { navigate(next); setMobileOpen(false) }
  const links = <><div className="nav-label">WORKSPACE</div>{([
    ['overview', Activity, 'Overview'], ['queues', Layers3, 'Queues'], ['jobs', ListFilter, 'Observed jobs'], ['workers', Users, 'Workers'],
  ] as const).map(([view, Icon, title]) => <Link key={view} to={{ view }} navigate={go} className={`nav-link ${route.view === view || view === 'queues' && route.view === 'queue' ? 'nav-active' : ''}`}><Icon size={18} />{title}{(route.view === view) && <span className="nav-active-dot" />}</Link>)}</>
  return <div className="app-shell"><a className="skip-link" href="#content">Skip to content</a>
    <aside className="sidebar"><Logo /><nav aria-label="Main navigation">{links}</nav><div className="sidebar-bottom"><div className="source-marker"><span />Broker + captured telemetry</div><p>Evidence from retained observations.</p><Button variant="ghost" onClick={onLogout}><LogOut size={16} />Sign out</Button></div></aside>
    <div className="main-shell"><header className="topbar"><div className="mobile-brand"><Logo /></div>
      <Dialog.Root open={mobileOpen} onOpenChange={setMobileOpen}><Dialog.Trigger asChild><Button variant="ghost" className="mobile-menu" aria-label="Open navigation"><Menu size={21} /></Button></Dialog.Trigger>
        <Dialog.Portal><Dialog.Overlay className="dialog-overlay" /><Dialog.Content className="mobile-dialog"><Dialog.Title className="sr-only">Dashboard navigation</Dialog.Title><Dialog.Description className="sr-only">Choose a dashboard view.</Dialog.Description><Logo /><Dialog.Close asChild><Button variant="ghost" className="dialog-close" aria-label="Close navigation"><X size={20} /></Button></Dialog.Close><nav aria-label="Mobile navigation">{links}</nav><Button variant="outline" onClick={onLogout}><LogOut size={16} />Sign out</Button></Dialog.Content></Dialog.Portal>
      </Dialog.Root><div className="breadcrumb">Workspace<ChevronRight size={14} /><span>{titles[route.view]}</span></div><div className="topbar-right"><span className="read-access"><ShieldCheck size={14} />Read access</span><span className="avatar">QL</span></div></header>
      <main id="content" className="content"><div className="page-heading"><div><p className="eyebrow">OBSERVE · UNDERSTAND</p><h1>{titles[route.view]}</h1><p className="muted">{route.view === 'overview' ? 'Broker state, selected outcomes, and the evidence behind them.' : route.view === 'queue' ? route.queue : route.view === 'job' ? route.jobID : 'Explore the current bounded page of retained observations.'}</p></div>
        <div className="heading-actions"><span className={`poll-marker ${poll.stale ? 'is-stale' : ''}`}><span />{poll.stale ? 'Stale data' : 'Refreshes every 5s'}</span>{['overview', 'queue'].includes(route.view) && <label className="compact-label"><Clock3 size={15} /><span className="sr-only">Metric window</span><select aria-label="Metric window" value={route.window} onChange={e => go({ ...route, window: e.target.value as Route['window'], cursor: '' })}><option value="1h">Last hour</option><option value="24h">Last 24 hours</option><option value="7d">Last 7 days</option><option value="30d">Last 30 days</option></select></label>}</div></div>
        {poll.error && <div role="alert"><Notice tone="warning">{poll.error} {poll.data ? 'Showing the last successful read. Updates retry with backoff.' : 'No successful read is available yet. Updates retry with backoff.'}</Notice></div>}
        {!poll.data && !poll.error && <div role="status" className="loading skeleton"><div /><div /><div /><p>Reading bounded observations…</p></div>}
        {poll.data && <>
          {route.view === 'overview' && <Overview data={poll.data} navigate={go} />}
          {route.view === 'queues' && <QueueTable queues={poll.data.queues?.items ?? []} errors={poll.data.queues?.adapter_errors ?? {}} navigate={go} />}
          {route.view === 'queue' && <><MetricsCards metrics={poll.data.metrics} broker={poll.data.broker ? [poll.data.broker] : null} scope="This queue" />{!poll.data.broker && <Notice>There is no current broker view for this queue. Retained job observations can still be available.</Notice>}<JobTable key={route.queue} data={poll.data} route={route} navigate={go} /></>}
          {route.view === 'jobs' && <JobTable key={route.queue} data={poll.data} route={route} navigate={go} />}
          {route.view === 'job' && poll.data.job && <Timeline job={poll.data.job} navigate={go} />}
          {route.view === 'workers' && <Workers data={poll.data} route={route} navigate={go} />}
          <Pagination route={route} next={poll.data.queues?.next_cursor ?? poll.data.jobs?.next_cursor ?? poll.data.job?.next_cursor ?? poll.data.next_cursor} navigate={go} />
          <EvidenceFooter evidence={poll.data} updatedAt={poll.updatedAt} />
        </>}
      </main></div></div>
}

function MetricsCards({ metrics, broker, scope }: { metrics?: Metric; broker: BrokerQueue[] | null; scope: string }) {
  const cards = [
    { title: 'Queue depth', value: broker ? number(broker.reduce((sum, q) => sum + q.depth, 0)) : 'Unknown', detail: broker ? `${scope} · last broker poll` : 'No current broker view', icon: Layers3, color: 'teal' },
    { title: 'Active', value: broker ? number(broker.reduce((sum, q) => sum + q.active, 0)) : 'Unknown', detail: broker ? `${scope} · last broker poll` : 'No current broker view', icon: Activity, color: 'blue' },
    { title: 'Completed / min', value: metrics?.available && metrics.completions_per_second !== null ? number(metrics.completions_per_second * 60) : 'Unavailable', detail: 'Selected retained attempts · window average', icon: Check, color: 'teal' },
    { title: 'Failure fraction', value: metrics?.available && metrics.failure_fraction !== null ? `${number(metrics.failure_fraction * 100)}%` : metrics?.available ? 'No outcomes' : 'Unavailable', detail: 'Selected failed / (failed + completed)', icon: TriangleAlert, color: 'amber' },
  ]
  return <><div className="metric-grid">{cards.map(card => <section className="metric-card" key={card.title}><div className="metric-label">{card.title}<span className={`metric-icon ${card.color}`}><card.icon size={17} /></span></div><div className="metric-value">{card.value}</div><p>{card.detail}</p></section>)}</div>
    {metrics && <div className="metric-footnote">{metrics.available ? <><span>{number(metrics.completed ?? 0)} completed · {number(metrics.failed ?? 0)} failed · {number(metrics.conflicting ?? 0)} conflicting selected outcomes</span><span>{dateUTC(metrics.from)} — {dateUTC(metrics.until)} · UTC bounds</span></> : <span>Metrics unavailable: {metrics.reason?.replaceAll('_', ' ')}. Unknown values remain unknown.</span>}</div>}
    {metrics && <p className="metric-caveat">Selected retained attempt outcomes can change with retries, late facts, clock disagreement, telemetry loss and pruning. Broker completed/failed counts are retained set sizes.</p>}</>
}

function Overview({ data, navigate }: { data: DashboardData; navigate: (route: Partial<Route>) => void }) {
  const queues = data.queues?.items ?? []
  return <><MetricsCards metrics={data.metrics} broker={queues} scope={`Current page · ${queues.length} queues`} />
    <section className="panel depth-panel"><div className="panel-heading"><div><h2>Broker depth</h2><p>First {Math.min(8, queues.length)} queues on this page · waiting, delayed, paused and prioritized</p></div><span className="badge badge-muted">Last poll</span></div>
      {queues.length === 0 ? <Empty title="No broker queues observed">A successful empty page does not establish that production queues are absent.</Empty> : <div className="chart" role="img" aria-label="Broker queue depth chart; exact counts are in the table below"><ResponsiveContainer width="100%" height="100%"><BarChart data={queues.slice(0, 8)} margin={{ top: 15, right: 16, left: -18, bottom: 5 }}><CartesianGrid vertical={false} stroke="#243040" strokeDasharray="3 4" /><XAxis dataKey="queue" tick={{ fill: '#8594a9', fontSize: 11 }} tickFormatter={value => String(value).slice(0, 18)} axisLine={false} tickLine={false} /><YAxis tick={{ fill: '#8594a9', fontSize: 11 }} axisLine={false} tickLine={false} /><Tooltip contentStyle={{ background: '#151e2b', border: '1px solid #344155', borderRadius: 8, color: '#e8edf5' }} cursor={{ fill: '#ffffff08' }} /><Bar dataKey="depth" name="Broker depth" fill="#42cbb5" radius={[4, 4, 0, 0]} maxBarSize={48} isAnimationActive={false} /></BarChart></ResponsiveContainer></div>}
    </section><QueueTable queues={queues} errors={data.queues?.adapter_errors ?? {}} navigate={navigate} /></>
}

function QueueTable({ queues, errors, navigate }: { queues: BrokerQueue[]; errors: Record<string, string>; navigate: (route: Partial<Route>) => void }) {
  return <section className="panel"><div className="panel-heading"><div><h2>Queues</h2><p>{queues.length} on this page · broker counts are independent of job telemetry</p></div><Layers3 size={19} className="muted" /></div>
    {Object.entries(errors).map(([adapter, error]) => <Notice tone="warning" key={adapter}>{adapter}: {error}. Last broker values may be stale.</Notice>)}
    {queues.length === 0 ? <Empty title="No queues on this page">Broker state is separate from retained job observations.</Empty> : <div className="table-scroll"><table><thead><tr><th>Queue</th><th className="numeric">Depth</th><th className="numeric">Active</th><th className="numeric">Completed set</th><th className="numeric">Failed set</th><th>Evidence</th></tr></thead><tbody>{queues.map(q => <tr key={q.queue}><td><Link to={{ view: 'queue', queue: q.queue }} navigate={navigate} className="queue-link"><span className="queue-symbol"><Layers3 size={15} /></span>{q.queue}<ChevronRight size={14} /></Link></td><td className="numeric">{number(q.depth)}</td><td className="numeric">{number(q.active)}</td><td className="numeric">{number(q.completed)}</td><td className="numeric">{number(q.failed)}</td><td><span className={`badge ${q.stale ? 'badge-amber' : 'badge-teal'}`}>{q.stale ? 'Stale poll' : 'Broker poll'}</span><small className="cell-note">{date(q.updated_at)}</small>{q.error && <small className="form-error">{q.error}</small>}</td></tr>)}</tbody></table></div>}</section>
}

function JobTable({ data, route, navigate }: { data: DashboardData; route: Route; navigate: (route: Partial<Route>) => void }) {
  const [queueInput, setQueueInput] = useState(route.queue)
  const [jobInput, setJobInput] = useState('')
  const jobs = data.jobs?.items ?? []
  return <section className="panel"><div className="panel-heading jobs-heading"><div><h2>Retained jobs</h2><p>Highest observed attempt · selected state · live keyset page</p></div><label className="compact-label"><ListFilter size={15} /><span className="sr-only">Observed state</span><select aria-label="Observed state" value={route.state} onChange={e => navigate({ ...route, state: e.target.value, cursor: '' })}><option value="all">All observed states</option>{Object.entries(stateLabels).map(([state, label]) => <option value={state} key={state}>{label}</option>)}</select></label></div>
    <form className="job-search" onSubmit={event => { event.preventDefault(); navigate(jobInput ? { view: 'job', queue: queueInput, jobID: jobInput } : { ...route, queue: queueInput, cursor: '' }) }}>
      <label><span>Queue{route.view === 'jobs' ? ' (optional for a list)' : ''}</span><input key={route.queue} defaultValue={route.queue} maxLength={256} onChange={e => setQueueInput(e.target.value)} required={route.view === 'queue' || jobInput !== ''} placeholder="Exact queue name" /></label><label><span>Job ID (optional)</span><input value={jobInput} maxLength={256} onChange={e => setJobInput(e.target.value)} placeholder="Exact job ID" /></label><Button variant="outline" type="submit"><Search size={15} />{jobInput ? 'Open timeline' : 'Filter queue'}</Button>
    </form>
    <p className="inline-caveat">“Stalled” is a supplied observation. Suspected stalls reflect missing receipts; silence does not prove failure or retry exhaustion.</p>
    {jobs.length === 0 ? <Empty title="No retained jobs match">Try another observed state. Missing telemetry or pruning can also leave an empty page.</Empty> : <div className="table-scroll"><table><thead><tr><th>Job / queue</th><th>Selected state</th><th className="numeric">Attempt</th><th className="numeric">Supplied duration</th><th>History evidence</th></tr></thead><tbody>{jobs.map(job => <tr key={JSON.stringify([job.queue, job.job_id])}><td><Link to={{ view: 'job', queue: job.queue, jobID: job.job_id }} navigate={navigate} className="job-link">{job.job_id}<ChevronRight size={14} /></Link><small className="cell-note">{job.queue}</small></td><td><StateBadge state={job.state} />{job.suspicion && <small className="cell-note">Suspected stalled · no receipt for {number(job.suspicion.threshold_seconds)}s</small>}</td><td className="numeric">{job.attempt}<small className="cell-note">{job.observed_attempts} observed</small></td><td className="numeric">{job.duration_ms === null ? 'Unknown' : `${number(job.duration_ms)} ms`}</td><td><HistoryFlags history={job.history} compact /></td></tr>)}</tbody></table></div>}</section>
}

function StateBadge({ state }: { state: State }) {
  return <span className={`badge ${state === 'completed' ? 'badge-teal' : state === 'failed' || state === 'stalled' ? 'badge-amber' : 'badge-muted'}`}>{stateLabels[state]}</span>
}
function HistoryFlags({ history, compact = false }: { history: History; compact?: boolean }) {
  const flags = [
    [history.missing_enqueued, 'Missing enqueue'], [history.missing_started, 'Missing start'], [history.missing_terminal, 'Missing terminal'],
    [history.pruned_history, 'Pruned / reconstructed'], [history.conflicting_terminal, 'Conflicting terminal facts'],
    [history.possible_job_id_reuse_or_clock_disagreement, 'Possible ID reuse / clock disagreement'],
    [history.legacy_identity, 'Legacy identity'], [history.missing_identity, 'Missing identity'],
    [history.incomplete_attempts > 0, `${number(history.incomplete_attempts)} structurally incomplete attempt${history.incomplete_attempts === 1 ? '' : 's'}`],
  ].filter(([enabled]) => enabled).map(([, label]) => String(label))
  return <div className={`history-flags ${compact ? 'compact-flags' : ''}`}>{flags.map(label => <span key={label} className="history-flag">{label}</span>)}{flags.length === 0 && <span className="muted">Observed structure · completeness unproven</span>}</div>
}

function Timeline({ job, navigate }: { job: JobDetail; navigate: (route: Partial<Route>) => void }) {
  const summary = job.summary
  return <><Link to={{ view: 'queue', queue: job.queue }} navigate={navigate} className="back-link"><ArrowLeft size={15} />Back to {job.queue}</Link>
    {job.availability !== 'observed' ? <section className="panel"><Empty title={job.availability === 'pruning' ? 'History is being pruned' : job.availability === 'pruned' ? 'Retained history was pruned' : 'No retained observation found'}>{job.availability === 'pruning' ? 'The timeline is withheld until pruning completes.' : job.availability === 'pruned' ? `A retained tombstone records pruning at ${date(job.pruned_at)}.` : 'No retained summary or tombstone is available.'} This does not establish that the production job is absent.</Empty></section> : <>
      {summary && <section className="panel job-summary"><div className="panel-heading"><div><h2>{summary.job_name || job.job_id}</h2><p>{job.queue} · highest observed attempt {summary.attempt}</p></div><StateBadge state={summary.state} /></div><div className="summary-grid"><div><span>Worker</span><strong>{summary.worker_id || 'Unknown'}</strong></div><div><span>Retained attempts</span><strong>{number(summary.observed_attempts)}</strong></div><div><span>Supplied duration</span><strong>{summary.duration_ms === null ? 'Unknown' : `${number(summary.duration_ms)} ms`}</strong></div><div><span>Retry exhaustion</span><strong>Unknown</strong></div></div><HistoryFlags history={summary.history} /><p className="inline-caveat">Selected source states: {summary.source_states.join(', ')} · {summary.state_evidence}. The timeline contains retained selected attempt summaries, not full event replay.</p><CaptureTimes item={summary} /></section>}
      <section className="panel timeline-panel"><div className="panel-heading"><div><h2>Attempt timeline</h2><p>{job.attempts.length} on this page · ascending attempt number</p></div><Clock3 size={19} className="muted" /></div>{job.attempts.length === 0 ? <Empty title="No attempts on this page">Retained membership can change between reads.</Empty> : <ol className="timeline">{job.attempts.map(attempt => <li key={attempt.attempt}><span className={`timeline-point ${attempt.state === 'completed' ? 'point-teal' : ''}`} /><div className="attempt-card"><div className="attempt-heading"><h3>Attempt {attempt.attempt}</h3><StateBadge state={attempt.state} /><span className="attempt-time">{date(attempt.state_at)}</span></div><div className="attempt-meta"><span>Duration: {attempt.duration_ms === null ? 'Unknown' : `${number(attempt.duration_ms)} ms (producer reported)`}</span><span>Worker: {attempt.worker_id || 'Unknown'}</span></div>{attempt.error && <pre className="error-text">{attempt.error}</pre>}<HistoryFlags history={attempt.history} /><details><summary>Source and capture evidence</summary><p className="inline-caveat">Supplied states: {attempt.source_states.join(', ')}. Retry exhaustion remains unknown.</p><CaptureTimes item={attempt} /></details></div></li>)}</ol>}</section>
    </>}</>
}

function CaptureTimes({ item }: { item: JobItem }) {
  return <dl className="capture-times"><div><dt>Enqueued (producer)</dt><dd>{date(item.enqueued_at)}</dd></div><div><dt>Started (producer)</dt><dd>{date(item.started_at)}</dd></div><div><dt>First / last supplied timestamp</dt><dd>{date(item.first_event_at)} / {date(item.last_event_at)}</dd></div><div><dt>First / last collector receipt</dt><dd>{date(item.first_receipt_at)} / {date(item.last_receipt_at)}</dd></div></dl>
}

function Workers({ data, route, navigate }: { data: DashboardData; route: Route; navigate: (route: Partial<Route>) => void }) {
  const workers = data.items ?? []
  return <section className="panel"><div className="panel-heading"><div><h2>Worker receipts</h2><p>{workers.length} on this page · collector receipt times</p></div><select aria-label="Worker receipt status" value={route.status} onChange={e => navigate({ ...route, status: e.target.value, cursor: '' })}><option value="all">All workers</option><option value="suspected_down">Suspected down</option></select></div><Notice>Missing heartbeats mean suspected worker down under the receipt/tick policy. A recent receipt does not prove liveness; silence does not prove death or terminal job failure.</Notice>
    {workers.length === 0 ? <Empty title="No retained worker receipts">Try another receipt filter. Telemetry loss and retention can change membership.</Empty> : <div className="table-scroll"><table><thead><tr><th>Worker / pool</th><th>Receipt evidence</th><th>Last receipt</th><th>Age at evaluation</th><th>First receipt</th></tr></thead><tbody>{workers.map(worker => <tr key={worker.worker_id}><td><span className="worker-name">{worker.worker_id}</span><small className="cell-note">{worker.pool || 'Unspecified pool'}</small></td><td><span className={`badge ${worker.status === 'suspected_down' ? 'badge-amber' : 'badge-teal'}`}>{worker.status === 'suspected_down' ? 'Suspected down' : 'Recent receipt'}</span>{worker.suspicion && <small className="cell-note">No receipt · {number(worker.suspicion.threshold_seconds)}s threshold</small>}</td><td>{date(worker.last_seen)}{new Date(worker.last_seen) > new Date(data.evaluated_at) && <small className="cell-note">Receipt ahead of evaluation tick</small>}</td><td>{number(worker.receipt_age_seconds)}s</td><td>{date(worker.first_seen)}</td></tr>)}</tbody></table></div>}</section>
}

function Pagination({ route, next, navigate }: { route: Route; next?: string; navigate: (route: Partial<Route>) => void }) {
  return <div className="pagination"><p>Up to 100 items per page · membership may change between pages</p><div>{route.cursor && <Button variant="outline" size="small" onClick={() => navigate({ ...route, cursor: '' })}><ArrowLeft size={14} />First page</Button>}<Button variant="outline" size="small" disabled={!next} onClick={() => navigate({ ...route, cursor: next ?? '' })}>Next page<ArrowRight size={14} /></Button></div></div>
}
function EvidenceFooter({ evidence, updatedAt }: { evidence: Evidence; updatedAt: Date | null }) {
  const lag = evidence.projection_lag
  return <footer className="evidence-footer"><div className="evidence-strip"><span><Database size={14} />{lag.known ? `Projection lag: ${lag.lag_seconds === null ? 'no sampled pending receipt' : `${number(lag.lag_seconds)}s`} · ${lag.sample_capped ? 'at least ' : ''}${number(lag.pending_rows ?? lag.pending_rows_lower_bound)} pending` : 'Projection lag: unknown'}</span><span>Read {updatedAt?.toLocaleTimeString() ?? '—'}</span></div><details><summary>History, clocks and read limitations</summary><p>Evaluated at {date(evidence.evaluated_at)} · history coverage from {date(evidence.history_from)} · live keyset consistency. Polling pauses in hidden tabs.</p>{evidence.limitations.map(text => <p key={text}>{text}</p>)}{!lag.known && <p>Lag evidence unavailable: {lag.reason ?? 'unknown'}.</p>}{lag.pruning_runs_lower_bound > 0 && <p>At least {lag.pruning_runs_lower_bound} histories are being pruned.</p>}</details></footer>
}
