export type State = 'enqueued' | 'started' | 'completed' | 'failed' | 'retried' | 'stalled'
export type View = 'overview' | 'queues' | 'queue' | 'jobs' | 'job' | 'workers'
export type WindowName = '1h' | '24h' | '7d' | '30d'
export interface Route { view: View; queue: string; jobID: string; state: string; window: WindowName; status: string; cursor: string }
export interface ProjectionLag {
  known: boolean; pending_rows: number | null; pending_rows_lower_bound: number; sample_capped: boolean;
  lag_seconds: number | null; reason?: string; pruning_runs_lower_bound: number
}
export interface Evidence {
  evaluated_at: string; history_from: string; projection_lag: ProjectionLag; consistency: string; limitations: string[]
}
export interface BrokerQueue {
  queue: string; waiting: number; active: number; completed: number; failed: number; delayed: number;
  paused: number; prioritized: number; depth: number; updated_at: string; stale: boolean; error?: string
}
export interface History {
  complete_history: boolean; missing_enqueued: boolean; missing_started: boolean; missing_terminal: boolean;
  legacy_identity: boolean; missing_identity: boolean; pruned_history: boolean; conflicting_terminal: boolean;
  possible_job_id_reuse_or_clock_disagreement: boolean; incomplete_attempts: number
}
export interface JobItem {
  queue: string; job_id: string; job_name?: string; worker_id?: string; attempt: number; observed_attempts: number;
  state: State; state_evidence: string; state_at: string; source_states: State[]; enqueued_at: string | null;
  started_at: string | null; duration_ms: number | null; duration_kind: string; error?: string; first_event_at: string;
  last_event_at: string; first_receipt_at: string; last_receipt_at: string; retry_exhausted: boolean | null;
  history: History; suspicion: { reason: string; threshold_seconds: number } | null
}
export interface Metric {
  available: boolean; reason?: string; window: WindowName; from: string; until: string; source: string;
  completed: number | null; failed: number | null; conflicting: number | null;
  completions_per_second: number | null; failure_fraction: number | null; limitations: string[]
}
export interface JobDetail {
  queue: string; job_id: string; availability: 'observed' | 'pruning' | 'pruned' | 'not_observed';
  summary: JobItem | null; attempts: JobItem[]; next_cursor?: string; pruned_at: string | null
}
export interface Worker {
  worker_id: string; pool: string; first_seen: string; last_seen: string; receipt_age_seconds: number;
  status: 'alive' | 'suspected_down'; suspicion: { reason: string; threshold_seconds: number } | null
}
export interface DashboardData extends Evidence {
  queues?: { items: BrokerQueue[]; next_cursor?: string; adapter_errors: Record<string, string>; source: string };
  broker?: BrokerQueue; metrics?: Metric; jobs?: { items: JobItem[]; next_cursor?: string }; job?: JobDetail;
  items?: Worker[]; next_cursor?: string
}
export interface Session { authenticated: boolean; expires_at: string | null }
