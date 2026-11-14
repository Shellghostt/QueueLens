import { randomUUID } from "node:crypto";

export type State = "enqueued" | "started" | "completed" | "failed" | "retried" | "stalled" | "drained";
export interface Event {
  v?: number; id?: string; ts?: string; queue: string; job_id: string; job_name?: string;
  attempt?: number; state: State; worker_id?: string; duration_ms?: number; error?: string;
}
export interface Options {
  url: string; token: string; workerId?: string; pool?: string; bufferSize?: number;
  batchSize?: number; flushEveryMs?: number; heartbeatEveryMs?: number; requestTimeoutMs?: number;
}
export interface Stats { emitted: number; sent: number; dropped: number; rejected: number; heartbeats: number; heartbeatErrors: number; buffered: number }

export function validateEvent(e: Event, now: number): string | null {
  if (e.v !== 1) return "v";
  for (const field of ["id","error"] as const) {
    if (e[field]!==undefined && typeof e[field]!=="string") return field;
  }
  for (const [field, limit, required] of [["queue",256,true],["job_id",256,true],["job_name",256,false],["worker_id",128,false]] as const) {
    const value=e[field];
    if (required && (typeof value!=="string" || !value.trim())) return field;
    if (value!==undefined && (typeof value!=="string" || Buffer.byteLength(value)>limit)) return field;
  }
  if (!["enqueued","started","completed","failed","retried","stalled","drained"].includes(e.state)) return "state";
  if (e.attempt!==undefined && (!Number.isInteger(e.attempt) || e.attempt<0 || e.attempt>32767)) return "attempt";
  if (e.duration_ms!==undefined && (!Number.isInteger(e.duration_ms) || e.duration_ms<0 || e.duration_ms>2147483647)) return "duration_ms";
  if (typeof e.ts!=="string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(e.ts)) return "ts";
  const timestamp=Date.parse(e.ts);
  if (!Number.isFinite(timestamp) || timestamp<now-7*86400000 || timestamp>now+3600000) return "ts";
  return null;
}

/** Best-effort instrumentation. emit performs no serialization, fetch or awaiting. */
export class Client {
  private readonly options: Required<Options>;
  private readonly ring: (Event | undefined)[];
  private head=0; private size=0; private closed=false;
  private active?: AbortController;
  private task?: Promise<void>;
  private closeTask?: Promise<void>;
  private closeExpired=false;
  private flushTimer: NodeJS.Timeout;
  private heartbeatTimer: NodeJS.Timeout;
  private heartbeatPending=true;
  private counters={emitted:0,sent:0,dropped:0,rejected:0,heartbeats:0,heartbeatErrors:0};

  constructor(options: Options) {
    const url=new URL(options.url);
    if (!["http:","https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("invalid collector base URL");
    if (options.token.length<16 || /[\r\n]/.test(options.token)) throw new Error("invalid token");
    this.options={url:options.url.replace(/\/+$/,""),token:options.token,workerId:options.workerId??randomUUID(),pool:options.pool??"",bufferSize:options.bufferSize??10000,batchSize:options.batchSize??500,flushEveryMs:options.flushEveryMs??250,heartbeatEveryMs:options.heartbeatEveryMs??15000,requestTimeoutMs:options.requestTimeoutMs??2000};
    if (!this.options.workerId.trim() || Buffer.byteLength(this.options.workerId)>128 || Buffer.byteLength(this.options.pool)>128) throw new Error("invalid worker identity");
    for (const key of ["bufferSize","batchSize","flushEveryMs","heartbeatEveryMs","requestTimeoutMs"] as const) {
      if (!Number.isInteger(this.options[key]) || this.options[key]<1) throw new Error(`invalid ${key}`);
    }
    if (this.options.batchSize>1000 || this.options.bufferSize>1000000 || this.options.flushEveryMs>2147483647 || this.options.heartbeatEveryMs>2147483647) throw new Error("option exceeds limit");
    this.ring=new Array(this.options.bufferSize);
    this.flushTimer=setInterval(()=>this.schedule(),this.options.flushEveryMs);this.flushTimer.unref();
    this.heartbeatTimer=setInterval(()=>{this.heartbeatPending=true;this.schedule()},this.options.heartbeatEveryMs);this.heartbeatTimer.unref();
    // Initial heartbeat is deferred out of constructor and every job path.
    queueMicrotask(()=>this.schedule());
  }

  emit(event: Event): boolean {
    this.counters.emitted++;
    if (this.closed || this.size===this.ring.length) {this.counters.dropped++;return false}
    // Copy only protocol fields; getters/payload objects are deliberately not retained.
    this.ring[(this.head+this.size)%this.ring.length]={v:event.v??1,id:event.id,ts:event.ts??new Date().toISOString(),queue:event.queue,job_id:event.job_id,job_name:event.job_name,attempt:event.attempt??1,state:event.state,worker_id:event.worker_id??this.options.workerId,duration_ms:event.duration_ms,error:event.error};
    this.size++;return true;
  }
  stats(): Stats {return {...this.counters,buffered:this.size}}
  prometheus(): string {return `# TYPE queuelens_dropped_total counter\nqueuelens_dropped_total ${this.counters.dropped}\n`}

  close(timeoutMs=5000): Promise<void> {
    if (!Number.isFinite(timeoutMs) || timeoutMs<1) return Promise.reject(new Error("invalid close timeout"));
    if (this.closeTask) return this.closeTask;
    this.closed=true;clearInterval(this.flushTimer);clearInterval(this.heartbeatTimer);
    this.closeTask=(async()=>{
      const timeout=setTimeout(()=>{this.closeExpired=true;this.active?.abort();this.dropBuffered()},timeoutMs);
      try {await this.task;if(this.heartbeatPending && !this.closeExpired){this.heartbeatPending=false;await this.heartbeat()}while(this.size>0 && !this.closeExpired) await this.flushBatch()}
      finally {clearTimeout(timeout)}
    })();
    return this.closeTask;
  }
  private dropBuffered(): void {this.counters.dropped+=this.size;this.ring.fill(undefined);this.size=0;this.head=0}
  private schedule(): void {
    if (this.closed || this.task) return;
    this.task=(async()=>{if(this.heartbeatPending){this.heartbeatPending=false;await this.heartbeat()}if(this.size>0)await this.flushBatch()})().finally(()=>{this.task=undefined});
  }
  private async post(path: string, payload: unknown): Promise<{status:number;body:string}> {
    const body=JSON.stringify(payload);
    if (Buffer.byteLength(body)>1048576) throw new Error("batch exceeds 1 MiB");
    const controller=new AbortController();this.active=controller;
    const timeout=setTimeout(()=>controller.abort(),this.options.requestTimeoutMs);
    try {
      const response=await fetch(this.options.url+path,{method:"POST",redirect:"error",headers:{authorization:`Bearer ${this.options.token}`,"content-type":"application/json"},body,signal:controller.signal});
      const reader=response.body?.getReader();const chunks:Uint8Array[]=[];let size=0;
      if(reader)try{while(true){const value=await reader.read();if(value.done)break;size+=value.value.length;if(size>65536)throw new Error("response too large");chunks.push(value.value)}}finally{await reader.cancel().catch(()=>{});reader.releaseLock()}
      return {status:response.status,body:Buffer.concat(chunks).toString("utf8")};
    } finally {clearTimeout(timeout);if(this.active===controller)this.active=undefined}
  }
  private async heartbeat(): Promise<void> {
    try {const result=await this.post("/v1/workers/heartbeat",{v:1,worker_id:this.options.workerId,pool:this.options.pool});if(result.status!==202)throw new Error("heartbeat rejected");this.counters.heartbeats++}
    catch {this.counters.heartbeatErrors++}
  }
  private async flushBatch(): Promise<void> {
    const batch:Event[]=[];
    while(this.size>0 && batch.length<this.options.batchSize){const event=this.ring[this.head]!;this.ring[this.head]=undefined;this.head=(this.head+1)%this.ring.length;this.size--;batch.push(event)}
    try {
      const result=await this.post("/v1/events",batch);const ack=JSON.parse(result.body) as {accepted:number;dropped:number;rejected:unknown[]};
      if (![202,429,400].includes(result.status) || !Number.isInteger(ack.accepted) || ack.accepted<0 || !Number.isInteger(ack.dropped) || ack.dropped<0 || !Array.isArray(ack.rejected) || ack.accepted+ack.dropped+ack.rejected.length!==batch.length) throw new Error("invalid acknowledgement");
      this.counters.sent+=ack.accepted;this.counters.dropped+=ack.dropped;this.counters.rejected+=ack.rejected.length;
    } catch {this.counters.dropped+=batch.length}
  }
}
