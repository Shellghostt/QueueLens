import assert from "node:assert/strict";
import {test} from "node:test";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {Client,validateEvent} from "../dist/index.js";

async function server(handler) {
  const s=createServer(handler);await new Promise(resolve=>s.listen(0,"127.0.0.1",resolve));
  return {url:`http://127.0.0.1:${s.address().port}`,close:()=>new Promise(resolve=>{s.close(resolve);s.closeAllConnections()})};
}
const token="0123456789abcdef";

test("shared protocol cases",async()=>{
  const fixture=JSON.parse(await readFile(new URL("../../../tests/conformance/events.json",import.meta.url)));
  for(const c of fixture.cases)assert.equal(validateEvent(c.event,Date.parse(fixture.now))===null,c.valid,c.name);
});

test("immutable metadata, defaults, partial acknowledgement and heartbeat",async()=>{
  const received=[];
  const s=await server(async(req,res)=>{
    assert.equal(req.headers.authorization,`Bearer ${token}`);let raw="";for await(const chunk of req)raw+=chunk;
    if(req.url==="/v1/workers/heartbeat"){assert.ok(JSON.parse(raw).worker_id);res.writeHead(202);res.end();return}
    received.push(...JSON.parse(raw));res.writeHead(429);res.end(JSON.stringify({accepted:1,dropped:1,rejected:[{index:2}]}));
  });
  const c=new Client({url:s.url,token,batchSize:3});
  try {
    const e={queue:"q",job_id:"j",state:"completed",duration_ms:17};for(let i=0;i<3;i++)assert.ok(c.emit(e));e.duration_ms=999;
    // Let the deferred heartbeat run before closing.
    await new Promise(resolve=>setTimeout(resolve,10));await c.close();
    assert.equal(received.length,3);for(const e of received){assert.equal(e.v,1);assert.equal(e.attempt,1);assert.equal(e.duration_ms,17);assert.ok(e.worker_id);assert.ok(e.ts)}
    assert.deepEqual([c.stats().sent,c.stats().dropped,c.stats().rejected,c.stats().buffered],[1,1,1,0]);assert.match(c.prometheus(),/queuelens_dropped_total 1/);
    assert.equal(c.emit(e),false);
  }finally{await c.close();await s.close()}
});

test("one million emits with a hung collector do not await transport",async()=>{
  const s=await server(async(req,res)=>{for await(const _ of req){};if(req.url==="/v1/workers/heartbeat"){res.writeHead(202);res.end()}});
  const c=new Client({url:s.url,token,requestTimeoutMs:1000});
  try {
    const start=performance.now();for(let i=0;i<1000000;i++)c.emit({queue:"q",job_id:"j",state:"started"});
    assert.ok(performance.now()-start<5000);assert.equal(c.stats().buffered,10000);await c.close(50);
    assert.equal(c.stats().dropped,1000000);assert.equal(c.stats().sent,0);assert.equal(c.stats().buffered,0);
  }finally{await c.close();await s.close()}
});

test("malformed acknowledgements are losses, not job errors",async()=>{
  const s=await server(async(req,res)=>{for await(const _ of req){};res.writeHead(202);res.end("bad-json")});
  const c=new Client({url:s.url,token});
  try{c.emit({queue:"q",job_id:"j",state:"started"});await c.close();assert.equal(c.stats().dropped,1)}finally{await s.close()}
});

test("redirects never forward the collector token",async()=>{
  let forwarded=0;
  const target=await server((req,res)=>{forwarded++;res.writeHead(202);res.end()});
  const source=await server((req,res)=>{res.writeHead(307,{location:target.url});res.end()});
  const c=new Client({url:source.url,token});
  try{c.emit({queue:"q",job_id:"j",state:"started"});await c.close();assert.equal(forwarded,0);assert.equal(c.stats().dropped,1);assert.equal(c.stats().heartbeatErrors,1)}finally{await source.close();await target.close()}
});
