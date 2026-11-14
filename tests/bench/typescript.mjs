import {createServer} from "node:http";
import {Session} from "node:inspector";
import {createHash} from "node:crypto";
import {Client} from "../../sdk/typescript/dist/index.js";

let sink=0;
const payload=Buffer.alloc(16384,97);
// Identical native work prevents call-site-dependent JIT loop optimization.
function job(){sink=createHash("sha256").update(payload).digest()[0]}
const server=createServer();await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));const url=`http://127.0.0.1:${server.address().port}`;await new Promise(resolve=>server.close(resolve));
const c=new Client({url,token:"0123456789abcdef",requestTimeoutMs:100});
const e={queue:"q",job_id:"j",state:"started"};const baseline=new Float64Array(1000000),instrumented=new Float64Array(1000000);
for(let i=0;i<10000;i++){job(i);c.emit(e)}
for(let i=0;i<1000000;i++){
  const a=()=>{const t=performance.now();job(i);baseline[i]=(performance.now()-t)*1e6};
  const b=()=>{const t=performance.now();job(i);c.emit(e);instrumented[i]=(performance.now()-t)*1e6};
  if(i%2===0){a();b()}else{b();a()}
}
await c.close();baseline.sort();instrumented.sort();
const p50=100*(instrumented[500000]/baseline[500000]-1),p99=100*(instrumented[990000]/baseline[990000]-1);
// Sample allocations for successful default emits, including transient Date objects.
const session=new Session();session.connect();const post=(method,params={})=>new Promise((resolve,reject)=>session.post(method,params,(err,result)=>err?reject(err):resolve(result)));
const allocationClient=new Client({url,token:"0123456789abcdef"});global.gc?.();
await post("HeapProfiler.startSampling",{samplingInterval:512,includeObjectsCollectedByMajorGC:true,includeObjectsCollectedByMinorGC:true});
for(let i=0;i<10000;i++)allocationClient.emit(e);
const {profile}=await post("HeapProfiler.stopSampling");session.disconnect();
function allocated(node,inside=false){inside ||= node.callFrame.functionName==="emit";return (inside?node.selfSize:0)+node.children.reduce((n,c)=>n+allocated(c,inside),0)}
const bytes=allocated(profile.head)/10000;await allocationClient.close();
console.log(JSON.stringify({jobs:1000000,baseline_p50_ns:baseline[500000],sdk_p50_ns:instrumented[500000],baseline_p99_ns:baseline[990000],sdk_p99_ns:instrumented[990000],added_p50_percent:p50,added_p99_percent:p99,sampled_bytes_per_successful_emit:bytes,stats:c.stats(),sink}));
if(p50>=2 || p99>=5 || bytes>=512 || c.stats().dropped!==1010000)throw new Error("release overhead budget exceeded");
