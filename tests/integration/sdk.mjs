import {Client} from "../../sdk/typescript/dist/index.js";
const c=new Client({url:process.env.QUEUELENS_TEST_COLLECTOR_URL,token:process.env.QUEUELENS_TEST_COLLECTOR_TOKEN,workerId:process.env.QUEUELENS_TEST_WORKER_ID});
for(let i=0;i<40;i++)c.emit({queue:"m2-sdk",job_id:`ts-${i}`,state:"completed",duration_ms:1});
await c.close();
if(c.stats().sent!==40 || c.stats().dropped!==0 || c.stats().heartbeats<1)throw new Error("SDK delivery failed");
console.log(JSON.stringify(c.stats()));
