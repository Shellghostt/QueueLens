// Seeds a real BullMQ queue and prints what BullMQ itself thinks the counts
// are.
//
// The Go integration test compares the QueueLens BullMQ adapter against this
// output. That is the point of the fixture: the adapter reads Redis keys
// directly, and this script proves that the key layout it assumes is the one
// BullMQ 6.x actually writes.
//
// Usage:
//
//	REDIS_HOST=127.0.0.1 REDIS_PORT=56379 node seed.js
//
// The script prints one JSON object on stdout and nothing else.

const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");

const host = process.env.REDIS_HOST || "127.0.0.1";
const port = Number(process.env.REDIS_PORT || 6379);
const db = Number(process.env.REDIS_DB || 0);

const connection = { host, port, db, maxRetriesPerRequest: null };

async function main() {
  const redis = new IORedis(connection);

  // Start from a clean slate so the counts are deterministic.
  const keys = await redis.keys("bull:*");
  if (keys.length > 0) {
    await redis.del(...keys);
  }

  const emails = new Queue("emails", { connection });
  const reports = new Queue("reports", { connection });

  // A worker that stays busy: one job is active, the rest wait.
  const slowWorker = new Worker(
    "emails",
    async () => {
      await new Promise((resolve) => setTimeout(resolve, 120 * 1000));
    },
    { connection, concurrency: 1 },
  );
  await slowWorker.waitUntilReady();

  await emails.add("send-welcome", { i: 0 }, { removeOnComplete: false, removeOnFail: false });
  await new Promise((resolve) => setTimeout(resolve, 750));
  for (let i = 0; i < 3; i += 1) {
    await emails.add("send-welcome", { i }, { removeOnComplete: false, removeOnFail: false });
  }
  await emails.add("send-reminder", { i: 0 }, { delay: 60 * 60 * 1000, removeOnComplete: false, removeOnFail: false });

  // A worker that completes one job and fails another.
  const reportsWorker = new Worker(
    "reports",
    async (job) => {
      if (job.name === "send-invoice") {
        throw new Error("SMTP timeout");
      }
      return { ok: true };
    },
    { connection, concurrency: 1 },
  );
  await reportsWorker.waitUntilReady();

  await reports.add("send-report", { i: 0 }, { removeOnComplete: false, removeOnFail: false });
  await reports.add("send-invoice", { i: 1 }, { removeOnComplete: false, removeOnFail: false });
  await new Promise((resolve) => setTimeout(resolve, 2500));

  const result = {
    emails: await emails.getJobCounts(),
    reports: await reports.getJobCounts(),
  };

  // The state is already in Redis. Exiting straight away keeps the fixture
  // fast: the slow worker is still holding a job, and that is exactly the
  // state the test expects to find.
  process.stdout.write(`${JSON.stringify(result)}\n`, () => {
    process.exit(0);
  });
}

main().catch((error) => {
  process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
  process.exit(1);
});
