/**
 * Active job polling example — monitor the currently running B2C job.
 *
 * Usage:
 *   node streaming.js
 */

const { PulsivoSalesman } = require("../index");

async function main() {
  const client = new PulsivoSalesman("http://localhost:4200");

  const active = await client.sales.getActiveJob("b2c");
  if (!active.job) {
    console.log("No active B2C job.");
    return;
  }

  const jobId = active.job.job_id || active.job.id;
  console.log("Polling job:", jobId);

  for (let attempt = 0; attempt < 10; attempt += 1) {
    const progress = await client.sales.getJob(jobId);
    console.log(progress);
    if (progress.status && progress.status !== "running") {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

main().catch(console.error);
