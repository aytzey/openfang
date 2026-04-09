/**
 * Basic example — inspect sales state through the public REST client.
 *
 * Usage:
 *   node basic.js
 */

const { PulsivoSalesman } = require("../index");

async function main() {
  const client = new PulsivoSalesman("http://localhost:4200");

  // Check server health
  const health = await client.health();
  console.log("Server:", health);

  // Inspect the current B2C profile
  const profile = await client.sales.getProfile("b2c");
  console.log("B2C profile:", profile);

  // Fetch the latest recent runs
  const runs = await client.sales.listRuns({ segment: "b2c", limit: 5 });
  console.log("Recent runs:", runs);
}

main().catch(console.error);
