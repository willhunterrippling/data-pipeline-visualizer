import { NextResponse } from "next/server";
import { v4 as uuid } from "uuid";
import { createJob, getDb, isStaticMode } from "@/lib/db";
import { Indexer, IndexerConfig } from "@/lib/indexer";

export async function POST() {
  // Disable indexing in production (static mode)
  if (isStaticMode()) {
    return NextResponse.json(
      { error: "Indexing is not available in production. Run locally with npm run dev." },
      { status: 405 }
    );
  }

  // Initialize database
  getDb();

  const jobId = uuid();
  createJob(jobId);

  const config: IndexerConfig = {
    dbtPath: process.env.RIPPLING_DBT_PATH || "~/Documents/GitHub/rippling-dbt",
    airflowPath: process.env.AIRFLOW_DAGS_PATH || "~/Documents/GitHub/airflow-dags",
    snowflakeEnabled: false, // Disabled for MVP Day 1
  };

  // Expand ~ to home directory
  if (config.dbtPath.startsWith("~")) {
    config.dbtPath = config.dbtPath.replace("~", process.env.HOME || "");
  }
  if (config.airflowPath.startsWith("~")) {
    config.airflowPath = config.airflowPath.replace("~", process.env.HOME || "");
  }

  // Start indexing in background using setImmediate to break Promise chain
  // This allows the HTTP response to be sent before the indexer starts
  const indexer = new Indexer(jobId, config);
  setImmediate(() => {
    indexer.run().catch((error) => {
      console.error("Indexing failed:", error);
    });
  });

  return NextResponse.json({ jobId });
}
