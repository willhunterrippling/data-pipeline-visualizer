import { createJob, updateJob } from "@/lib/jobs/jobStore";

export async function POST() {
  const job = createJob();

  // fire-and-forget background work
  setTimeout(async () => {
    updateJob(job.id, {
      status: "running",
      stage: "initializing",
      message: "Starting repo and Snowflake analysis",
    });

    // placeholder — real worker later
    await new Promise((r) => setTimeout(r, 5000));

    updateJob(job.id, {
      status: "completed",
      stage: "done",
      message: "Graph built successfully",
    });
  }, 0);

  return Response.json({ jobId: job.id });
}
