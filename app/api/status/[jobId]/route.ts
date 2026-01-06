import { getJob } from "@/lib/jobs/jobStore";

export async function GET(
  _: Request,
  { params }: { params: { jobId: string } }
) {
  const job = getJob(params.jobId);
  if (!job) {
    return new Response("Not found", { status: 404 });
  }
  return Response.json(job);
}
