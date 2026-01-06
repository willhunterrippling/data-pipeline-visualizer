import { NextRequest, NextResponse } from "next/server";
import { getJob, getActivityLog, getUsageStats } from "@/lib/db";
import { INDEXING_STAGES } from "@/lib/types";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ jobId: string }> }
) {
  const { jobId } = await params;
  const job = getJob(jobId);

  if (!job) {
    return NextResponse.json({ error: "Job not found" }, { status: 404 });
  }

  // Calculate overall progress based on stage
  let overallProgress = 0;
  if (job.status === "completed") {
    overallProgress = 100;
  } else if (job.stage) {
    const stageInfo = INDEXING_STAGES.find((s) => s.id === job.stage);
    if (stageInfo) {
      const stageRange = stageInfo.endPct - stageInfo.startPct;
      overallProgress = stageInfo.startPct + (stageRange * job.stage_progress) / 100;
    }
  }

  // Get activity log (last 15 entries for UI)
  const activityLog = getActivityLog(jobId).slice(-15);

  // Get usage stats (only relevant when completed)
  const usageStats = job.status === "completed" ? getUsageStats(jobId) : null;

  return NextResponse.json({
    id: job.id,
    status: job.status,
    stage: job.stage,
    stageName: INDEXING_STAGES.find((s) => s.id === job.stage)?.name,
    stageProgress: job.stage_progress,
    overallProgress: Math.round(overallProgress),
    message: job.message,
    error: job.error,
    activityLog,
    usageStats,
    startedAt: job.started_at,
    updatedAt: job.updated_at,
  });
}
