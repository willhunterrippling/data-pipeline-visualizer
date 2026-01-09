import { NextRequest, NextResponse } from "next/server";
import { getJob, submitSchemaSelection } from "@/lib/db";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { jobId, selectedSchemas } = body;

    if (!jobId) {
      return NextResponse.json(
        { error: "Missing jobId" },
        { status: 400 }
      );
    }

    if (!selectedSchemas || !Array.isArray(selectedSchemas)) {
      return NextResponse.json(
        { error: "Missing or invalid selectedSchemas" },
        { status: 400 }
      );
    }

    // Verify job exists and is waiting for schema selection
    const job = getJob(jobId);
    if (!job) {
      return NextResponse.json(
        { error: "Job not found" },
        { status: 404 }
      );
    }

    if (job.status !== "waiting_for_input" || job.waiting_for !== "schema_selection") {
      return NextResponse.json(
        { error: "Job is not waiting for schema selection" },
        { status: 400 }
      );
    }

    // Submit the selection - this will resume the indexer
    submitSchemaSelection(jobId, selectedSchemas);

    return NextResponse.json({
      success: true,
      selectedSchemas,
      message: `Selected ${selectedSchemas.length} schemas`,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json(
      { error: `Failed to submit schema selection: ${message}` },
      { status: 500 }
    );
  }
}

