import { NextRequest, NextResponse } from "next/server";
import { getNodeById, getCitationsForNode } from "@/lib/db";
import { extractColumnLineage } from "@/lib/parsers/sqlParser";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { nodeId, column } = body as { nodeId: string; column?: string };

    if (!nodeId) {
      return NextResponse.json({ error: "nodeId is required" }, { status: 400 });
    }

    const dbNode = getNodeById(nodeId);
    if (!dbNode) {
      return NextResponse.json({ error: "Node not found" }, { status: 404 });
    }

    const metadata = dbNode.metadata ? JSON.parse(dbNode.metadata) : {};
    const filePath = metadata.filePath;

    if (!filePath) {
      return NextResponse.json({
        nodeId,
        column,
        lineage: [],
        message: "No source file available for this node",
      });
    }

    // Try to find the SQL file
    const dbtPath = process.env.RIPPLING_DBT_PATH?.replace("~", process.env.HOME || "") || "";
    const fullPath = filePath.startsWith("/") ? filePath : join(dbtPath, filePath);

    if (!existsSync(fullPath)) {
      return NextResponse.json({
        nodeId,
        column,
        lineage: [],
        message: "Source file not found",
      });
    }

    const sql = readFileSync(fullPath, "utf-8");
    const columnLineage = extractColumnLineage(sql);

    // If specific column requested, filter
    if (column) {
      const sources = columnLineage.get(column) || columnLineage.get(column.toUpperCase()) || [];
      return NextResponse.json({
        nodeId,
        column,
        lineage: sources,
      });
    }

    // Return all column lineage
    const allLineage: Record<string, { sourceTable: string; sourceColumn: string }[]> = {};
    for (const [col, sources] of columnLineage) {
      allLineage[col] = sources;
    }

    return NextResponse.json({
      nodeId,
      lineage: allLineage,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

