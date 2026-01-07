import { NextResponse } from "next/server";
import { clearLineageCache } from "@/lib/db";

export async function POST() {
  try {
    clearLineageCache();
    return NextResponse.json({ success: true, message: "Lineage cache cleared" });
  } catch (error) {
    console.error("Failed to clear lineage cache:", error);
    return NextResponse.json(
      { success: false, error: "Failed to clear lineage cache" },
      { status: 500 }
    );
  }
}

