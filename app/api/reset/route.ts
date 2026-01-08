import { NextResponse } from "next/server";
import { clearAllData, isStaticMode } from "@/lib/db";

export async function POST() {
  // Disable reset in production (static mode)
  if (isStaticMode()) {
    return NextResponse.json(
      { error: "Database reset is not available in production." },
      { status: 405 }
    );
  }

  try {
    clearAllData();
    return NextResponse.json({ success: true, message: "All data has been cleared" });
  } catch (error) {
    console.error("Failed to reset database:", error);
    return NextResponse.json(
      { success: false, error: "Failed to reset database" },
      { status: 500 }
    );
  }
}

