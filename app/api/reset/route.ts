import { NextResponse } from "next/server";
import { clearAllData } from "@/lib/db";

export async function POST() {
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

