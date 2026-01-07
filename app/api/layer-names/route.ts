import { NextResponse } from "next/server";
import { getLayerNames } from "@/lib/db";

export async function GET() {
  try {
    const layerNames = getLayerNames();

    const layers = layerNames.map((ln) => ({
      layerNumber: ln.layer_number,
      name: ln.name,
      description: ln.description,
      nodeCount: ln.node_count,
      sampleNodes: ln.sample_nodes ? JSON.parse(ln.sample_nodes) : [],
      inferenceReason: ln.inference_reason,
    }));

    return NextResponse.json({
      layers,
      total: layers.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

