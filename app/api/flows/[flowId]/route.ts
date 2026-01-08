import { NextRequest, NextResponse } from "next/server";
import { getFlows, insertFlow, getNodeById } from "@/lib/db";
import { buildFlowMembers } from "../route";
import type { GraphFlow } from "@/lib/types";

// GET a specific flow
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ flowId: string }> }
) {
  try {
    const { flowId } = await params;
    const dbFlows = getFlows();
    const dbFlow = dbFlows.find((f) => f.id === flowId);

    if (!dbFlow) {
      return NextResponse.json({ error: "Flow not found" }, { status: 404 });
    }

    const flow: GraphFlow = {
      id: dbFlow.id,
      name: dbFlow.name,
      description: dbFlow.description || undefined,
      anchorNodes: dbFlow.anchor_nodes ? JSON.parse(dbFlow.anchor_nodes) : [],
      memberNodes: dbFlow.member_nodes ? JSON.parse(dbFlow.member_nodes) : [],
      userDefined: dbFlow.user_defined === 1,
      inferenceReason: dbFlow.inference_reason || undefined,
    };

    return NextResponse.json(flow);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

// PATCH update a flow's anchor node
export async function PATCH(
  request: NextRequest,
  { params }: { params: Promise<{ flowId: string }> }
) {
  try {
    const { flowId } = await params;
    const body = await request.json();
    const { anchorNodeId } = body as { anchorNodeId: string };

    if (!anchorNodeId) {
      return NextResponse.json({ error: "anchorNodeId is required" }, { status: 400 });
    }

    // Get existing flow
    const dbFlows = getFlows();
    const dbFlow = dbFlows.find((f) => f.id === flowId);

    if (!dbFlow) {
      return NextResponse.json({ error: "Flow not found" }, { status: 404 });
    }

    // Validate anchor node exists
    const anchorNode = getNodeById(anchorNodeId);
    if (!anchorNode) {
      return NextResponse.json({ error: "Anchor node not found" }, { status: 400 });
    }

    // Rebuild member nodes by traversing upstream from new anchor
    const memberNodes = buildFlowMembers(anchorNodeId);

    // Update flow
    insertFlow({
      id: flowId,
      name: dbFlow.name,
      description: dbFlow.description,
      anchor_nodes: JSON.stringify([anchorNodeId]),
      member_nodes: JSON.stringify(memberNodes),
      user_defined: dbFlow.user_defined,
      inference_reason: `Updated anchor to: ${anchorNode.name}`,
    });

    const updatedFlow: GraphFlow = {
      id: flowId,
      name: dbFlow.name,
      description: dbFlow.description || undefined,
      anchorNodes: [anchorNodeId],
      memberNodes,
      userDefined: dbFlow.user_defined === 1,
      inferenceReason: `Updated anchor to: ${anchorNode.name}`,
    };

    return NextResponse.json(updatedFlow);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
