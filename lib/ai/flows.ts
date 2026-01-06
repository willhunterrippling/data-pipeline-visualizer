import { completeJson } from "./client";
import type { GraphNode, GraphEdge, GraphFlow } from "../types";
import { v4 as uuid } from "uuid";

interface FlowProposalResult {
  flows: Array<{
    name: string;
    description: string;
    anchorNodePattern: string; // Regex or name to find anchor
    inferenceReason: string;
  }>;
}

const FLOW_PROMPT = `You are an expert data engineer analyzing a data pipeline graph to identify logical data flows.

A "flow" is a connected subgraph that represents a complete data pipeline from sources to a final output table.

Given information about the nodes and their relationships, propose flows based on:
1. Known data pipeline patterns (ETL, ELT, dimensional modeling)
2. Business domain patterns (sales pipeline, marketing attribution, revenue reporting)
3. Anchor tables (usually mart_ or rpt_ prefixed models that are the "output" of the flow)

Output a JSON object with a "flows" array. Each flow should have:
- name: human-readable flow name
- description: what this flow does
- anchorNodePattern: regex pattern to find the anchor node(s)
- inferenceReason: why this flow was identified

Example output:
{
  "flows": [
    {
      "name": "Mechanized Outreach",
      "description": "Lead enrichment pipeline from Apollo, ZoomInfo to LSW Lead Data",
      "anchorNodePattern": "mart_growth__lsw_lead",
      "inferenceReason": "Detected mart_growth__lsw_lead_data as anchor with mech_outreach sources upstream"
    }
  ]
}`;

/**
 * Use AI to propose flows based on graph structure
 */
export async function proposeFlows(
  nodes: GraphNode[],
  edges: GraphEdge[]
): Promise<GraphFlow[]> {
  // Find potential anchor nodes (marts, reports)
  const anchorCandidates = nodes.filter(
    (n) =>
      n.name.startsWith("mart_") ||
      n.name.startsWith("rpt_") ||
      n.metadata?.tags?.includes("p1")
  );

  // Build adjacency for traversal
  const upstreamMap = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!upstreamMap.has(edge.to)) {
      upstreamMap.set(edge.to, new Set());
    }
    upstreamMap.get(edge.to)!.add(edge.from);
  }

  // Get upstream depth for each anchor
  const anchorInfo = anchorCandidates.map((anchor) => {
    const upstream = getUpstreamNodes(anchor.id, upstreamMap, 5);
    return {
      name: anchor.name,
      upstreamCount: upstream.size,
      upstreamSample: [...upstream].slice(0, 10),
    };
  });

  const userMessage = `Analyze this data pipeline and propose logical flows.

Found ${nodes.length} total nodes, ${edges.length} edges.

Potential anchor tables (marts/reports):
${JSON.stringify(anchorInfo, null, 2)}

Propose 3-5 key flows that would help users understand the major data pipelines.`;

  try {
    const result = await completeJson<FlowProposalResult>([
      { role: "system", content: FLOW_PROMPT },
      { role: "user", content: userMessage },
    ]);

    const flows: GraphFlow[] = [];

    for (const proposal of result.flows) {
      // Find anchor node(s) matching pattern
      const pattern = new RegExp(proposal.anchorNodePattern, "i");
      const anchors = nodes.filter((n) => pattern.test(n.name));

      if (anchors.length === 0) continue;

      // Get all upstream nodes for the flow
      const memberSet = new Set<string>();
      for (const anchor of anchors) {
        memberSet.add(anchor.id);
        const upstream = getUpstreamNodes(anchor.id, upstreamMap, 6);
        upstream.forEach((id) => memberSet.add(id));
      }

      flows.push({
        id: uuid(),
        name: proposal.name,
        description: proposal.description,
        anchorNodes: anchors.map((a) => a.id),
        memberNodes: [...memberSet],
        userDefined: false,
        inferenceReason: proposal.inferenceReason,
      });
    }

    // Add known flows if not already present
    const knownFlows = getKnownFlows(nodes, upstreamMap);
    for (const known of knownFlows) {
      if (!flows.some((f) => f.name === known.name)) {
        flows.push(known);
      }
    }

    return flows;
  } catch (error) {
    console.error("AI flow proposal failed, using fallback:", error);
    return getKnownFlows(nodes, upstreamMap);
  }
}

/**
 * Get known flows based on common patterns
 */
function getKnownFlows(
  nodes: GraphNode[],
  upstreamMap: Map<string, Set<string>>
): GraphFlow[] {
  const flows: GraphFlow[] = [];

  // Mechanized Outreach
  const moAnchor = nodes.find((n) => n.name === "mart_growth__lsw_lead_data");
  if (moAnchor) {
    const members = new Set([moAnchor.id]);
    getUpstreamNodes(moAnchor.id, upstreamMap, 6).forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Mechanized Outreach",
      description: "Lead enrichment from Apollo, ZoomInfo, Cognism to LSW Lead Data mart",
      anchorNodes: [moAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected mart_growth__lsw_lead_data as anchor with MECH_OUTREACH sources upstream",
    });
  }

  // Bookings Pipeline
  const bookingsAnchor = nodes.find((n) => n.name === "mart_bookings__line_items_final");
  if (bookingsAnchor) {
    const members = new Set([bookingsAnchor.id]);
    getUpstreamNodes(bookingsAnchor.id, upstreamMap, 6).forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Bookings Pipeline",
      description: "Revenue/ARR calculations through 5-step transformation",
      anchorNodes: [bookingsAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected mart_bookings__line_items_final as P1 priority hourly model",
    });
  }

  // Sales Opportunities
  const salesAnchor = nodes.find((n) => n.name === "int_sales__opportunities");
  if (salesAnchor) {
    const members = new Set([salesAnchor.id]);
    getUpstreamNodes(salesAnchor.id, upstreamMap, 5).forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Sales Opportunities",
      description: "Central opportunity data with product mix, stage history, splits",
      anchorNodes: [salesAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected int_sales__opportunities as central sales model",
    });
  }

  return flows;
}

/**
 * Traverse upstream from a node
 */
function getUpstreamNodes(
  nodeId: string,
  upstreamMap: Map<string, Set<string>>,
  maxDepth: number,
  visited = new Set<string>()
): Set<string> {
  if (maxDepth === 0 || visited.has(nodeId)) {
    return new Set();
  }
  visited.add(nodeId);

  const result = new Set<string>();
  const upstream = upstreamMap.get(nodeId) || new Set();

  for (const upId of upstream) {
    result.add(upId);
    const deeper = getUpstreamNodes(upId, upstreamMap, maxDepth - 1, visited);
    deeper.forEach((id) => result.add(id));
  }

  return result;
}

