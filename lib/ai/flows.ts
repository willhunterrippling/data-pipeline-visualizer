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
  refinedFlows?: Array<{
    originalName: string;
    anchorNodePattern?: string; // Updated pattern if needed
    description?: string; // Improved description
    upstreamDepth?: number; // Recommended depth (default 6)
    skipReason?: string; // If set, skip this flow entirely
  }>;
}

/**
 * Predefined flow templates that the AI can refine based on actual graph context
 */
const KNOWN_FLOW_TEMPLATES = [
  {
    name: "Mechanized Outreach",
    anchorPattern: "mart_growth__lsw_lead_data",
    description: "Lead enrichment from Apollo, ZoomInfo, Cognism to LSW Lead Data mart",
    upstreamDepth: 6,
  },
  {
    name: "Bookings Pipeline",
    anchorPattern: "mart_bookings__line_items_final",
    description: "Revenue/ARR calculations through 5-step transformation",
    upstreamDepth: 6,
  },
  {
    name: "Sales Opportunities",
    anchorPattern: "int_sales__opportunities",
    description: "Central opportunity data with product mix, stage history, splits",
    upstreamDepth: 5,
  },
  {
    name: "Automated Intent",
    anchorPattern: "AUTOMATED_INTENT_LEADS|AUTOMATED_INTENT_POPULATION|automated_intent",
    description: "Intent-based lead qualification pipeline processing leads from G2, Gartner, and other intent sources",
    upstreamDepth: 6,
  },
  {
    name: "Job Change",
    anchorPattern: "mart_growth__job_change|JOB_CHANGE_CLAY_RESULTS_RAW_OUTPUT|job_change",
    description: "Employment change signals from Clay integration for prospecting triggers",
    upstreamDepth: 6,
  },
  {
    name: "Direct Mail",
    anchorPattern: "mart_growth__direct_mail_lead_enrichment|direct_mail",
    description: "Physical mail campaign lead enrichment for Lahlouh integration",
    upstreamDepth: 6,
  },
];

const FLOW_PROMPT = `You are an expert data engineer analyzing a data pipeline graph to identify logical data flows.

A "flow" is a connected subgraph that represents a complete data pipeline from sources to a final output table.

You have two tasks:

## Task 1: Refine Predefined Flows
You will be given a list of predefined flows with their anchor patterns. For each one:
- Check if the anchor pattern matches any actual nodes in the graph
- If the pattern doesn't match but you find a similar table, suggest an updated anchorNodePattern
- If you have better context about what the flow does, suggest an improved description
- If the default upstream depth seems wrong based on graph structure, suggest a different upstreamDepth
- If the flow doesn't make sense for this graph, set skipReason to explain why

## Task 2: Discover New Flows
Propose additional flows based on:
1. Known data pipeline patterns (ETL, ELT, dimensional modeling)
2. Business domain patterns (sales pipeline, marketing attribution, revenue reporting)
3. Anchor tables (usually mart_ or rpt_ prefixed models that are the "output" of the flow)

Output a JSON object with:
- "flows": array of NEW flows to add (not refinements of predefined ones)
- "refinedFlows": array of modifications to predefined flows

Each new flow should have:
- name: human-readable flow name
- description: what this flow does
- anchorNodePattern: regex pattern to find the anchor node(s)
- inferenceReason: why this flow was identified

Each refined flow should have:
- originalName: exact name of the predefined flow being refined
- anchorNodePattern: (optional) updated regex pattern if the original doesn't match
- description: (optional) improved description based on actual graph context
- upstreamDepth: (optional) recommended depth if different from default
- skipReason: (optional) if set, this flow will be skipped

Example output:
{
  "flows": [
    {
      "name": "Customer Churn Analysis",
      "description": "Churn prediction pipeline from usage metrics to churn scores",
      "anchorNodePattern": "mart_cx__churn",
      "inferenceReason": "Detected customer experience churn models with usage data upstream"
    }
  ],
  "refinedFlows": [
    {
      "originalName": "Mechanized Outreach",
      "description": "Lead enrichment and scoring pipeline from Apollo, ZoomInfo, Cognism, Clearbit to unified lead database",
      "upstreamDepth": 8
    },
    {
      "originalName": "Job Change",
      "anchorNodePattern": "mart_growth__job_change_signals",
      "description": "Real-time employment change detection from Clay and LinkedIn for sales triggers"
    }
  ]
}`;

export interface FlowProgressCallback {
  (progress: number, message: string): void;
}

/**
 * Use AI to propose flows based on graph structure
 */
export async function proposeFlows(
  nodes: GraphNode[],
  edges: GraphEdge[],
  onProgress?: FlowProgressCallback
): Promise<GraphFlow[]> {
  onProgress?.(0, `Analyzing ${nodes.length} nodes for flow patterns...`);

  // Find potential anchor nodes (marts, reports)
  const anchorCandidates = nodes.filter(
    (n) =>
      n.name.startsWith("mart_") ||
      n.name.startsWith("rpt_") ||
      n.metadata?.tags?.includes("p1")
  );

  onProgress?.(5, `Found ${anchorCandidates.length} potential anchor tables (marts/reports)...`);

  // Build adjacency for traversal (both directions)
  const upstreamMap = new Map<string, Set<string>>();
  const downstreamMap = new Map<string, Set<string>>();
  for (const edge of edges) {
    // Upstream: to -> from (what feeds into a node)
    if (!upstreamMap.has(edge.to)) {
      upstreamMap.set(edge.to, new Set());
    }
    upstreamMap.get(edge.to)!.add(edge.from);
    
    // Downstream: from -> to (what a node feeds into)
    if (!downstreamMap.has(edge.from)) {
      downstreamMap.set(edge.from, new Set());
    }
    downstreamMap.get(edge.from)!.add(edge.to);
  }

  onProgress?.(10, "Building dependency graph for flow traversal...");

  // Get lineage info for each anchor
  const anchorInfo = anchorCandidates.map((anchor) => {
    const upstream = getUpstreamNodes(anchor.id, upstreamMap);
    const downstream = getDownstreamNodes(anchor.id, downstreamMap);
    return {
      name: anchor.name,
      upstreamCount: upstream.size,
      downstreamCount: downstream.size,
      upstreamSample: [...upstream].slice(0, 10),
    };
  });

  // Check which predefined flows have matching anchors
  const predefinedFlowStatus = KNOWN_FLOW_TEMPLATES.map((template) => {
    const pattern = new RegExp(template.anchorPattern, "i");
    const matchingNodes = nodes.filter((n) => pattern.test(n.name));
    return {
      name: template.name,
      anchorPattern: template.anchorPattern,
      description: template.description,
      upstreamDepth: template.upstreamDepth,
      matchingNodes: matchingNodes.map((n) => n.name).slice(0, 5),
      hasMatch: matchingNodes.length > 0,
    };
  });

  const matchedFlows = predefinedFlowStatus.filter(f => f.hasMatch).length;
  onProgress?.(15, `Checking ${KNOWN_FLOW_TEMPLATES.length} known flow patterns (${matchedFlows} matched)...`);

  const userMessage = `Analyze this data pipeline and propose logical flows.

Found ${nodes.length} total nodes, ${edges.length} edges.

## Predefined Flows to Refine
These are predefined flows. Check if they match actual nodes and suggest refinements:
${JSON.stringify(predefinedFlowStatus, null, 2)}

## Potential Anchor Tables
These mart/report tables could be anchors for new flows:
${JSON.stringify(anchorInfo.slice(0, 30), null, 2)}

Please:
1. For each predefined flow, suggest refinements if needed (updated patterns, descriptions, or skip reasons)
2. Propose 2-4 additional key flows that would help users understand other major data pipelines`;

  const promptSize = (FLOW_PROMPT.length + userMessage.length) / 1024;
  onProgress?.(20, `Sending AI request (${promptSize.toFixed(1)}KB prompt)...`);

  const startTime = Date.now();

  try {
    onProgress?.(25, "Waiting for AI response...");

    const result = await completeJson<FlowProposalResult>([
      { role: "system", content: FLOW_PROMPT },
      { role: "user", content: userMessage },
    ]);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const newFlowCount = result.flows?.length || 0;
    const refinedCount = result.refinedFlows?.length || 0;
    onProgress?.(50, `AI response in ${elapsed}s: ${newFlowCount} new flows, ${refinedCount} refinements`);

    const flows: GraphFlow[] = [];

    // Build a map of refinements by original name
    type RefinedFlow = NonNullable<typeof result.refinedFlows>[number];
    const refinements = new Map<string, RefinedFlow>();
    if (result.refinedFlows) {
      for (const refined of result.refinedFlows) {
        refinements.set(refined.originalName, refined);
      }
    }

    onProgress?.(60, "Processing predefined flow templates...");

    // Process predefined flows with AI refinements
    for (const template of KNOWN_FLOW_TEMPLATES) {
      const refinement = refinements.get(template.name);

      // Skip if AI says to skip
      if (refinement?.skipReason) {
        console.log(`Skipping flow "${template.name}": ${refinement.skipReason}`);
        continue;
      }

      // Use refined pattern or original
      const patternStr = refinement?.anchorNodePattern || template.anchorPattern;
      const pattern = new RegExp(patternStr, "i");
      const anchors = nodes.filter((n) => pattern.test(n.name));

      if (anchors.length === 0) continue;

      // Get all upstream AND downstream nodes for the flow (full lineage)
      const memberSet = new Set<string>();
      for (const anchor of anchors) {
        memberSet.add(anchor.id);
        const upstream = getUpstreamNodes(anchor.id, upstreamMap);
        upstream.forEach((id) => memberSet.add(id));
        const downstream = getDownstreamNodes(anchor.id, downstreamMap);
        downstream.forEach((id) => memberSet.add(id));
      }

      // Use refined description or original
      const description = refinement?.description || template.description;

      flows.push({
        id: uuid(),
        name: template.name,
        description,
        anchorNodes: anchors.map((a) => a.id),
        memberNodes: [...memberSet],
        userDefined: false,
        inferenceReason: refinement
          ? `AI-refined: ${refinement.anchorNodePattern ? "updated anchor pattern" : ""}${refinement.description ? " improved description" : ""}${refinement.upstreamDepth ? ` depth=${depth}` : ""}`.trim() || "AI-validated predefined flow"
          : `Predefined flow with anchor pattern: ${template.anchorPattern}`,
      });
    }

    onProgress?.(75, `Created ${flows.length} predefined flows, processing AI proposals...`);

    // Process new AI-proposed flows
    for (const proposal of result.flows || []) {
      // Skip if we already have a flow with this name
      if (flows.some((f) => f.name === proposal.name)) continue;

      // Find anchor node(s) matching pattern
      const pattern = new RegExp(proposal.anchorNodePattern, "i");
      const anchors = nodes.filter((n) => pattern.test(n.name));

      if (anchors.length === 0) continue;

      // Get all upstream AND downstream nodes for the flow (full lineage)
      const memberSet = new Set<string>();
      for (const anchor of anchors) {
        memberSet.add(anchor.id);
        const upstream = getUpstreamNodes(anchor.id, upstreamMap);
        upstream.forEach((id) => memberSet.add(id));
        const downstream = getDownstreamNodes(anchor.id, downstreamMap);
        downstream.forEach((id) => memberSet.add(id));
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

    onProgress?.(90, `Finalized ${flows.length} total flows`);

    return flows;
  } catch (error) {
    console.error("AI flow proposal failed, using fallback:", error);
    onProgress?.(50, "AI request failed, using predefined flows...");
    return getKnownFlows(nodes, upstreamMap, downstreamMap);
  }
}

/**
 * Get known flows based on common patterns
 */
function getKnownFlows(
  nodes: GraphNode[],
  upstreamMap: Map<string, Set<string>>,
  downstreamMap: Map<string, Set<string>>
): GraphFlow[] {
  const flows: GraphFlow[] = [];

  // Helper to get full lineage (upstream + downstream)
  const getFullLineage = (anchorId: string): Set<string> => {
    const members = new Set([anchorId]);
    getUpstreamNodes(anchorId, upstreamMap).forEach((id) => members.add(id));
    getDownstreamNodes(anchorId, downstreamMap).forEach((id) => members.add(id));
    return members;
  };

  // Mechanized Outreach
  const moAnchor = nodes.find((n) => n.name === "mart_growth__lsw_lead_data");
  if (moAnchor) {
    flows.push({
      id: uuid(),
      name: "Mechanized Outreach",
      description: "Lead enrichment from Apollo, ZoomInfo, Cognism to LSW Lead Data mart",
      anchorNodes: [moAnchor.id],
      memberNodes: [...getFullLineage(moAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected mart_growth__lsw_lead_data as anchor with MECH_OUTREACH sources upstream",
    });
  }

  // Bookings Pipeline
  const bookingsAnchor = nodes.find((n) => n.name === "mart_bookings__line_items_final");
  if (bookingsAnchor) {
    flows.push({
      id: uuid(),
      name: "Bookings Pipeline",
      description: "Revenue/ARR calculations through 5-step transformation",
      anchorNodes: [bookingsAnchor.id],
      memberNodes: [...getFullLineage(bookingsAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected mart_bookings__line_items_final as P1 priority hourly model",
    });
  }

  // Sales Opportunities
  const salesAnchor = nodes.find((n) => n.name === "int_sales__opportunities");
  if (salesAnchor) {
    flows.push({
      id: uuid(),
      name: "Sales Opportunities",
      description: "Central opportunity data with product mix, stage history, splits",
      anchorNodes: [salesAnchor.id],
      memberNodes: [...getFullLineage(salesAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected int_sales__opportunities as central sales model",
    });
  }

  // Automated Intent
  const aiAnchor = nodes.find(
    (n) =>
      n.name === "AUTOMATED_INTENT_LEADS" ||
      n.name === "AUTOMATED_INTENT_POPULATION" ||
      n.name.toLowerCase().includes("automated_intent")
  );
  if (aiAnchor) {
    flows.push({
      id: uuid(),
      name: "Automated Intent",
      description: "Intent-based lead qualification pipeline processing leads from G2, Gartner, and other intent sources",
      anchorNodes: [aiAnchor.id],
      memberNodes: [...getFullLineage(aiAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected AUTOMATED_INTENT tables as anchor for intent-based lead qualification",
    });
  }

  // Job Change
  const jobChangeAnchor = nodes.find(
    (n) =>
      n.name.toLowerCase().includes("mart_growth__job_change") ||
      n.name === "JOB_CHANGE_CLAY_RESULTS_RAW_OUTPUT" ||
      n.name.toLowerCase().includes("job_change")
  );
  if (jobChangeAnchor) {
    flows.push({
      id: uuid(),
      name: "Job Change",
      description: "Employment change signals from Clay integration for prospecting triggers",
      anchorNodes: [jobChangeAnchor.id],
      memberNodes: [...getFullLineage(jobChangeAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected job change tables as anchor for employment change signal processing",
    });
  }

  // Direct Mail
  const directMailAnchor = nodes.find(
    (n) =>
      n.name === "mart_growth__direct_mail_lead_enrichment" ||
      n.name.toLowerCase().includes("direct_mail")
  );
  if (directMailAnchor) {
    flows.push({
      id: uuid(),
      name: "Direct Mail",
      description: "Physical mail campaign lead enrichment for Lahlouh integration",
      anchorNodes: [directMailAnchor.id],
      memberNodes: [...getFullLineage(directMailAnchor.id)],
      userDefined: false,
      inferenceReason: "Detected direct mail lead enrichment table as anchor for physical mail campaigns",
    });
  }

  return flows;
}

/**
 * Traverse upstream from a node (full lineage, no depth limit)
 */
function getUpstreamNodes(
  nodeId: string,
  upstreamMap: Map<string, Set<string>>,
  visited = new Set<string>()
): Set<string> {
  if (visited.has(nodeId)) {
    return new Set();
  }
  visited.add(nodeId);

  const result = new Set<string>();
  const upstream = upstreamMap.get(nodeId) || new Set();

  for (const upId of upstream) {
    result.add(upId);
    const deeper = getUpstreamNodes(upId, upstreamMap, visited);
    deeper.forEach((id) => result.add(id));
  }

  return result;
}

/**
 * Traverse downstream from a node (full lineage, no depth limit)
 */
function getDownstreamNodes(
  nodeId: string,
  downstreamMap: Map<string, Set<string>>,
  visited = new Set<string>()
): Set<string> {
  if (visited.has(nodeId)) {
    return new Set();
  }
  visited.add(nodeId);

  const result = new Set<string>();
  const downstream = downstreamMap.get(nodeId) || new Set();

  for (const downId of downstream) {
    result.add(downId);
    const deeper = getDownstreamNodes(downId, downstreamMap, visited);
    deeper.forEach((id) => result.add(id));
  }

  return result;
}

