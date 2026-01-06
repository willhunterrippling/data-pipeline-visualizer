import { v4 as uuid } from "uuid";
import { spawn } from "child_process";
import { join } from "path";
import {
  getDb,
  insertNodes,
  insertEdges,
  insertCitations,
  insertGroups,
  insertFlow,
  insertExplanation,
  updateJob,
  clearAllData,
  updateGroupNodeCounts,
  appendActivityLog,
  updateUsageStats,
  DbNode,
  DbEdge,
  DbCitation,
} from "../db";
import { parseDbtManifest, findManifestPath, parseDbtProjectFallback } from "./dbtParser";
import { parseAirflowDags } from "./airflowParser";
import { linkCrossRepo } from "./linker";
import { enrichWithSnowflakeMetadata } from "./snowflakeMetadata";
import { inferGroups as aiInferGroups } from "../ai/grouping";
import { proposeFlows as aiProposeFlows } from "../ai/flows";
import { batchExplainNodes } from "../ai/explain";
import { resetUsageTracking, getUsageStats as getAiUsageStats } from "../ai/client";
import type { GraphNode, GraphEdge, Citation, IndexingStageId } from "../types";

export interface IndexerConfig {
  dbtPath: string;
  airflowPath: string;
  snowflakeEnabled: boolean;
}

// Convert domain types to DB types
function graphNodeToDb(node: GraphNode): Omit<DbNode, "created_at"> {
  return {
    id: node.id,
    name: node.name,
    type: node.type,
    subtype: node.subtype || null,
    group_id: node.groupId || null,
    repo: node.repo || null,
    metadata: node.metadata ? JSON.stringify(node.metadata) : null,
  };
}

function graphEdgeToDb(edge: GraphEdge): DbEdge {
  return {
    id: edge.id,
    from_node: edge.from,
    to_node: edge.to,
    type: edge.type,
    metadata: edge.metadata ? JSON.stringify(edge.metadata) : null,
  };
}

function citationToDb(citation: Citation): DbCitation {
  return {
    id: citation.id,
    node_id: citation.nodeId || null,
    edge_id: citation.edgeId || null,
    file_path: citation.filePath,
    start_line: citation.startLine || null,
    end_line: citation.endLine || null,
    snippet: citation.snippet || null,
  };
}

export class Indexer {
  private jobId: string;
  private config: IndexerConfig;
  private allNodes: GraphNode[] = [];
  private allEdges: GraphEdge[] = [];
  private allCitations: Citation[] = [];
  private stageStartTime: number = 0;
  private currentStage: IndexingStageId | null = null;

  constructor(jobId: string, config: IndexerConfig) {
    this.jobId = jobId;
    this.config = config;
  }

  private log(message: string, stage?: IndexingStageId) {
    const stageLabel = stage || this.currentStage || "init";
    const logMessage = `[indexer:${stageLabel}] ${message}`;
    console.log(logMessage);
    appendActivityLog(this.jobId, message);
  }

  private updateProgress(stage: IndexingStageId, stageProgress: number, message?: string) {
    // Track stage transitions
    if (stage !== this.currentStage) {
      if (this.currentStage && this.stageStartTime) {
        const elapsed = ((Date.now() - this.stageStartTime) / 1000).toFixed(1);
        console.log(`[indexer] Stage complete: ${this.currentStage} (${elapsed}s)`);
      }
      console.log(`[indexer] Starting stage: ${stage}`);
      this.currentStage = stage;
      this.stageStartTime = Date.now();
    }

    // Log meaningful progress updates
    if (message) {
      this.log(message, stage);
    }

    updateJob(this.jobId, {
      status: "running",
      stage,
      stage_progress: stageProgress,
      message,
    });
  }

  async run(): Promise<void> {
    try {
      // Reset AI usage tracking for this job
      resetUsageTracking();

      // Clear existing data
      clearAllData();

      // Stage 1: Compile dbt (if needed)
      await this.stageDbtCompile();

      // Stage 2: Parse manifest
      await this.stageParseManifest();

      // Stage 3: Parse Airflow
      await this.stageParseAirflow();

      // Stage 4: Parse SQL (done within Airflow parsing)
      this.updateProgress("parse_sql", 100, "SQL dependencies extracted");

      // Stage 5: Snowflake metadata (placeholder for now)
      await this.stageSnowflakeMetadata();

      // Stage 6: Cross-repo linking
      await this.stageCrossRepoLink();

      // Stage 7: AI grouping
      await this.stageAiGrouping();

      // Stage 8: AI flows
      await this.stageAiFlows();

      // Stage 9: Pre-compute explanations
      await this.stagePrecomputeExplanations();

      // Store AI usage stats
      const usageStats = getAiUsageStats();
      updateUsageStats(this.jobId, usageStats);

      // Log usage summary
      if (usageStats.totalCalls > 0) {
        this.log(
          `AI usage: ${usageStats.totalCalls} calls, ${(usageStats.totalInputTokens / 1000).toFixed(1)}K input tokens, ${(usageStats.totalOutputTokens / 1000).toFixed(1)}K output tokens, ~$${usageStats.estimatedCostUsd.toFixed(4)} estimated cost`
        );
      }

      // Mark complete
      updateJob(this.jobId, {
        status: "completed",
        stage: "complete",
        stage_progress: 100,
        message: `Indexed ${this.allNodes.length} nodes and ${this.allEdges.length} edges`,
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      updateJob(this.jobId, {
        status: "failed",
        error: errorMessage,
      });
      throw error;
    }
  }

  private async stageDbtCompile(): Promise<void> {
    this.updateProgress("dbt_compile", 0, `Checking dbt project at ${this.config.dbtPath}...`);

    const manifestPath = findManifestPath(this.config.dbtPath);
    
    if (manifestPath) {
      this.updateProgress("dbt_compile", 100, "Found existing manifest.json, skipping compile");
      return;
    }

    // Try to run dbt compile, but don't fail if it doesn't work
    // (fallback parser will be used in stageParseManifest)
    this.updateProgress("dbt_compile", 20, "No manifest found, running dbt compile...");
    
    try {
      await new Promise<void>((resolve, reject) => {
        const proc = spawn("dbt", ["compile"], {
          cwd: this.config.dbtPath,
          shell: true,
        });

        let output = "";
        proc.stdout?.on("data", (data) => {
          output += data.toString();
        });
        proc.stderr?.on("data", (data) => {
          output += data.toString();
        });

        proc.on("close", (code) => {
          if (code === 0) {
            resolve();
          } else {
            reject(new Error(`dbt compile failed: ${output}`));
          }
        });

        proc.on("error", reject);
      });

      this.updateProgress("dbt_compile", 100, "dbt compile successful");
    } catch (error) {
      // dbt compile failed, but we can continue with fallback parsing
      console.warn("dbt compile failed, will use fallback SQL file parsing:", error);
      this.updateProgress("dbt_compile", 100, "dbt compile unavailable, using fallback SQL parser");
    }
  }

  private async stageParseManifest(): Promise<void> {
    this.updateProgress("parse_manifest", 0, "Looking for manifest.json...");

    const manifestPath = findManifestPath(this.config.dbtPath);
    
    let result;
    if (manifestPath) {
      // Use manifest.json for parsing (preferred)
      this.updateProgress("parse_manifest", 30, "Parsing dbt manifest...");
      result = parseDbtManifest(manifestPath, this.config.dbtPath);
    } else {
      // Fallback: parse SQL files directly without manifest
      this.updateProgress("parse_manifest", 30, "No manifest found, using fallback SQL parser...");
      try {
        result = parseDbtProjectFallback(this.config.dbtPath);
        this.updateProgress(
          "parse_manifest",
          50,
          `Fallback parser: found ${result.nodes.length} models from SQL files`
        );
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        throw new Error(`Failed to parse dbt project (no manifest.json and fallback failed): ${errorMessage}`);
      }
    }
    
    this.allNodes.push(...result.nodes);
    this.allEdges.push(...result.edges);
    this.allCitations.push(...result.citations);

    this.updateProgress(
      "parse_manifest",
      100,
      `Parsed ${result.nodes.length} dbt models, ${result.edges.length} edges`
    );
  }

  private async stageParseAirflow(): Promise<void> {
    this.updateProgress("parse_airflow", 0, "Scanning Airflow DAGs...");

    const result = await parseAirflowDags(
      this.config.airflowPath,
      (progress, message) => {
        this.updateProgress("parse_airflow", progress, message);
      }
    );

    this.allNodes.push(...result.nodes);
    this.allEdges.push(...result.edges);
    this.allCitations.push(...result.citations);

    this.updateProgress(
      "parse_airflow",
      100,
      `Parsed ${result.nodes.length} Airflow tables, ${result.edges.length} DAG edges`
    );
  }

  private async stageSnowflakeMetadata(): Promise<void> {
    if (!this.config.snowflakeEnabled) {
      this.updateProgress("snowflake_metadata", 100, "Snowflake integration disabled");
      return;
    }

    const result = await enrichWithSnowflakeMetadata((progress, message) => {
      this.updateProgress("snowflake_metadata", progress, message);
    });

    if (result.errors.length > 0) {
      console.warn("Snowflake enrichment errors:", result.errors);
    }

    this.updateProgress(
      "snowflake_metadata",
      100,
      `Enriched ${result.enrichedCount} nodes with Snowflake metadata`
    );
  }

  private async stageCrossRepoLink(): Promise<void> {
    this.updateProgress("cross_repo_link", 0, `Linking ${this.allNodes.length} entities across repos...`);

    const { mergedNodes, additionalEdges, conflicts } = linkCrossRepo(
      this.allNodes,
      this.allEdges
    );

    this.allNodes = mergedNodes;
    this.allEdges.push(...additionalEdges);

    if (additionalEdges.length > 0) {
      this.updateProgress("cross_repo_link", 30, `Found ${additionalEdges.length} cross-repo connections`);
    }

    // Store nodes and edges in database
    this.updateProgress("cross_repo_link", 40, `Storing ${this.allNodes.length} nodes in database...`);
    insertNodes(this.allNodes.map(graphNodeToDb));
    
    this.updateProgress("cross_repo_link", 60, `Storing ${this.allEdges.length} edges in database...`);
    insertEdges(this.allEdges.map(graphEdgeToDb));
    
    this.updateProgress("cross_repo_link", 80, `Storing ${this.allCitations.length} source citations...`);
    insertCitations(this.allCitations.map(citationToDb));

    const conflictMsg = conflicts.length > 0 ? `, ${conflicts.length} naming conflicts resolved` : "";
    this.updateProgress(
      "cross_repo_link",
      100,
      `Stored ${this.allNodes.length} nodes, ${this.allEdges.length} edges${conflictMsg}`
    );
  }

  private async stageAiGrouping(): Promise<void> {
    this.updateProgress("ai_grouping", 0, "Inferring groups from code structure...");

    let groups;
    const hasApiKey = !!process.env.OPENAI_API_KEY;
    
    if (hasApiKey) {
      try {
        groups = await aiInferGroups(this.allNodes, (progress, message) => {
          // Map AI progress (0-90) to stage progress (0-70)
          const mappedProgress = Math.round(progress * 0.7);
          this.updateProgress("ai_grouping", mappedProgress, message);
        });
      } catch (error) {
        console.warn("AI grouping failed, using fallback:", error);
        this.log("AI grouping error, falling back to rule-based grouping");
        groups = inferGroups(this.allNodes);
      }
    } else {
      this.updateProgress("ai_grouping", 10, "No AI API key, using rule-based grouping...");
      groups = inferGroups(this.allNodes);
    }

    this.updateProgress("ai_grouping", 75, `Storing ${groups.length} groups in database...`);
    // Both AI and fallback grouping now return the same DB-ready format
    insertGroups(groups);

    this.updateProgress("ai_grouping", 85, "Updating node group assignments...");
    // Update nodes with group assignments
    const db = getDb();
    let assignedCount = 0;
    for (const node of this.allNodes) {
      if (node.groupId) {
        db.prepare("UPDATE nodes SET group_id = ? WHERE id = ?").run(node.groupId, node.id);
        assignedCount++;
      }
    }

    updateGroupNodeCounts();

    this.updateProgress("ai_grouping", 100, `Created ${groups.length} groups, assigned ${assignedCount} nodes`);
  }

  private async stageAiFlows(): Promise<void> {
    this.updateProgress("ai_flows", 0, "Proposing data flows...");

    let flows;
    const hasApiKey = !!process.env.OPENAI_API_KEY;
    
    if (hasApiKey) {
      try {
        flows = await aiProposeFlows(this.allNodes, this.allEdges, (progress, message) => {
          // Map AI progress (0-90) to stage progress (0-70)
          const mappedProgress = Math.round(progress * 0.7);
          this.updateProgress("ai_flows", mappedProgress, message);
        });
      } catch (error) {
        console.warn("AI flow proposal failed, using fallback:", error);
        this.log("AI flow proposal error, using predefined flows");
        flows = proposeFlows(this.allNodes, this.allEdges);
      }
    } else {
      this.updateProgress("ai_flows", 10, "No AI API key, using predefined flows...");
      flows = proposeFlows(this.allNodes, this.allEdges);
    }

    this.updateProgress("ai_flows", 75, `Storing ${flows.length} flows in database...`);
    for (const flow of flows) {
      insertFlow({
        id: flow.id,
        name: flow.name,
        description: flow.description || null,
        anchor_nodes: JSON.stringify(flow.anchorNodes),
        member_nodes: JSON.stringify(flow.memberNodes),
        user_defined: flow.userDefined ? 1 : 0,
        inference_reason: flow.inferenceReason || null,
      });
    }

    const flowNames = flows.map(f => f.name).slice(0, 3).join(", ");
    this.updateProgress("ai_flows", 100, `Created ${flows.length} flows: ${flowNames}${flows.length > 3 ? "..." : ""}`);
  }

  private async stagePrecomputeExplanations(): Promise<void> {
    this.updateProgress("precompute_explanations", 0, "Identifying key models for explanation...");

    // Get key nodes (marts, reports, P1 priority)
    const keyNodes = this.allNodes.filter(
      (n) =>
        n.name.startsWith("mart_") ||
        n.name.startsWith("rpt_") ||
        n.metadata?.tags?.includes("p1")
    ).slice(0, 20); // Limit to top 20 for MVP to save time/cost

    const hasApiKey = !!process.env.OPENAI_API_KEY;
    
    if (hasApiKey && keyNodes.length > 0) {
      const keyNodeNames = keyNodes.slice(0, 3).map(n => n.name).join(", ");
      this.updateProgress("precompute_explanations", 5, `Found ${keyNodes.length} key models: ${keyNodeNames}...`);
      
      try {
        const explanations = await batchExplainNodes(keyNodes, {
          repoPath: this.config.dbtPath,
          onProgress: (completed, total) => {
            const progress = 10 + Math.floor((completed / total) * 80);
            const currentNode = keyNodes[completed - 1]?.name || "";
            this.updateProgress("precompute_explanations", progress, `Explaining ${currentNode} (${completed}/${total})`);
          },
        });

        this.updateProgress("precompute_explanations", 95, "Storing explanations in database...");

        // Store explanations
        for (const [nodeId, summary] of explanations) {
          insertExplanation({
            node_id: nodeId,
            summary,
            generated_at: new Date().toISOString(),
            model_used: process.env.OPENAI_MODEL || "o1",
          });
        }

        this.updateProgress(
          "precompute_explanations",
          100,
          `Generated ${explanations.size} AI explanations for key models`
        );
      } catch (error) {
        console.warn("Pre-compute explanations failed:", error);
        this.updateProgress(
          "precompute_explanations",
          100,
          `Explanation pre-compute skipped (will generate on-demand)`
        );
      }
    } else {
      const reason = !hasApiKey ? "No AI API key" : "No key models found";
      this.updateProgress(
        "precompute_explanations",
        100,
        `${reason}, explanations will be generated on-demand`
      );
    }
  }
}

// Rule-based grouping (will be enhanced with AI in Day 4)
function inferGroups(nodes: GraphNode[]): Array<{
  id: string;
  name: string;
  description: string | null;
  parent_id: string | null;
  inference_reason: string | null;
  collapsed_default: number;
}> {
  const groups: Map<string, { name: string; description: string; nodes: string[] }> = new Map();

  // Layer groups
  const layers = [
    { prefix: "stg_", id: "layer_staging", name: "Staging", desc: "Raw source staging models" },
    { prefix: "int_", id: "layer_intermediate", name: "Intermediate", desc: "Business logic transformations" },
    { prefix: "mart_", id: "layer_marts", name: "Marts", desc: "Final business tables" },
    { prefix: "rpt_", id: "layer_reports", name: "Reports", desc: "Reporting views" },
  ];

  // Domain detection from node names/metadata
  const domains = new Set<string>();

  for (const node of nodes) {
    // Detect domain from name pattern like "mart_sales__*" or "int_growth__*"
    const match = node.name.match(/^(?:stg_|int_|mart_|rpt_)(\w+)__/);
    if (match) {
      domains.add(match[1]);
    }

    // Also check tags
    if (node.metadata?.tags) {
      for (const tag of node.metadata.tags) {
        if (["sales", "growth", "finance", "bookings", "marketing", "customer_experience"].includes(tag)) {
          domains.add(tag);
        }
      }
    }
  }

  const result: Array<{
    id: string;
    name: string;
    description: string | null;
    parent_id: string | null;
    inference_reason: string | null;
    collapsed_default: number;
  }> = [];

  // Create layer groups
  for (const layer of layers) {
    result.push({
      id: layer.id,
      name: layer.name,
      description: layer.desc,
      parent_id: null,
      inference_reason: `Inferred from model naming convention (prefix: ${layer.prefix})`,
      collapsed_default: 1,
    });
  }

  // Create domain groups
  for (const domain of domains) {
    result.push({
      id: `domain_${domain}`,
      name: domain.charAt(0).toUpperCase() + domain.slice(1),
      description: `${domain} domain models`,
      parent_id: null,
      inference_reason: `Inferred from model naming pattern (*_${domain}__*)`,
      collapsed_default: 1,
    });
  }

  // Create sources group
  result.push({
    id: "group_sources",
    name: "Data Sources",
    description: "External data sources and seeds",
    parent_id: null,
    inference_reason: "Contains source and seed nodes",
    collapsed_default: 1,
  });

  // Assign nodes to groups (update the nodes array)
  for (const node of nodes) {
    if (node.type === "source" || node.type === "seed") {
      node.groupId = "group_sources";
      continue;
    }

    // Layer assignment
    for (const layer of layers) {
      if (node.name.startsWith(layer.prefix)) {
        node.groupId = layer.id;
        break;
      }
    }
  }

  return result;
}

// Flow proposal (will be enhanced with AI in Day 4)
function proposeFlows(
  nodes: GraphNode[],
  edges: GraphEdge[]
): Array<{
  id: string;
  name: string;
  description?: string;
  anchorNodes: string[];
  memberNodes: string[];
  userDefined: boolean;
  inferenceReason?: string;
}> {
  const flows: Array<{
    id: string;
    name: string;
    description?: string;
    anchorNodes: string[];
    memberNodes: string[];
    userDefined: boolean;
    inferenceReason?: string;
  }> = [];

  // Build adjacency for upstream traversal
  const downstreamMap = new Map<string, Set<string>>();
  const upstreamMap = new Map<string, Set<string>>();
  
  for (const edge of edges) {
    if (!downstreamMap.has(edge.from)) downstreamMap.set(edge.from, new Set());
    if (!upstreamMap.has(edge.to)) upstreamMap.set(edge.to, new Set());
    downstreamMap.get(edge.from)!.add(edge.to);
    upstreamMap.get(edge.to)!.add(edge.from);
  }

  function getUpstream(nodeId: string, depth: number, visited = new Set<string>()): string[] {
    if (depth === 0 || visited.has(nodeId)) return [];
    visited.add(nodeId);
    
    const upstream = upstreamMap.get(nodeId) || new Set();
    const result = [...upstream];
    
    for (const up of upstream) {
      result.push(...getUpstream(up, depth - 1, visited));
    }
    
    return result;
  }

  // Mechanized Outreach flow
  const moAnchor = nodes.find((n) => n.name === "mart_growth__lsw_lead_data");
  if (moAnchor) {
    const members = new Set([moAnchor.id]);
    const upstream = getUpstream(moAnchor.id, 5);
    upstream.forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Mechanized Outreach",
      description: "Lead enrichment and outreach pipeline from Apollo, ZoomInfo, Cognism sources to LSW Lead Data mart",
      anchorNodes: [moAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected mart_growth__lsw_lead_data as anchor with MECH_OUTREACH sources upstream",
    });
  }

  // Bookings flow
  const bookingsAnchor = nodes.find((n) => n.name === "mart_bookings__line_items_final");
  if (bookingsAnchor) {
    const members = new Set([bookingsAnchor.id]);
    const upstream = getUpstream(bookingsAnchor.id, 5);
    upstream.forEach((id) => members.add(id));

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

  // Sales Opportunities flow
  const salesAnchor = nodes.find((n) => n.name === "int_sales__opportunities");
  if (salesAnchor) {
    const members = new Set([salesAnchor.id]);
    const upstream = getUpstream(salesAnchor.id, 4);
    upstream.forEach((id) => members.add(id));

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

  // Automated Intent flow
  const aiAnchor = nodes.find(
    (n) =>
      n.name === "AUTOMATED_INTENT_LEADS" ||
      n.name === "AUTOMATED_INTENT_POPULATION" ||
      n.name.toLowerCase().includes("automated_intent")
  );
  if (aiAnchor) {
    const members = new Set([aiAnchor.id]);
    const upstream = getUpstream(aiAnchor.id, 5);
    upstream.forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Automated Intent",
      description: "Intent-based lead qualification pipeline processing leads from G2, Gartner, and other intent sources",
      anchorNodes: [aiAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected AUTOMATED_INTENT tables as anchor for intent-based lead qualification",
    });
  }

  // Job Change flow
  const jobChangeAnchor = nodes.find(
    (n) =>
      n.name.toLowerCase().includes("mart_growth__job_change") ||
      n.name === "JOB_CHANGE_CLAY_RESULTS_RAW_OUTPUT" ||
      n.name.toLowerCase().includes("job_change")
  );
  if (jobChangeAnchor) {
    const members = new Set([jobChangeAnchor.id]);
    const upstream = getUpstream(jobChangeAnchor.id, 5);
    upstream.forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Job Change",
      description: "Employment change signals from Clay integration for prospecting triggers",
      anchorNodes: [jobChangeAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected job change tables as anchor for employment change signal processing",
    });
  }

  // Direct Mail flow
  const directMailAnchor = nodes.find(
    (n) =>
      n.name === "mart_growth__direct_mail_lead_enrichment" ||
      n.name.toLowerCase().includes("direct_mail")
  );
  if (directMailAnchor) {
    const members = new Set([directMailAnchor.id]);
    const upstream = getUpstream(directMailAnchor.id, 5);
    upstream.forEach((id) => members.add(id));

    flows.push({
      id: uuid(),
      name: "Direct Mail",
      description: "Physical mail campaign lead enrichment for Lahlouh integration",
      anchorNodes: [directMailAnchor.id],
      memberNodes: [...members],
      userDefined: false,
      inferenceReason: "Detected direct mail lead enrichment table as anchor for physical mail campaigns",
    });
  }

  return flows;
}

