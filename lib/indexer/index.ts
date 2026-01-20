import { v4 as uuid } from "uuid";
import { spawn } from "child_process";
import { join } from "path";
import {
  getDb,
  getJob,
  insertNodes,
  insertEdges,
  insertCitations,
  insertGroups,
  insertFlow,
  insertExplanation,
  updateJob,
  clearAllData,
  cleanupOrphanedExplanations,
  updateGroupNodeCounts,
  appendActivityLog,
  updateUsageStats,
  updateNodeLayouts,
  updateNodeSemanticLayers,
  updateNodeImportanceScores,
  insertLayerNames,
  insertAnchorCandidates,
  markStageSkipped,
  setJobWaitingForSchemas,
  getSelectedSchemas,
  DbNode,
  DbEdge,
  DbCitation,
} from "../db";
import { 
  precomputeLayout, 
  computeLayoutWithDiff, 
  type NodePosition,
  type IncrementalLayoutResult,
} from "../graph/layout";
import { parseDbtManifest, findManifestPath, parseDbtProjectFallback, buildSqlContentMap } from "./dbtParser";
import { parseAirflowDags, type ExternalSystemDetection } from "./airflowParser";
import { parseExternalSystems, KNOWN_EXTERNAL_SYSTEMS } from "./externalParser";
import { inferExternalSystems, summarizeDetections } from "./externalInference";
import { parseCensusConfig, normalizeCensusResponse, validateCensusConfig, type CensusConfig } from "./censusParser";
import { linkCrossRepo } from "./linker";
import { enrichWithSnowflakeMetadata, discoverSnowflakeTables, getSnowflakeConfig, hasSnowflakeCredentials, type SnowflakeDiscoveryResult } from "./snowflakeMetadata";
import { connect, getSchemas, disconnect } from "../snowflake/client";
import { inferGroups as aiInferGroups } from "../ai/grouping";
import { proposeFlows as aiProposeFlows } from "../ai/flows";
import { generateLayerNames } from "../ai/layerNaming";
import { batchExplainNodes } from "../ai/explain";
import { resetUsageTracking, getUsageStats as getAiUsageStats } from "../ai/client";
import { 
  classifyAllNodes, 
  computeImportanceScores, 
  getTopAnchorCandidates 
} from "../graph/semantic";
import type { GraphNode, GraphEdge, Citation, IndexingStageId } from "../types";

export interface IndexerConfig {
  dbtPath: string;
  airflowPath: string;
  snowflakeEnabled: boolean;
  /** Optional Census sync configuration (JSON object or path to JSON file) */
  censusConfig?: CensusConfig | string;
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
    sql_content: node.sqlContent || null,
    layout_x: node.layoutX ?? null,
    layout_y: node.layoutY ?? null,
    layout_layer: node.layoutLayer ?? null,
    semantic_layer: node.semanticLayer ?? null,
    importance_score: node.importanceScore ?? null,
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
  
  // Previous layout data for incremental layout support
  private previousPositions: Map<string, NodePosition> = new Map();
  private previousEdgeCount: number = 0;
  
  // External systems detected from Airflow DAGs
  private airflowExternalSystems: ExternalSystemDetection[] = [];
  
  // Snowflake discovery results for final summary
  private snowflakeDiscoveryResult: SnowflakeDiscoveryResult | null = null;

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
        const completeMsg = `Stage complete: ${this.currentStage} (${elapsed}s)`;
        console.log(`[indexer] ${completeMsg}`);
        appendActivityLog(this.jobId, completeMsg);
      }
      const startMsg = `Starting stage: ${stage}`;
      console.log(`[indexer] ${startMsg}`);
      appendActivityLog(this.jobId, startMsg);
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

      // Capture previous layout data for incremental layout support
      await this.capturePreviousLayout();

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

      // Stage 5: Parse external systems (exposures, config, detected from DAGs)
      await this.stageParseExternals();

      // Stage 5.5: Infer external destinations from SQL patterns
      await this.stageInferExternalDestinations();

      // Stage 5.6: Parse Census reverse ETL configuration
      await this.stageParseCensus();

      // Stage 6: Snowflake metadata enrichment
      await this.stageSnowflakeMetadata();

      // Stage 7: Snowflake table discovery
      await this.stageSnowflakeDiscovery();

      // Stage 8: Cross-repo linking
      await this.stageCrossRepoLink();

      // Stage 6.5: Pre-compute layout (after nodes/edges are stored)
      await this.stagePrecomputeLayout();

      // Stage 6.6: Semantic layer classification
      await this.stageSemanticClassification();

      // Stage 6.7: Importance scoring and anchor candidates
      await this.stageImportanceScoring();

      // Stage 7: AI grouping (repurposed for layer naming)
      await this.stageAiLayerNaming();

      // Stage 8: AI flows
      await this.stageAiFlows();

      // Stage 9: Pre-compute explanations
      await this.stagePrecomputeExplanations();

      // Clean up orphaned explanations (from nodes that no longer exist)
      const orphanedCount = cleanupOrphanedExplanations();
      if (orphanedCount > 0) {
        this.log(`Cleaned up ${orphanedCount} orphaned explanations`);
      }

      // Store AI usage stats
      const usageStats = getAiUsageStats();
      updateUsageStats(this.jobId, usageStats);

      // Log usage summary
      if (usageStats.totalCalls > 0) {
        this.log(
          `AI usage: ${usageStats.totalCalls} calls, ${(usageStats.totalInputTokens / 1000).toFixed(1)}K input tokens, ${(usageStats.totalOutputTokens / 1000).toFixed(1)}K output tokens, ~$${usageStats.estimatedCostUsd.toFixed(4)} estimated cost`
        );
      }

      // Mark complete with summary including Snowflake discovery stats
      const sfStats = this.snowflakeDiscoveryResult?.stats;
      const sfSummary = sfStats && sfStats.newTablesAdded > 0 
        ? ` (${sfStats.newTablesAdded} from Snowflake discovery)` 
        : "";
      
      updateJob(this.jobId, {
        status: "completed",
        stage: "complete",
        stage_progress: 100,
        message: `Indexed ${this.allNodes.length} nodes${sfSummary} and ${this.allEdges.length} edges`,
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
    
    // Store detected external systems for later processing
    this.airflowExternalSystems = result.externalSystems;

    const externalMsg = result.externalSystems.length > 0 
      ? `, ${result.externalSystems.length} external systems detected` 
      : "";
    this.updateProgress(
      "parse_airflow",
      100,
      `Parsed ${result.nodes.length} Airflow tables, ${result.edges.length} DAG edges${externalMsg}`
    );
  }

  private async stageSnowflakeMetadata(): Promise<void> {
    if (!this.config.snowflakeEnabled) {
      markStageSkipped(this.jobId, "snowflake_metadata");
      this.updateProgress("snowflake_metadata", 100, "Snowflake integration disabled");
      return;
    }

    const result = await enrichWithSnowflakeMetadata((progress, message) => {
      this.updateProgress("snowflake_metadata", progress, message);
    });

    if (result.errors.length > 0) {
      console.warn("Snowflake enrichment errors:", result.errors);
      // Mark as skipped if there were connection errors
      if (result.enrichedCount === 0) {
        markStageSkipped(this.jobId, "snowflake_metadata");
      }
    }

    this.updateProgress(
      "snowflake_metadata",
      100,
      `Enriched ${result.enrichedCount} nodes with Snowflake metadata`
    );
  }

  /**
   * Discover all tables in Snowflake that aren't already in the graph.
   * Creates nodes for raw tables and links them to models that reference them.
   */
  /**
   * Wait for user to select schemas from the UI.
   * Polls the job record until selected_schemas is populated.
   */
  private async waitForSchemaSelection(): Promise<string[] | null> {
    const POLL_INTERVAL = 500; // ms
    const MAX_WAIT = 5 * 60 * 1000; // 5 minutes
    const startTime = Date.now();

    while (Date.now() - startTime < MAX_WAIT) {
      const job = getJob(this.jobId);
      
      // Check if job was cancelled or failed
      if (!job || job.status === "failed") {
        return null;
      }
      
      // Check if user submitted selection
      if (job.status === "running" && job.selected_schemas) {
        return JSON.parse(job.selected_schemas);
      }
      
      // Wait before polling again
      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
    }

    // Timeout - user didn't select in time
    return null;
  }

  private async stageSnowflakeDiscovery(): Promise<void> {
    if (!this.config.snowflakeEnabled) {
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.updateProgress("snowflake_discovery", 100, "Snowflake discovery disabled");
      return;
    }

    // Check for credentials
    if (!hasSnowflakeCredentials()) {
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.log("⚠️ Snowflake credentials not configured");
      this.updateProgress("snowflake_discovery", 100, "⚠️ Skipped: Credentials not configured");
      return;
    }

    this.updateProgress("snowflake_discovery", 5, "Connecting to Snowflake to fetch schemas...");

    const config = getSnowflakeConfig();

    // Step 1: Connect and get available schemas
    let availableSchemas: string[];
    try {
      await connect(config);
      const allSchemas = await getSchemas(config.database);
      
      // Filter out system and dev/test schemas
      availableSchemas = allSchemas.filter((schema) => {
        const upper = schema.toUpperCase();
        if (upper === "INFORMATION_SCHEMA" || upper === "PUBLIC") return false;
        if (upper.includes("_DEV") || upper.includes("_TEST")) return false;
        return true;
      }).sort();

      await disconnect();
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.log(`⚠️ Snowflake connection failed: ${msg}`);
      this.updateProgress("snowflake_discovery", 100, `⚠️ Skipped: ${msg}`);
      return;
    }

    if (availableSchemas.length === 0) {
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.log("⚠️ No schemas found in Snowflake");
      this.updateProgress("snowflake_discovery", 100, "⚠️ Skipped: No schemas found");
      return;
    }

    this.log(`Found ${availableSchemas.length} schemas in Snowflake`);
    this.updateProgress("snowflake_discovery", 10, `Found ${availableSchemas.length} schemas. Waiting for selection...`);

    // Step 2: Set job to waiting state and wait for user selection
    setJobWaitingForSchemas(this.jobId, availableSchemas);

    const selectedSchemas = await this.waitForSchemaSelection();

    if (!selectedSchemas || selectedSchemas.length === 0) {
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.log("⚠️ Schema selection cancelled or timed out");
      this.updateProgress("snowflake_discovery", 100, "⚠️ Skipped: No schemas selected");
      return;
    }

    this.log(`User selected ${selectedSchemas.length} schemas: ${selectedSchemas.slice(0, 5).join(", ")}${selectedSchemas.length > 5 ? "..." : ""}`);
    this.updateProgress("snowflake_discovery", 15, `Discovering tables from ${selectedSchemas.length} schemas...`);

    // Step 3: Run discovery with selected schemas
    const existingNodeIds = new Set(this.allNodes.map(n => n.id));

    const result = await discoverSnowflakeTables(
      existingNodeIds,
      this.allNodes,
      (progress, message) => {
        // Map 0-100 to 15-100
        const mappedProgress = 15 + Math.floor(progress * 0.85);
        this.updateProgress("snowflake_discovery", mappedProgress, message);
      },
      selectedSchemas // Pass selected schemas to filter
    );

    // Store result for final summary
    this.snowflakeDiscoveryResult = result;

    // Handle skipped case
    if (result.skipped) {
      markStageSkipped(this.jobId, "snowflake_discovery");
      this.log(`⚠️ Snowflake discovery skipped: ${result.skipReason}`);
      this.updateProgress("snowflake_discovery", 100, `⚠️ Skipped: ${result.skipReason}`);
      return;
    }

    // Log any errors
    if (result.errors.length > 0) {
      for (const error of result.errors) {
        console.warn("Snowflake discovery error:", error);
      }
    }

    // Add discovered nodes and edges to our collections
    if (result.nodes.length > 0) {
      this.allNodes.push(...result.nodes);
      this.log(`Snowflake discovery: Found ${result.stats.totalTablesInSnowflake} tables across ${result.stats.schemasScanned.length} schemas`);
      this.log(`Snowflake discovery: Added ${result.stats.newTablesAdded} new tables (${result.stats.tablesAlreadyInGraph} already in graph)`);
    }

    if (result.edges.length > 0) {
      this.allEdges.push(...result.edges);
      this.log(`Snowflake discovery: Created ${result.stats.edgesCreated} edges from SQL references`);
    }

    this.updateProgress(
      "snowflake_discovery",
      100,
      `Discovered ${result.stats.newTablesAdded} tables, ${result.stats.edgesCreated} edges from Snowflake`
    );
  }

  /**
   * Parse external systems from multiple sources:
   * 1. dbt exposures (already parsed in manifest stage)
   * 2. Airflow DAG detection (already collected in Airflow stage)
   * 3. externals.yml configuration file
   */
  private async stageParseExternals(): Promise<void> {
    this.updateProgress("parse_externals", 0, "Discovering external data consumers...");

    let externalNodesCount = 0;
    let externalEdgesCount = 0;

    // Count external nodes already added from dbt exposures
    const existingExternalNodes = this.allNodes.filter(n => n.type === "external").length;
    if (existingExternalNodes > 0) {
      this.updateProgress("parse_externals", 20, `Found ${existingExternalNodes} external systems from dbt exposures`);
      externalNodesCount += existingExternalNodes;
    }

    // Process Airflow-detected external systems
    if (this.airflowExternalSystems.length > 0) {
      this.updateProgress("parse_externals", 40, `Processing ${this.airflowExternalSystems.length} external systems from Airflow...`);
      
      // Build a lookup map for existing nodes by name
      const nodesByName = new Map<string, GraphNode>();
      for (const node of this.allNodes) {
        nodesByName.set(node.name.toLowerCase(), node);
      }

      for (const ext of this.airflowExternalSystems) {
        // Create external node ID
        const externalId = `external.${ext.type}.${ext.name.toLowerCase().replace(/\s+/g, "_")}`;
        
        // Check if this external system already exists (from dbt exposures or config)
        if (this.allNodes.some(n => n.id === externalId)) {
          continue;
        }

        // Create the external node
        const node: GraphNode = {
          id: externalId,
          name: ext.name,
          type: "external",
          subtype: ext.type === "reverse_etl" ? "reverse_etl" : 
                   ext.type === "dashboard" ? "dashboard" : "application",
          repo: "airflow-dags",
          metadata: {
            filePath: ext.detectedFrom,
            description: `Auto-detected from Airflow DAG patterns`,
          },
        };
        this.allNodes.push(node);
        externalNodesCount++;

        // Add citation
        this.allCitations.push({
          id: uuid(),
          nodeId: externalId,
          filePath: ext.detectedFrom,
        });

        // Create edges from consumed tables to this external system
        for (const tableName of ext.consumesFrom) {
          const sourceNode = nodesByName.get(tableName.toLowerCase());
          if (sourceNode) {
            this.allEdges.push({
              id: uuid(),
              from: sourceNode.id,
              to: externalId,
              type: "exposure",
              metadata: {
                transformationType: "airflow-detected",
              },
            });
            externalEdgesCount++;
          }
        }
      }
    }

    // Parse externals.yml configuration
    this.updateProgress("parse_externals", 60, "Checking for externals.yml configuration...");
    
    const configResult = parseExternalSystems(
      this.config.dbtPath,
      this.allNodes,
      (progress, message) => {
        const mappedProgress = 60 + Math.round(progress * 0.3);
        this.updateProgress("parse_externals", mappedProgress, message);
      }
    );

    if (configResult.nodes.length > 0) {
      // Deduplicate - only add nodes that don't already exist
      for (const node of configResult.nodes) {
        if (!this.allNodes.some(n => n.id === node.id)) {
          this.allNodes.push(node);
          externalNodesCount++;
        }
      }
      
      this.allEdges.push(...configResult.edges);
      this.allCitations.push(...configResult.citations);
      externalEdgesCount += configResult.edges.length;
    }

    // Count edges to external systems
    const externalEdges = this.allEdges.filter(e => e.type === "exposure").length;

    this.updateProgress(
      "parse_externals",
      100,
      `Discovered ${externalNodesCount} external consumers with ${externalEdges} connections`
    );
  }

  /**
   * Infer external system destinations from SQL patterns, model names,
   * column names, and macro usage. This creates edges from mart models
   * to external systems like Outreach, Brevo, Salesforce, etc.
   */
  private async stageInferExternalDestinations(): Promise<void> {
    this.updateProgress("parse_externals", 60, "Inferring external destinations from SQL patterns...");

    // Build SQL content map for all dbt models
    const sqlContentMap = buildSqlContentMap(this.allNodes, this.config.dbtPath);
    
    this.updateProgress("parse_externals", 65, `Loaded SQL content for ${sqlContentMap.size} models`);

    // Create SQL lookup function
    const getSqlContent = (node: GraphNode): string | null => {
      return sqlContentMap.get(node.id) || null;
    };

    // Run inference
    const inferenceResult = await inferExternalSystems(
      this.allNodes,
      getSqlContent,
      (progress, message) => {
        // Map 0-100 to 65-95
        const mappedProgress = 65 + Math.round(progress * 0.30);
        this.updateProgress("parse_externals", mappedProgress, message);
      }
    );

    // Add new external nodes (deduplicating against existing)
    const existingNodeIds = new Set(this.allNodes.map(n => n.id));
    let newNodesCount = 0;
    for (const node of inferenceResult.nodes) {
      if (!existingNodeIds.has(node.id)) {
        this.allNodes.push(node);
        existingNodeIds.add(node.id);
        newNodesCount++;
      }
    }

    // Add new edges (deduplicating by from+to combination)
    const existingEdgeKeys = new Set(this.allEdges.map(e => `${e.from}|${e.to}`));
    let newEdgesCount = 0;
    for (const edge of inferenceResult.edges) {
      const key = `${edge.from}|${edge.to}`;
      if (!existingEdgeKeys.has(key)) {
        this.allEdges.push(edge);
        existingEdgeKeys.add(key);
        newEdgesCount++;
      }
    }

    // Add citations
    this.allCitations.push(...inferenceResult.citations);

    // Log summary
    const summary = summarizeDetections(inferenceResult);
    if (newNodesCount > 0 || newEdgesCount > 0) {
      this.log(`Inferred ${newNodesCount} external systems, ${newEdgesCount} destination edges`);
      if (summary) {
        this.log(`External systems: ${summary}`);
      }
    }

    this.updateProgress(
      "parse_externals",
      98,
      `Inferred ${newNodesCount} external destinations with ${newEdgesCount} edges`
    );
  }

  /**
   * Parse Census sync configuration to create reverse ETL edges.
   * This captures "loop-back" patterns where mart models feed external pipelines
   * that write back to Snowflake tables consumed by staging models.
   * 
   * If no censusConfig is provided, attempts to load from data/census.json.
   * Fails gracefully with a warning if the file is missing or invalid.
   */
  private async stageParseCensus(): Promise<void> {
    this.updateProgress("parse_externals", 85, "Processing Census sync configuration...");

    try {
      let config: CensusConfig;
      const fs = await import("fs");

      if (this.config.censusConfig) {
        // Handle explicitly provided config (string path or object)
        if (typeof this.config.censusConfig === "string") {
          const content = fs.readFileSync(this.config.censusConfig, "utf-8");
          const parsed = JSON.parse(content);
          
          const validation = validateCensusConfig(parsed);
          if (!validation.valid) {
            this.log(`⚠️ Invalid Census config: ${validation.error}`);
            return;
          }
          
          config = normalizeCensusResponse(parsed);
        } else {
          config = this.config.censusConfig;
        }
      } else {
        // Try to auto-load from default location: data/census.json
        const defaultPath = join(process.cwd(), "data", "census.json");
        
        if (!fs.existsSync(defaultPath)) {
          this.log(`ℹ️ Census data file not found at ${defaultPath} - skipping Census integration`);
          return;
        }
        
        const content = fs.readFileSync(defaultPath, "utf-8");
        const parsed = JSON.parse(content);
        
        // Check if the file is just a placeholder (empty syncs array)
        if (!parsed.syncs || parsed.syncs.length === 0) {
          this.log(`ℹ️ Census data file is empty - run scripts/export_census_data.py to populate`);
          return;
        }
        
        const validation = validateCensusConfig(parsed);
        if (!validation.valid) {
          this.log(`⚠️ Invalid Census data file: ${validation.error}`);
          return;
        }
        
        config = normalizeCensusResponse(parsed);
        this.log(`📊 Loaded Census data from ${defaultPath}`);
      }

      const result = parseCensusConfig(
        config,
        this.allNodes,
        (progress, message) => {
          // Map 0-100 to 85-98
          const mappedProgress = 85 + Math.round(progress * 0.13);
          this.updateProgress("parse_externals", mappedProgress, message);
        }
      );

      // Add new Census node if created
      for (const node of result.nodes) {
        if (!this.allNodes.some(n => n.id === node.id)) {
          this.allNodes.push(node);
        }
      }

      // Add Census edges
      const existingEdgeKeys = new Set(this.allEdges.map(e => `${e.from}|${e.to}`));
      for (const edge of result.edges) {
        const key = `${edge.from}|${edge.to}`;
        if (!existingEdgeKeys.has(key)) {
          this.allEdges.push(edge);
          existingEdgeKeys.add(key);
        }
      }

      // Add citations
      this.allCitations.push(...result.citations);

      // Log detailed results
      const matchedCount = result.stats.matchedSources.length;
      this.log(`📊 Census Summary: ${result.stats.syncsProcessed} syncs processed`);
      this.log(`   🔄 Sync nodes created: ${result.stats.syncNodesCreated}`);
      this.log(`   ✅ Source matches: ${matchedCount} → ${result.stats.edgesCreated} source edges`);
      this.log(`   ❌ Unmatched: ${result.stats.unmatchedSources.length} sources (model names not found in graph)`);
      
      if (result.stats.loopBacksDetected > 0) {
        this.log(`   ↩️ Loop-backs: ${result.stats.loopBacksDetected} reverse ETL cycles to Snowflake`);
      }
      
      // Log destination stats
      if (result.stats.destinationNodesCreated > 0) {
        this.log(`   📤 Destination nodes: ${result.stats.destinationNodesCreated} (${result.stats.destinationEdgesCreated} edges)`);
        this.log(`   📍 Systems: ${result.stats.destinationTypes.join(", ")}`);
        
        // Show example sync nodes created
        const syncNodes = result.nodes.filter(n => n.subtype === "census_sync").slice(0, 3);
        if (syncNodes.length > 0) {
          const examples = syncNodes.map(n => n.name).join(", ");
          this.log(`   📝 Example syncs: ${examples}${result.stats.syncNodesCreated > 3 ? "..." : ""}`);
        }
      }

      // Always write detailed Census matching report
      const logPath = join(process.cwd(), "data", "logs", "census-matching-report.json");
      
      // Ensure logs directory exists
      const logsDir = join(process.cwd(), "data", "logs");
      if (!fs.existsSync(logsDir)) {
        fs.mkdirSync(logsDir, { recursive: true });
      }
      
      fs.writeFileSync(logPath, JSON.stringify({
        timestamp: new Date().toISOString(),
        summary: {
          totalSyncs: result.stats.syncsProcessed,
          syncNodesCreated: result.stats.syncNodesCreated,
          matchedSources: matchedCount,
          unmatchedSources: result.stats.unmatchedSources.length,
          sourceEdges: result.stats.edgesCreated,
          loopBacks: result.stats.loopBacksDetected,
          destinationNodes: result.stats.destinationNodesCreated,
          destinationEdges: result.stats.destinationEdgesCreated,
          destinationTypes: result.stats.destinationTypes,
        },
        matchedSources: result.stats.matchedSources,
        unmatchedSources: result.stats.unmatchedSources,
        unmatchedDestinations: result.stats.unmatchedDestinations,
      }, null, 2));
      
      this.log(`   📝 Full matching report: data/logs/census-matching-report.json`);
      
      // Show first few matched for quick reference
      if (matchedCount > 0) {
        const examples = result.stats.matchedSources.slice(0, 3)
          .map(m => `"${m.censusName}" → ${m.matchedNodeName}`)
          .join(", ");
        this.log(`   Example matches: ${examples}${matchedCount > 3 ? "..." : ""}`);
      }
      
      // Show first few unmatched for quick reference
      if (result.stats.unmatchedSources.length > 0) {
        this.log(`   First few unmatched: ${result.stats.unmatchedSources.slice(0, 5).join(", ")}${result.stats.unmatchedSources.length > 5 ? "..." : ""}`);
      }

    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.log(`⚠️ Census parsing failed: ${msg}`);
    }
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

  /**
   * Capture previous layout positions before clearing data.
   * This enables incremental layout on subsequent indexing runs.
   */
  private async capturePreviousLayout(): Promise<void> {
    try {
      const db = getDb();
      
      // Get existing layout positions
      const existingNodes = db.prepare(`
        SELECT id, layout_x, layout_y, layout_layer 
        FROM nodes 
        WHERE layout_x IS NOT NULL
      `).all() as Array<{ id: string; layout_x: number; layout_y: number; layout_layer: number }>;
      
      for (const node of existingNodes) {
        this.previousPositions.set(node.id, {
          x: node.layout_x,
          y: node.layout_y,
          layer: node.layout_layer,
        });
      }
      
      // Get previous edge count
      const edgeCount = db.prepare("SELECT COUNT(*) as count FROM edges").get() as { count: number };
      this.previousEdgeCount = edgeCount.count;
      
      if (this.previousPositions.size > 0) {
        this.log(`Captured ${this.previousPositions.size} previous layout positions for incremental update`);
      }
    } catch (error) {
      // Database might not exist yet on first run
      this.previousPositions.clear();
      this.previousEdgeCount = 0;
    }
  }

  private async stagePrecomputeLayout(): Promise<void> {
    this.updateProgress("cross_repo_link", 90, `Pre-computing layout for ${this.allNodes.length} nodes...`);

    const startTime = Date.now();

    // Convert to layout format
    const layoutNodes = this.allNodes.map((n) => ({ id: n.id, name: n.name }));
    const layoutEdges = this.allEdges.map((e) => ({ from: e.from, to: e.to }));

    // Yield to event loop before CPU-intensive layout computation
    // This allows pending HTTP requests (like status polls) to be processed
    await new Promise(resolve => setImmediate(resolve));

    // Use incremental layout if we have previous positions
    let result: IncrementalLayoutResult;
    
    if (this.previousPositions.size > 0) {
      result = computeLayoutWithDiff(
        layoutNodes,
        layoutEdges,
        this.previousPositions,
        this.previousEdgeCount
      );
      
      if (result.usedIncremental) {
        this.log(`Incremental layout: +${result.addedCount} nodes, -${result.removedCount} nodes`);
      }
    } else {
      // Full layout for first run
      const fullResult = precomputeLayout(layoutNodes, layoutEdges);
      result = {
        ...fullResult,
        usedIncremental: false,
        addedCount: layoutNodes.length,
        removedCount: 0,
      };
    }
    
    // Yield again after layout to allow pending requests to complete
    await new Promise(resolve => setImmediate(resolve));

    // Store layout positions in database
    const layoutPositions = Array.from(result.positions.entries()).map(([nodeId, pos]) => ({
      nodeId,
      x: pos.x,
      y: pos.y,
      layer: pos.layer,
    }));

    updateNodeLayouts(layoutPositions);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const layerCount = new Set(layoutPositions.map((p) => p.layer)).size;
    const layoutType = result.usedIncremental ? "Incremental" : "Full";
    
    this.updateProgress(
      "cross_repo_link",
      95,
      `${layoutType} layout computed in ${elapsed}s: ${layerCount} layers, bounds ${Math.round(result.bounds.maxX - result.bounds.minX)}x${Math.round(result.bounds.maxY - result.bounds.minY)}`
    );

    // Update layout positions on in-memory nodes too
    for (const node of this.allNodes) {
      const pos = result.positions.get(node.id);
      if (pos) {
        node.layoutX = pos.x;
        node.layoutY = pos.y;
        node.layoutLayer = pos.layer;
      }
    }
  }

  private async stageSemanticClassification(): Promise<void> {
    this.updateProgress("ai_grouping", 0, `Classifying ${this.allNodes.length} nodes into semantic layers...`);

    // Classify all nodes
    const classifications = classifyAllNodes(this.allNodes);
    
    // Update in-memory nodes
    const classMap = new Map(classifications.map(c => [c.nodeId, c.semanticLayer]));
    for (const node of this.allNodes) {
      node.semanticLayer = classMap.get(node.id);
    }

    // Store in database
    updateNodeSemanticLayers(classifications);

    // Count by layer
    const layerCounts = new Map<string, number>();
    for (const c of classifications) {
      layerCounts.set(c.semanticLayer, (layerCounts.get(c.semanticLayer) || 0) + 1);
    }

    const summary = [...layerCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 4)
      .map(([layer, count]) => `${layer}: ${count}`)
      .join(", ");

    this.updateProgress("ai_grouping", 15, `Semantic classification: ${summary}`);
  }

  private async stageImportanceScoring(): Promise<void> {
    this.updateProgress("ai_grouping", 20, `Computing importance scores for anchor suggestions...`);

    // Compute importance scores
    const scores = computeImportanceScores(this.allNodes, this.allEdges);

    // Update in-memory nodes
    const scoreMap = new Map(scores.map(s => [s.nodeId, s.importanceScore]));
    for (const node of this.allNodes) {
      node.importanceScore = scoreMap.get(node.id);
    }

    // Store importance scores in nodes table
    updateNodeImportanceScores(scores.map(s => ({
      nodeId: s.nodeId,
      importanceScore: s.importanceScore,
    })));

    // Get and store top anchor candidates
    const topCandidates = getTopAnchorCandidates(this.allNodes, this.allEdges, 50);
    insertAnchorCandidates(topCandidates.map(c => ({
      node_id: c.nodeId,
      importance_score: c.importanceScore,
      upstream_count: c.upstreamCount,
      downstream_count: c.downstreamCount,
      total_connections: c.totalConnections,
      reason: c.reason,
    })));

    const topNames = topCandidates.slice(0, 3).map(c => {
      const node = this.allNodes.find(n => n.id === c.nodeId);
      return node?.name || c.nodeId;
    }).join(", ");

    this.updateProgress("ai_grouping", 35, `Identified ${topCandidates.length} anchor candidates: ${topNames}...`);
  }

  private async stageAiLayerNaming(): Promise<void> {
    this.updateProgress("ai_grouping", 40, "Generating meaningful layer names...");

    const hasApiKey = !!process.env.OPENAI_API_KEY;

    if (!hasApiKey) {
      this.updateProgress("ai_grouping", 100, "No AI API key, layer names will use semantic defaults");
      return;
    }

    try {
      const layerNames = await generateLayerNames(this.allNodes, (progress, message) => {
        // Map progress (0-100) to stage progress (40-70)
        const mappedProgress = 40 + Math.round(progress * 0.3);
        this.updateProgress("ai_grouping", mappedProgress, message);
      });

      this.updateProgress("ai_grouping", 75, `Storing ${layerNames.length} layer names...`);
      insertLayerNames(layerNames);

      const layerSummary = layerNames.slice(0, 3).map(l => l.name).join(", ");
      this.updateProgress("ai_grouping", 80, `Generated layer names: ${layerSummary}...`);
    } catch (error) {
      console.warn("AI layer naming failed:", error);
      this.updateProgress("ai_grouping", 80, "Layer naming failed, using defaults");
    }
  }

  // Keep existing AI grouping for backward compatibility, but it runs after layer naming
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

