/**
 * Census Reverse ETL Parser
 * 
 * Parses Census sync configurations to create edges between:
 * 1. Source models (dbt marts) → Census (external node)
 * 2. Census → Destination tables (when destination is Snowflake and matches a dbt source)
 * 
 * This captures "reverse ETL loop-back" patterns where data flows:
 * mart_model → Census sync → Snowflake table → dbt staging model
 */

import { v4 as uuid } from "uuid";
import type { GraphNode, GraphEdge, Citation } from "../types";

// ============================================================================
// Census API Types (based on Census API v1)
// ============================================================================

export interface CensusConnection {
  id: number;
  name: string;
  type: string;  // e.g., "snowflake", "salesforce", "hubspot"
}

export interface CensusSyncSource {
  connection_id: number;
  object: string;  // Model name or SQL query identifier
}

export interface CensusSyncDestination {
  connection_id: number;
  object: string;  // Table/object name (e.g., "GROWTH.MECHANIZED_OUTREACH_POPULATION")
}

export interface CensusSync {
  id: number;
  label?: string;
  source: CensusSyncSource;
  destination: CensusSyncDestination;
  enabled?: boolean;
  schedule?: string;
}

export interface CensusConfig {
  syncs: CensusSync[];
  connections?: CensusConnection[];
}

// ============================================================================
// Parser Result Types
// ============================================================================

export interface CensusParseResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
  citations: Citation[];
  stats: {
    syncsProcessed: number;
    edgesCreated: number;
    loopBacksDetected: number;
    unmatchedSources: string[];
    unmatchedDestinations: string[];
  };
}

// ============================================================================
// Node Matching Functions
// ============================================================================

/**
 * Find a graph node that matches a Census source object name.
 * Census sources typically reference dbt model names.
 */
export function findSourceNode(
  sourceName: string,
  nodes: GraphNode[]
): GraphNode | null {
  const normalizedSource = sourceName.toLowerCase().trim();
  
  // Try exact match on name
  const byName = nodes.find(n => 
    n.name.toLowerCase() === normalizedSource
  );
  if (byName) return byName;
  
  // Try FQN suffix match (for qualified names like "database.schema.model")
  const bySuffix = nodes.find(n => 
    n.id.toLowerCase().endsWith(`.${normalizedSource}`)
  );
  if (bySuffix) return bySuffix;
  
  // Try partial match (source might be "mart_growth__lsw_lead_data" without schema)
  const byPartial = nodes.find(n => {
    const nodeName = n.name.toLowerCase();
    return nodeName === normalizedSource || 
           nodeName.endsWith(`__${normalizedSource}`) ||
           normalizedSource.endsWith(`__${nodeName}`);
  });
  if (byPartial) return byPartial;
  
  return null;
}

/**
 * Find a dbt source node that matches a Census Snowflake destination.
 * This enables detection of "loop-back" patterns where Census writes
 * to a Snowflake table that is then read by a dbt staging model.
 */
export function findDestinationSourceNode(
  destObject: string,
  nodes: GraphNode[]
): GraphNode | null {
  // Parse "SCHEMA.TABLE" or "DATABASE.SCHEMA.TABLE" format
  const parts = destObject.toUpperCase().split(".");
  
  let schema: string;
  let table: string;
  
  if (parts.length === 3) {
    // DATABASE.SCHEMA.TABLE
    schema = parts[1];
    table = parts[2];
  } else if (parts.length === 2) {
    // SCHEMA.TABLE
    schema = parts[0];
    table = parts[1];
  } else {
    // Just TABLE
    schema = "";
    table = parts[0];
  }
  
  // Find dbt source with matching schema/table
  // Sources are typically named like the table and have schema in metadata
  for (const node of nodes) {
    if (node.type !== "source") continue;
    
    const nodeSchema = node.metadata?.schema?.toUpperCase() || "";
    const nodeName = node.name.toUpperCase();
    
    // Match by schema + name
    if (schema && nodeSchema === schema && nodeName === table) {
      return node;
    }
    
    // Match by name only if no schema specified
    if (!schema && nodeName === table) {
      return node;
    }
    
    // Also check FQN for raw tables
    if (node.id.toUpperCase().includes(`.${schema}.${table}`)) {
      return node;
    }
  }
  
  // Also check for staging models that read from this source
  // Pattern: stg_growth__mechanized_outreach_population reads from GROWTH.MECHANIZED_OUTREACH_POPULATION
  const expectedStagingName = `stg_${schema.toLowerCase()}__${table.toLowerCase()}`;
  const stagingModel = nodes.find(n => 
    n.name.toLowerCase() === expectedStagingName ||
    n.name.toLowerCase().endsWith(`__${table.toLowerCase()}`)
  );
  
  if (stagingModel) {
    // Return the source that the staging model reads from (if we can find it)
    // For now, return the staging model itself as the connection point
    return stagingModel;
  }
  
  return null;
}

/**
 * Check if a Census connection is a Snowflake connection.
 */
export function isSnowflakeConnection(
  connectionId: number,
  connections: CensusConnection[]
): boolean {
  const conn = connections.find(c => c.id === connectionId);
  return conn?.type?.toLowerCase() === "snowflake";
}

/**
 * Get connection type by ID.
 */
export function getConnectionType(
  connectionId: number,
  connections: CensusConnection[]
): string | null {
  const conn = connections.find(c => c.id === connectionId);
  return conn?.type || null;
}

// ============================================================================
// Main Parser Function
// ============================================================================

/**
 * Parse Census sync configuration and create graph edges.
 * 
 * For each sync:
 * 1. Create edge: source_model → external.reverse_etl.census
 * 2. If destination is Snowflake and matches a dbt source/staging model,
 *    create edge: external.reverse_etl.census → destination_node
 */
export function parseCensusConfig(
  config: CensusConfig,
  existingNodes: GraphNode[],
  onProgress?: (percent: number, message: string) => void
): CensusParseResult {
  const result: CensusParseResult = {
    nodes: [],
    edges: [],
    citations: [],
    stats: {
      syncsProcessed: 0,
      edgesCreated: 0,
      loopBacksDetected: 0,
      unmatchedSources: [],
      unmatchedDestinations: [],
    },
  };
  
  const { syncs, connections = [] } = config;
  
  if (!syncs || syncs.length === 0) {
    onProgress?.(100, "No Census syncs found in configuration");
    return result;
  }
  
  onProgress?.(0, `Processing ${syncs.length} Census syncs...`);
  
  // Create Census external node if it doesn't exist
  const censusNodeId = "external.reverse_etl.census";
  const existingCensusNode = existingNodes.find(n => n.id === censusNodeId);
  
  if (!existingCensusNode) {
    result.nodes.push({
      id: censusNodeId,
      name: "Census",
      type: "external",
      subtype: "reverse_etl",
      repo: "census",
      metadata: {
        description: "Census reverse ETL platform - syncs data from warehouse to business tools",
        tags: ["reverse_etl", "census"],
      },
      semanticLayer: "external",
    });
    
    result.citations.push({
      id: uuid(),
      nodeId: censusNodeId,
      filePath: "census-syncs.json",
    });
  }
  
  // Track edges we've created to avoid duplicates
  const createdEdges = new Set<string>();
  
  // Process each sync
  for (let i = 0; i < syncs.length; i++) {
    const sync = syncs[i];
    result.stats.syncsProcessed++;
    
    // Skip disabled syncs
    if (sync.enabled === false) {
      continue;
    }
    
    const syncLabel = sync.label || `Sync ${sync.id}`;
    
    // Find source node in the graph
    const sourceNode = findSourceNode(sync.source.object, existingNodes);
    
    if (!sourceNode) {
      result.stats.unmatchedSources.push(sync.source.object);
      continue;
    }
    
    // Create edge: source_model → Census
    const sourceEdgeKey = `${sourceNode.id}|${censusNodeId}`;
    if (!createdEdges.has(sourceEdgeKey)) {
      result.edges.push({
        id: uuid(),
        from: sourceNode.id,
        to: censusNodeId,
        type: "reverse_etl",
        metadata: {
          transformationType: "census-sync",
          censusSyncId: sync.id,
          censusSyncLabel: syncLabel,
          inferredFrom: "census-config",
        },
      });
      createdEdges.add(sourceEdgeKey);
      result.stats.edgesCreated++;
    }
    
    // Check if destination is Snowflake (loop-back pattern)
    const destConnectionType = getConnectionType(sync.destination.connection_id, connections);
    const isSnowflakeDest = destConnectionType?.toLowerCase() === "snowflake" ||
                           // If no connections provided, try to detect from object name
                           (connections.length === 0 && sync.destination.object.includes("."));
    
    if (isSnowflakeDest) {
      // Try to find the destination as a dbt source/staging model
      const destNode = findDestinationSourceNode(sync.destination.object, existingNodes);
      
      if (destNode) {
        // Create edge: Census → destination_source/staging_model
        const destEdgeKey = `${censusNodeId}|${destNode.id}`;
        if (!createdEdges.has(destEdgeKey)) {
          result.edges.push({
            id: uuid(),
            from: censusNodeId,
            to: destNode.id,
            type: "reverse_etl",
            metadata: {
              transformationType: "census-loopback",
              censusSyncId: sync.id,
              censusSyncLabel: syncLabel,
              inferredFrom: "census-config",
            },
          });
          createdEdges.add(destEdgeKey);
          result.stats.edgesCreated++;
          result.stats.loopBacksDetected++;
        }
      } else {
        result.stats.unmatchedDestinations.push(sync.destination.object);
      }
    }
    
    // Progress update
    const progress = Math.round(((i + 1) / syncs.length) * 100);
    if (progress % 20 === 0 || i === syncs.length - 1) {
      onProgress?.(progress, `Processed ${i + 1}/${syncs.length} syncs`);
    }
  }
  
  onProgress?.(100, `Created ${result.stats.edgesCreated} edges from ${result.stats.syncsProcessed} syncs (${result.stats.loopBacksDetected} loop-backs)`);
  
  return result;
}

/**
 * Validate Census config JSON structure.
 */
export function validateCensusConfig(data: unknown): { valid: boolean; error?: string } {
  if (!data || typeof data !== "object") {
    return { valid: false, error: "Invalid JSON: expected object" };
  }
  
  const config = data as Record<string, unknown>;
  
  // Check for syncs array
  if (!Array.isArray(config.syncs) && !Array.isArray(config.data)) {
    return { valid: false, error: "Missing 'syncs' or 'data' array in Census config" };
  }
  
  const syncs = (config.syncs || config.data) as unknown[];
  
  if (syncs.length === 0) {
    return { valid: false, error: "No syncs found in Census config" };
  }
  
  // Validate first sync has required fields
  const firstSync = syncs[0] as Record<string, unknown>;
  if (!firstSync.source || !firstSync.destination) {
    return { valid: false, error: "Sync missing required 'source' or 'destination' fields" };
  }
  
  return { valid: true };
}

/**
 * Normalize Census API response to our internal format.
 * Census API may return data in different formats depending on endpoint version.
 */
export function normalizeCensusResponse(data: unknown): CensusConfig {
  const obj = data as Record<string, unknown>;
  
  // Handle direct syncs array
  if (Array.isArray(obj.syncs)) {
    return {
      syncs: obj.syncs as CensusSync[],
      connections: (obj.connections || obj.destinations || []) as CensusConnection[],
    };
  }
  
  // Handle "data" wrapper (common in paginated APIs)
  if (Array.isArray(obj.data)) {
    return {
      syncs: obj.data as CensusSync[],
      connections: (obj.connections || obj.destinations || []) as CensusConnection[],
    };
  }
  
  // Assume it's already in the right format
  return obj as CensusConfig;
}
