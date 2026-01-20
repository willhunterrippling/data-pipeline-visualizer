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
// Census API Types (supports both v1 and v2 API formats)
// ============================================================================

export interface CensusConnection {
  id: number;
  name: string;
  type: string;  // e.g., "snowflake", "salesforce", "hubspot"
}

// Normalized internal format
export interface CensusSyncSource {
  connection_id: number;
  object: string;  // Model name or SQL query identifier
}

export interface CensusSyncDestination {
  connection_id: number;
  object: string;  // Table/object name (e.g., "GROWTH.MECHANIZED_OUTREACH_POPULATION")
}

// Census API v2 format (source_attributes/destination_attributes)
export interface CensusV2SourceObject {
  type?: string;
  id?: number;
  name?: string;
  query?: string;
  source_object_id?: number;
  resource_identifier?: string;
}

export interface CensusV2SourceAttributes {
  connection_id: number;
  object: CensusV2SourceObject | string;
}

export interface CensusV2DestinationAttributes {
  connection_id: number;
  object: string;
}

// Raw sync from Census API (supports both formats)
export interface CensusRawSync {
  id: number;
  label?: string;
  // v1 format
  source?: CensusSyncSource;
  destination?: CensusSyncDestination;
  // v2 format
  source_attributes?: CensusV2SourceAttributes;
  destination_attributes?: CensusV2DestinationAttributes;
  // Common fields
  enabled?: boolean;
  paused?: boolean;
  schedule?: string;
  // Rich fields for AI agent
  operation?: string;           // upsert, insert, update, mirror
  cron_expression?: string;     // Schedule cron expression
  schedule_frequency?: string;  // expression, daily, hourly, etc.
}

// Normalized sync format used internally
export interface CensusSync {
  id: number;
  label?: string;
  source: CensusSyncSource;
  destination: CensusSyncDestination;
  enabled?: boolean;
  schedule?: string;
  // Additional fields for v2 format matching
  sourceTableRefs?: string[];  // Table references extracted from query
  sourceDisplayName?: string;   // Human-readable name for logging
  // Rich sync details for AI agent
  sourceQuery?: string;         // The actual SQL query
  operation?: string;           // upsert, insert, update, mirror
  cronExpression?: string;      // Schedule cron expression
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
    syncNodesCreated: number;
    edgesCreated: number;
    loopBacksDetected: number;
    destinationNodesCreated: number;
    destinationEdgesCreated: number;
    matchedSources: Array<{ censusName: string; matchedNodeId: string; matchedNodeName: string }>;
    unmatchedSources: string[];
    unmatchedDestinations: string[];
    destinationTypes: string[];
  };
}

// ============================================================================
// Node Matching Functions
// ============================================================================

/**
 * Parse a fully qualified table name into its components.
 * Handles: database.schema.table, schema.table, or just table
 */
function parseTableName(fullName: string): { database?: string; schema?: string; table: string } {
  const parts = fullName.toLowerCase().trim().split(".");
  
  if (parts.length >= 3) {
    // database.schema.table (take last 3 parts)
    return {
      database: parts[parts.length - 3],
      schema: parts[parts.length - 2],
      table: parts[parts.length - 1],
    };
  } else if (parts.length === 2) {
    // schema.table
    return {
      schema: parts[0],
      table: parts[1],
    };
  } else {
    // just table
    return { table: parts[0] };
  }
}

/**
 * Find a graph node that matches a Census source object name.
 * Census sources can be:
 * - Fully qualified: prod_dbt_db.core_growth.mart_growth__model_name
 * - Schema qualified: growth.mechanized_outreach_population
 * - Simple name: mart_growth__model_name
 */
export function findSourceNode(
  sourceName: string,
  nodes: GraphNode[]
): GraphNode | null {
  const normalizedSource = sourceName.toLowerCase().trim();
  const parsed = parseTableName(normalizedSource);
  
  // 1. Try exact match on full source name against node id
  const byFullId = nodes.find(n => 
    n.id.toLowerCase() === normalizedSource
  );
  if (byFullId) return byFullId;
  
  // 2. Try exact match on name
  const byName = nodes.find(n => 
    n.name.toLowerCase() === normalizedSource
  );
  if (byName) return byName;
  
  // 3. Try matching just the table name (last part of FQN)
  const byTableName = nodes.find(n => 
    n.name.toLowerCase() === parsed.table
  );
  if (byTableName) return byTableName;
  
  // 4. Try FQN suffix match (node.id ends with the source)
  const bySuffix = nodes.find(n => 
    n.id.toLowerCase().endsWith(`.${normalizedSource}`) ||
    n.id.toLowerCase().endsWith(`.${parsed.table}`)
  );
  if (bySuffix) return bySuffix;
  
  // 5. Try matching schema.table pattern against node id
  if (parsed.schema) {
    const schemaTable = `${parsed.schema}.${parsed.table}`;
    const bySchemaTable = nodes.find(n => 
      n.id.toLowerCase().includes(schemaTable) ||
      n.id.toLowerCase().endsWith(`.${parsed.table}`)
    );
    if (bySchemaTable) return bySchemaTable;
  }
  
  // 6. Try partial match (handles dbt naming conventions like mart_growth__model)
  const byPartial = nodes.find(n => {
    const nodeName = n.name.toLowerCase();
    return nodeName === parsed.table || 
           nodeName.endsWith(`__${parsed.table}`) ||
           parsed.table.endsWith(`__${nodeName}`) ||
           // Also check if the table name contains the node name
           parsed.table.includes(nodeName) ||
           nodeName.includes(parsed.table);
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
 * Parse a destination object string to extract the specific object being updated.
 * Different destination types have different object formats:
 * - Salesforce: "Contact", "Lead", "Account", "Custom_Object__c"
 * - HubSpot: "contacts", "companies", "deals"
 * - Google Sheets: JSON with spreadsheet_id and sheet_id
 * - HTTP: "sync:uuid" format
 * - LinkedIn: audience IDs
 */
function parseDestinationObject(
  destObject: string,
  connectionType: string
): { objectName: string | null; displayName: string } {
  if (!destObject) {
    return { objectName: null, displayName: connectionType };
  }

  // Handle JSON objects (Google Sheets, etc.)
  if (destObject.startsWith("{")) {
    try {
      const parsed = JSON.parse(destObject);
      if (parsed.spreadsheet_id) {
        return { objectName: "spreadsheet", displayName: "Spreadsheet" };
      }
      return { objectName: null, displayName: connectionType };
    } catch {
      return { objectName: null, displayName: connectionType };
    }
  }

  // Handle sync:uuid format (HTTP webhooks)
  if (destObject.startsWith("sync:")) {
    return { objectName: "webhook", displayName: "Webhook" };
  }

  // Handle Salesforce objects
  if (connectionType === "salesforce") {
    // Clean up the object name
    const objName = destObject.toLowerCase().replace(/_c$/, "").replace(/__c$/, "");
    const displayName = formatSalesforceObject(destObject);
    return { objectName: objName, displayName };
  }

  // Handle HubSpot objects
  if (connectionType === "hubspot") {
    const hubspotObjects: Record<string, string> = {
      contacts: "Contacts",
      companies: "Companies",
      deals: "Deals",
      tickets: "Tickets",
      products: "Products",
      line_items: "Line Items",
    };
    const display = hubspotObjects[destObject.toLowerCase()] || destObject;
    return { objectName: destObject.toLowerCase(), displayName: display };
  }

  // Handle LinkedIn (usually audience lists)
  if (connectionType === "linkedin") {
    return { objectName: "audience", displayName: "Ad Audience" };
  }

  // Handle Brevo
  if (connectionType === "brevo") {
    if (destObject.toLowerCase().includes("contact")) {
      return { objectName: "contacts", displayName: "Contacts" };
    }
    if (destObject.toLowerCase().includes("list")) {
      return { objectName: "lists", displayName: "Lists" };
    }
    return { objectName: destObject.toLowerCase(), displayName: destObject };
  }

  // Handle Outreach
  if (connectionType === "outreach") {
    const outreachObjects: Record<string, string> = {
      prospects: "Prospects",
      accounts: "Accounts",
      sequences: "Sequences",
      users: "Users",
    };
    const display = outreachObjects[destObject.toLowerCase()] || destObject;
    return { objectName: destObject.toLowerCase(), displayName: display };
  }

  // Handle Marketo
  if (connectionType === "marketo") {
    return { objectName: "leads", displayName: "Leads" };
  }

  // Generic handling for other types
  if (destObject.length < 50 && !destObject.includes(" ")) {
    return { 
      objectName: destObject.toLowerCase().replace(/[^a-z0-9_]/g, "_"), 
      displayName: destObject.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())
    };
  }

  return { objectName: null, displayName: connectionType };
}

/**
 * Format Salesforce object names for display.
 */
function formatSalesforceObject(objName: string): string {
  // Common Salesforce objects
  const sfObjects: Record<string, string> = {
    contact: "Contact",
    lead: "Lead",
    account: "Account",
    opportunity: "Opportunity",
    user: "User",
    campaign: "Campaign",
    campaignmember: "Campaign Member",
    task: "Task",
    event: "Event",
    case: "Case",
  };
  
  const normalized = objName.toLowerCase().replace(/__c$/, "").replace(/_c$/, "");
  
  if (sfObjects[normalized]) {
    return sfObjects[normalized];
  }
  
  // Handle custom objects (ends with __c)
  if (objName.endsWith("__c") || objName.endsWith("_c")) {
    const baseName = objName.replace(/__c$/, "").replace(/_c$/, "");
    return baseName.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase()) + " (Custom)";
  }
  
  return objName.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
}

/**
 * Format a human-readable name for a Census destination type.
 */
function formatDestinationName(connectionType: string, connectionName?: string): string {
  // Map connection types to friendly names
  const typeNames: Record<string, string> = {
    salesforce: "Salesforce",
    hubspot: "HubSpot",
    google_sheets: "Google Sheets",
    linkedin: "LinkedIn Ads",
    facebook_ads: "Facebook Ads",
    google_ads: "Google Ads",
    http_request: "HTTP/Webhook",
    brevo: "Brevo",
    marketo: "Marketo",
    outreach: "Outreach",
    intercom: "Intercom",
    zendesk: "Zendesk",
    slack: "Slack",
    segment: "Segment",
    amplitude: "Amplitude",
    mixpanel: "Mixpanel",
    snowflake: "Snowflake",
    bigquery: "BigQuery",
    redshift: "Redshift",
    postgres: "PostgreSQL",
    mysql: "MySQL",
    optimizely: "Optimizely",
    casita: "Census Store",
    iterable: "Iterable",
    customer_io: "Customer.io",
    klaviyo: "Klaviyo",
    attio: "Attio",
    clay: "Clay",
  };
  
  return typeNames[connectionType] || connectionType.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
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
 * Parse Census sync configuration and create graph nodes and edges.
 * 
 * Creates individual nodes for each Census sync, connecting:
 * 1. source_model → [Census Sync Node] → destination_object
 * 
 * This provides granular visibility into each data flow.
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
      syncNodesCreated: 0,
      edgesCreated: 0,
      loopBacksDetected: 0,
      destinationNodesCreated: 0,
      destinationEdgesCreated: 0,
      matchedSources: [],
      unmatchedSources: [],
      unmatchedDestinations: [],
      destinationTypes: [],
    },
  };
  
  const { syncs, connections = [] } = config;
  
  // Build connection lookup map for destination type resolution
  const connectionMap = new Map<number, CensusConnection>();
  for (const conn of connections) {
    connectionMap.set(conn.id, conn);
  }
  
  if (!syncs || syncs.length === 0) {
    onProgress?.(100, "No Census syncs found in configuration");
    return result;
  }
  
  onProgress?.(0, `Processing ${syncs.length} Census syncs...`);
  
  // Track edges, sync nodes, and destination nodes we've created to avoid duplicates
  const createdEdges = new Set<string>();
  const createdSyncNodes = new Set<string>();
  const createdDestNodes = new Set<string>();
  
  // Process each sync - create a node for each sync
  for (let i = 0; i < syncs.length; i++) {
    const sync = syncs[i];
    result.stats.syncsProcessed++;
    
    // Skip disabled/paused syncs
    if (sync.enabled === false) {
      continue;
    }
    
    const syncLabel = sync.label || sync.sourceDisplayName || `Sync ${sync.id}`;
    const censusDisplayName = sync.sourceDisplayName || sync.source.object;
    const tableRefs = sync.sourceTableRefs || [sync.source.object];
    
    // Get destination info for naming
    const destConnection = connectionMap.get(sync.destination.connection_id);
    const destConnectionType = destConnection?.type?.toLowerCase() || 
                               getConnectionType(sync.destination.connection_id, connections)?.toLowerCase();
    const destObjectInfo = parseDestinationObject(sync.destination.object, destConnectionType || "unknown");
    
    // Create a unique sync node ID
    const syncNodeId = `census.sync.${sync.id}`;
    
    // Create readable sync name: "Sync: Label → Destination"
    const destName = destConnectionType 
      ? `${formatDestinationName(destConnectionType)}${destObjectInfo.objectName ? ` (${destObjectInfo.displayName})` : ""}`
      : "External";
    const syncNodeName = syncLabel.length > 40 
      ? `${syncLabel.substring(0, 37)}...`
      : syncLabel;
    
    // Try to find source node(s) in the graph
    // First try matching table references (from SQL query), then fall back to display name
    let matchedNodes: GraphNode[] = [];
    
    for (const tableRef of tableRefs) {
      const node = findSourceNode(tableRef, existingNodes);
      if (node && !matchedNodes.some(n => n.id === node.id)) {
        matchedNodes.push(node);
      }
    }
    
    // If no matches from table refs, try the display name
    if (matchedNodes.length === 0) {
      const node = findSourceNode(censusDisplayName, existingNodes);
      if (node) {
        matchedNodes.push(node);
      }
    }
    
    if (matchedNodes.length === 0) {
      result.stats.unmatchedSources.push(censusDisplayName);
      continue;
    }
    
    // Track successful matches (may have multiple from one sync's query)
    for (const sourceNode of matchedNodes) {
      result.stats.matchedSources.push({
        censusName: censusDisplayName,
        matchedNodeId: sourceNode.id,
        matchedNodeName: sourceNode.name,
      });
    }
    
    // =========================================================================
    // Create the Census Sync node (one per sync)
    // =========================================================================
    if (!createdSyncNodes.has(syncNodeId)) {
      result.nodes.push({
        id: syncNodeId,
        name: syncNodeName,
        type: "external",
        subtype: "census_sync",
        repo: "census",
        metadata: {
          description: `Census sync: ${syncLabel}`,
          censusSyncId: sync.id,
          sourceQuery: sync.sourceQuery,
          operation: sync.operation,
          schedule: sync.cronExpression,
          destinationType: destConnectionType,
          destinationObject: destObjectInfo.displayName,
          tags: ["census", "sync", destConnectionType].filter(Boolean) as string[],
        },
        semanticLayer: "external",
      });
      createdSyncNodes.add(syncNodeId);
      result.stats.syncNodesCreated++;
    }
    
    // =========================================================================
    // Create edges: source_model(s) → Sync Node
    // =========================================================================
    for (const sourceNode of matchedNodes) {
      const sourceEdgeKey = `${sourceNode.id}|${syncNodeId}`;
      if (!createdEdges.has(sourceEdgeKey)) {
        result.edges.push({
          id: uuid(),
          from: sourceNode.id,
          to: syncNodeId,
          type: "reverse_etl",
          metadata: {
            transformationType: "census-source",
            censusSyncId: sync.id,
            censusSyncLabel: syncLabel,
            inferredFrom: "census-config",
          },
        });
        createdEdges.add(sourceEdgeKey);
        result.stats.edgesCreated++;
      }
    }
    
    // =========================================================================
    // Create destination node and edge: Sync Node → Destination
    // =========================================================================
    const isSnowflakeDest = destConnectionType === "snowflake" ||
                           (connections.length === 0 && sync.destination.object.includes("."));
    
    if (isSnowflakeDest) {
      // Try to find the destination as a dbt source/staging model (loop-back pattern)
      const destNode = findDestinationSourceNode(sync.destination.object, existingNodes);
      
      if (destNode) {
        const destEdgeKey = `${syncNodeId}|${destNode.id}`;
        if (!createdEdges.has(destEdgeKey)) {
          result.edges.push({
            id: uuid(),
            from: syncNodeId,
            to: destNode.id,
            type: "reverse_etl",
            metadata: {
              transformationType: "census-loopback",
              censusSyncId: sync.id,
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
    } else if (destConnectionType && destConnection) {
      // Create granular destination node ID
      const destNodeId = destObjectInfo.objectName 
        ? `external.census_dest.${destConnectionType}.${destObjectInfo.objectName.toLowerCase()}`
        : `external.census_dest.${destConnectionType}`;
      
      // Create human-readable name
      const destNodeName = destObjectInfo.objectName
        ? `${formatDestinationName(destConnectionType)} → ${destObjectInfo.displayName}`
        : formatDestinationName(destConnectionType, destConnection.name);
      
      // Track this destination type
      if (!result.stats.destinationTypes.includes(destConnectionType)) {
        result.stats.destinationTypes.push(destConnectionType);
      }
      
      // Create destination node if it doesn't exist
      if (!createdDestNodes.has(destNodeId)) {
        result.nodes.push({
          id: destNodeId,
          name: destNodeName,
          type: "external",
          subtype: "census_destination",
          repo: "census",
          metadata: {
            description: destObjectInfo.objectName
              ? `Census syncs to ${formatDestinationName(destConnectionType)} ${destObjectInfo.displayName} object`
              : `Census destination: ${destConnection.name}`,
            connectionType: destConnectionType,
            objectType: destObjectInfo.objectName ?? undefined,
            tags: ["census", "destination", destConnectionType, destObjectInfo.objectName].filter(Boolean) as string[],
          },
          semanticLayer: "external",
        });
        createdDestNodes.add(destNodeId);
        result.stats.destinationNodesCreated++;
      }
      
      // Create edge: Sync Node → Destination
      const destEdgeKey = `${syncNodeId}|${destNodeId}`;
      if (!createdEdges.has(destEdgeKey)) {
        result.edges.push({
          id: uuid(),
          from: syncNodeId,
          to: destNodeId,
          type: "reverse_etl",
          metadata: {
            transformationType: "census-destination",
            censusSyncId: sync.id,
            destinationType: destConnectionType,
            destinationObject: destObjectInfo.objectName || sync.destination.object,
            inferredFrom: "census-config",
          },
        });
        createdEdges.add(destEdgeKey);
        result.stats.destinationEdgesCreated++;
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
 * Supports both v1 (source/destination) and v2 (source_attributes/destination_attributes) formats.
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
  
  // Validate first sync has required fields (support both v1 and v2 formats)
  const firstSync = syncs[0] as Record<string, unknown>;
  const hasV1Format = firstSync.source && firstSync.destination;
  const hasV2Format = firstSync.source_attributes && firstSync.destination_attributes;
  
  if (!hasV1Format && !hasV2Format) {
    return { valid: false, error: "Sync missing required source/destination fields (checked both v1 and v2 formats)" };
  }
  
  return { valid: true };
}

/**
 * Extract all table references from a SQL query.
 * Looks for patterns like: FROM schema.table, JOIN schema.table, etc.
 */
function extractTablesFromQuery(query: string): string[] {
  const tables: Set<string> = new Set();
  
  // Match fully qualified table names (database.schema.table or schema.table)
  // Look for FROM, JOIN, and similar clauses
  const tablePatterns = [
    /FROM\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
    /JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
    /LEFT\s+JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
    /RIGHT\s+JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
    /INNER\s+JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
    /CROSS\s+JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi,
  ];
  
  for (const pattern of tablePatterns) {
    let match;
    while ((match = pattern.exec(query)) !== null) {
      tables.add(match[1].toLowerCase());
    }
  }
  
  return Array.from(tables);
}

/**
 * Extract model name from a Census v2 source object.
 * Returns the human-readable name for logging purposes.
 */
function extractSourceModelName(sourceObj: CensusV2SourceObject | string): string {
  if (typeof sourceObj === "string") {
    return sourceObj;
  }
  
  // Prefer the name field (for display/logging)
  if (sourceObj.name) {
    return sourceObj.name;
  }
  
  // Try to extract from resource_identifier (e.g., "model:prod-ca-core-v3-cannon")
  if (sourceObj.resource_identifier) {
    const match = sourceObj.resource_identifier.match(/^model:(.+)$/);
    if (match) {
      return match[1];
    }
    return sourceObj.resource_identifier;
  }
  
  return `unknown-source-${sourceObj.id || ""}`;
}

/**
 * Extract table references from the query in a Census v2 source object.
 * These are the actual tables that can be matched to dbt models.
 */
function extractSourceTableRefs(sourceObj: CensusV2SourceObject | string): string[] {
  if (typeof sourceObj === "string") {
    return [sourceObj];
  }
  
  // If there's a query, extract table references from it
  if (sourceObj.query) {
    const tables = extractTablesFromQuery(sourceObj.query);
    if (tables.length > 0) {
      return tables;
    }
  }
  
  // Fall back to name or resource_identifier
  if (sourceObj.name) {
    return [sourceObj.name];
  }
  
  if (sourceObj.resource_identifier) {
    return [sourceObj.resource_identifier];
  }
  
  return [];
}

/**
 * Normalize a single raw sync to our internal format.
 * Handles both v1 and v2 API formats.
 */
function normalizeSync(rawSync: CensusRawSync): CensusSync | null {
  // v1 format - already normalized
  if (rawSync.source && rawSync.destination) {
    return {
      id: rawSync.id,
      label: rawSync.label,
      source: rawSync.source,
      destination: rawSync.destination,
      enabled: rawSync.enabled,
      schedule: rawSync.schedule,
    };
  }
  
  // v2 format - needs conversion
  if (rawSync.source_attributes && rawSync.destination_attributes) {
    const sourceDisplayName = extractSourceModelName(rawSync.source_attributes.object);
    const sourceTableRefs = extractSourceTableRefs(rawSync.source_attributes.object);
    
    // Extract the SQL query from the source object
    const sourceObj = rawSync.source_attributes.object;
    const sourceQuery = typeof sourceObj === "object" ? sourceObj.query : undefined;
    
    return {
      id: rawSync.id,
      label: rawSync.label,
      source: {
        connection_id: rawSync.source_attributes.connection_id,
        object: sourceDisplayName,
      },
      destination: {
        connection_id: rawSync.destination_attributes.connection_id,
        object: rawSync.destination_attributes.object,
      },
      // v2 uses 'paused' instead of 'enabled'
      enabled: rawSync.paused === true ? false : rawSync.enabled,
      schedule: rawSync.schedule,
      // v2-specific: table refs extracted from query for better matching
      sourceTableRefs,
      sourceDisplayName,
      // Rich details for AI agent
      sourceQuery,
      operation: rawSync.operation,
      cronExpression: rawSync.cron_expression,
    };
  }
  
  // Invalid sync - skip it
  return null;
}

/**
 * Normalize Census API response to our internal format.
 * Census API may return data in different formats depending on endpoint version.
 */
export function normalizeCensusResponse(data: unknown): CensusConfig {
  const obj = data as Record<string, unknown>;
  
  // Get raw syncs array
  let rawSyncs: CensusRawSync[] = [];
  if (Array.isArray(obj.syncs)) {
    rawSyncs = obj.syncs as CensusRawSync[];
  } else if (Array.isArray(obj.data)) {
    rawSyncs = obj.data as CensusRawSync[];
  }
  
  // Normalize each sync
  const normalizedSyncs: CensusSync[] = [];
  for (const rawSync of rawSyncs) {
    const normalized = normalizeSync(rawSync);
    if (normalized) {
      normalizedSyncs.push(normalized);
    }
  }
  
  return {
    syncs: normalizedSyncs,
    connections: (obj.connections || obj.destinations || []) as CensusConnection[],
  };
}
