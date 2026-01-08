/**
 * External System Inference Engine
 * 
 * Automatically detects external data stores from SQL patterns, model names,
 * column names, and source references. Creates bidirectional edges:
 * - Inbound: External system → staging (data source via Fivetran)
 * - Outbound: Mart → external system (data destination via Census/reverse ETL)
 */

import { v4 as uuid } from "uuid";
import type { GraphNode, GraphEdge, Citation, NodeSubtype } from "../types";

// ============================================================================
// External System Pattern Registry
// ============================================================================

export interface ExternalSystemPattern {
  /** Display name for the external system */
  name: string;
  /** System category */
  type: "application" | "reverse_etl" | "dashboard" | "crm" | "marketing";
  /** Node subtype for the graph */
  subtype: NodeSubtype;
  /** Description for the external system node */
  description: string;
  /** Patterns that indicate DATA FROM this system (via Fivetran/ETL) */
  sourcePatterns: {
    /** Staging model name patterns (e.g., stg_outreach__*) */
    stagingModels?: RegExp[];
    /** dbt source() call patterns */
    sourceRefs?: RegExp[];
    /** Raw Fivetran schema names */
    fivetranSchemas?: string[];
  };
  /** Patterns that indicate DATA TO this system (via reverse ETL) */
  destinationPatterns: {
    /** Model name patterns that imply this destination */
    modelNames?: RegExp[];
    /** Column name patterns in output */
    columnNames?: RegExp[];
    /** Macro name patterns used in SQL */
    macroNames?: RegExp[];
    /** Tags that indicate this destination */
    tags?: string[];
  };
}

/**
 * Registry of known external systems with detection patterns.
 * Order matters - more specific patterns should come first.
 */
export const EXTERNAL_SYSTEM_PATTERNS: ExternalSystemPattern[] = [
  // ============================================================================
  // Sales & Marketing Engagement
  // ============================================================================
  {
    name: "Outreach.io",
    type: "application",
    subtype: "application",
    description: "Sales engagement platform for automated email sequences",
    sourcePatterns: {
      stagingModels: [/^stg_outreach__/i, /^int_outreach__/i],
      sourceRefs: [/source\s*\(\s*['"]outreach['"]/i, /source\s*\(\s*['"]OUTREACH['"]/i],
      fivetranSchemas: ["OUTREACH", "outreach"],
    },
    destinationPatterns: {
      modelNames: [/outreach_sync/i, /outreach_eligible/i, /_for_outreach/i, /mo_eligible_audience/i],
      columnNames: [/sequence_id/i, /outreach_sequence/i, /final_sequence/i, /is_eligible_outreach/i],
      macroNames: [/outreach_/i],
    },
  },
  {
    name: "Brevo",
    type: "marketing",
    subtype: "application",
    description: "Email marketing and automation platform (formerly Sendinblue)",
    sourcePatterns: {
      stagingModels: [/^stg_brevo__/i, /^stg_sendinblue__/i],
      sourceRefs: [/source\s*\(\s*['"]brevo['"]/i, /source\s*\(\s*['"]sendinblue['"]/i],
      fivetranSchemas: ["BREVO", "SENDINBLUE", "brevo", "sendinblue"],
    },
    destinationPatterns: {
      modelNames: [/brevo_sync/i, /brevo_eligible/i, /_for_brevo/i, /brevo_nls/i],
      columnNames: [/brevo_list_id/i, /is_eligible_brevo/i, /lc_is_eligible_brevo/i, /brevo_campaign/i],
      macroNames: [/brevo_nls_audience/i, /brevo_/i],
    },
  },
  {
    name: "HubSpot",
    type: "marketing",
    subtype: "application",
    description: "Marketing, sales, and service platform",
    sourcePatterns: {
      stagingModels: [/^stg_hubspot__/i],
      sourceRefs: [/source\s*\(\s*['"]hubspot['"]/i],
      fivetranSchemas: ["HUBSPOT", "hubspot"],
    },
    destinationPatterns: {
      modelNames: [/hubspot_sync/i, /_for_hubspot/i, /hubspot_export/i],
      columnNames: [/hubspot_contact_id/i, /hs_/i],
    },
  },
  {
    name: "Marketo",
    type: "marketing",
    subtype: "application",
    description: "Marketing automation platform",
    sourcePatterns: {
      stagingModels: [/^stg_marketo__/i],
      sourceRefs: [/source\s*\(\s*['"]marketo['"]/i],
      fivetranSchemas: ["MARKETO", "marketo"],
    },
    destinationPatterns: {
      modelNames: [/marketo_sync/i, /_for_marketo/i],
      columnNames: [/marketo_lead_id/i, /mkto_/i],
    },
  },
  {
    name: "Iterable",
    type: "marketing",
    subtype: "application",
    description: "Cross-channel marketing platform",
    sourcePatterns: {
      stagingModels: [/^stg_iterable__/i],
      sourceRefs: [/source\s*\(\s*['"]iterable['"]/i],
      fivetranSchemas: ["ITERABLE", "iterable"],
    },
    destinationPatterns: {
      modelNames: [/iterable_sync/i, /_for_iterable/i],
      columnNames: [/iterable_user_id/i],
    },
  },

  // ============================================================================
  // CRM Systems
  // ============================================================================
  {
    name: "Salesforce",
    type: "crm",
    subtype: "application",
    description: "Customer relationship management platform",
    sourcePatterns: {
      stagingModels: [/^stg_salesforce__/i, /^stg_sfdc__/i],
      sourceRefs: [/source\s*\(\s*['"]salesforce['"]/i, /source\s*\(\s*['"]sfdc['"]/i],
      fivetranSchemas: ["SALESFORCE", "SFDC", "salesforce", "sfdc"],
    },
    destinationPatterns: {
      modelNames: [/sfdc_sync/i, /salesforce_sync/i, /_to_salesforce/i, /_for_sfdc/i],
      columnNames: [/sfdc_account_id/i, /sfdc_contact_id/i, /sfdc_opportunity_id/i, /salesforce_id/i],
      tags: ["sfdc", "salesforce"],
    },
  },

  // ============================================================================
  // Reverse ETL Platforms
  // ============================================================================
  {
    name: "Census",
    type: "reverse_etl",
    subtype: "reverse_etl",
    description: "Reverse ETL platform for syncing data to business tools",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^census_/i, /_census$/i, /census_sync/i],
      tags: ["census", "reverse_etl"],
    },
  },
  {
    name: "Hightouch",
    type: "reverse_etl",
    subtype: "reverse_etl",
    description: "Reverse ETL platform for operational analytics",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^hightouch_/i, /_hightouch$/i, /hightouch_sync/i],
      tags: ["hightouch", "reverse_etl"],
    },
  },

  // ============================================================================
  // BI & Dashboard Platforms
  // ============================================================================
  {
    name: "Looker",
    type: "dashboard",
    subtype: "dashboard",
    description: "Business intelligence and data visualization platform",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^looker_/i, /_for_looker/i, /looker_export/i],
      tags: ["looker", "bi", "dashboard"],
    },
  },
  {
    name: "Tableau",
    type: "dashboard",
    subtype: "dashboard",
    description: "Data visualization and analytics platform",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^tableau_/i, /_for_tableau/i],
      tags: ["tableau", "bi", "dashboard"],
    },
  },
  {
    name: "Mode",
    type: "dashboard",
    subtype: "dashboard",
    description: "Collaborative data science platform",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^mode_/i, /_for_mode/i],
      tags: ["mode", "bi"],
    },
  },
  {
    name: "Metabase",
    type: "dashboard",
    subtype: "dashboard",
    description: "Open source business intelligence tool",
    sourcePatterns: {},
    destinationPatterns: {
      modelNames: [/^metabase_/i, /_for_metabase/i],
      tags: ["metabase", "bi"],
    },
  },

  // ============================================================================
  // Data Enrichment & Prospecting
  // ============================================================================
  {
    name: "Apollo",
    type: "application",
    subtype: "application",
    description: "Sales intelligence and engagement platform",
    sourcePatterns: {
      stagingModels: [/^stg_apollo__/i],
      sourceRefs: [/source\s*\(\s*['"]apollo['"]/i],
      fivetranSchemas: ["APOLLO", "apollo"],
    },
    destinationPatterns: {
      modelNames: [/apollo_sync/i, /_for_apollo/i],
    },
  },
  {
    name: "ZoomInfo",
    type: "application",
    subtype: "application",
    description: "B2B contact database and sales intelligence",
    sourcePatterns: {
      stagingModels: [/^stg_zoominfo__/i, /^stg_zi__/i],
      sourceRefs: [/source\s*\(\s*['"]zoominfo['"]/i],
      fivetranSchemas: ["ZOOMINFO", "zoominfo"],
    },
    destinationPatterns: {},
  },
  {
    name: "Cognism",
    type: "application",
    subtype: "application",
    description: "Sales intelligence and prospecting platform",
    sourcePatterns: {
      stagingModels: [/^stg_cognism__/i],
      sourceRefs: [/source\s*\(\s*['"]cognism['"]/i],
      fivetranSchemas: ["COGNISM", "cognism"],
    },
    destinationPatterns: {},
  },
  {
    name: "Clay",
    type: "application",
    subtype: "application",
    description: "Data enrichment and automation platform",
    sourcePatterns: {
      stagingModels: [/^stg_clay__/i],
      sourceRefs: [/source\s*\(\s*['"]clay['"]/i],
      fivetranSchemas: ["CLAY", "clay"],
    },
    destinationPatterns: {
      modelNames: [/clay_sync/i, /_for_clay/i, /clay_results/i],
    },
  },

  // ============================================================================
  // Intent Data Providers
  // ============================================================================
  {
    name: "G2",
    type: "application",
    subtype: "application",
    description: "Software review and intent data platform",
    sourcePatterns: {
      stagingModels: [/^stg_g2__/i],
      sourceRefs: [/source\s*\(\s*['"]g2['"]/i],
      fivetranSchemas: ["G2", "g2"],
    },
    destinationPatterns: {},
  },
  {
    name: "Gartner",
    type: "application",
    subtype: "application",
    description: "Technology research and intent signals",
    sourcePatterns: {
      stagingModels: [/^stg_gartner__/i],
      sourceRefs: [/source\s*\(\s*['"]gartner['"]/i],
      fivetranSchemas: ["GARTNER", "gartner"],
    },
    destinationPatterns: {},
  },
];

// ============================================================================
// Inference Types
// ============================================================================

export interface ExternalDetection {
  /** The external system that was detected */
  system: ExternalSystemPattern;
  /** How the system was detected */
  detectionType: "source" | "destination";
  /** Specific patterns that matched */
  matchedPatterns: string[];
  /** Confidence score 0-1 based on number of matches */
  confidence: number;
}

export interface ModelExternalAnalysis {
  /** The model being analyzed */
  nodeId: string;
  nodeName: string;
  /** External systems this model receives data FROM */
  sources: ExternalDetection[];
  /** External systems this model sends data TO */
  destinations: ExternalDetection[];
}

export interface ExternalInferenceResult {
  /** New external system nodes to create */
  nodes: GraphNode[];
  /** Edges from models to/from external systems */
  edges: GraphEdge[];
  /** Citations for the external nodes */
  citations: Citation[];
  /** Analysis details for each model */
  analyses: ModelExternalAnalysis[];
}

// ============================================================================
// Inference Functions
// ============================================================================

/**
 * Extract macro names from SQL content.
 * Matches patterns like {{ macro_name(...) }}
 */
export function extractMacroNames(sql: string): string[] {
  const macros: string[] = [];
  const macroPattern = /\{\{\s*(\w+)\s*\(/g;
  let match;
  while ((match = macroPattern.exec(sql)) !== null) {
    // Skip common dbt macros
    const name = match[1];
    if (!["ref", "source", "config", "var", "env_var", "this", "adapter"].includes(name)) {
      macros.push(name);
    }
  }
  return [...new Set(macros)];
}

/**
 * Extract column names from SQL SELECT statement.
 * This is a heuristic - won't catch all cases but gets common patterns.
 */
export function extractSelectColumns(sql: string): string[] {
  const columns: string[] = [];
  
  // Normalize SQL
  const normalized = sql
    .replace(/--.*$/gm, "") // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, "") // Remove block comments
    .replace(/\s+/g, " ");
  
  // Match column aliases (AS column_name)
  const aliasPattern = /\bAS\s+["']?(\w+)["']?/gi;
  let match;
  while ((match = aliasPattern.exec(normalized)) !== null) {
    columns.push(match[1].toLowerCase());
  }
  
  // Match direct column references (table.column or just column)
  // This is less reliable but catches some cases
  const selectPattern = /SELECT\s+([\s\S]*?)\s+FROM/i;
  const selectMatch = selectPattern.exec(normalized);
  if (selectMatch) {
    const selectClause = selectMatch[1];
    // Split by comma and extract column names
    const parts = selectClause.split(",");
    for (const part of parts) {
      const trimmed = part.trim();
      // Get the last word (column name or alias)
      const words = trimmed.split(/\s+/);
      const lastWord = words[words.length - 1]?.replace(/['"]/g, "");
      if (lastWord && /^[a-z_][a-z0-9_]*$/i.test(lastWord)) {
        columns.push(lastWord.toLowerCase());
      }
    }
  }
  
  return [...new Set(columns)];
}

/**
 * Extract ref() calls from SQL.
 */
export function extractRefs(sql: string): string[] {
  const refs: string[] = [];
  const refPattern = /\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g;
  let match;
  while ((match = refPattern.exec(sql)) !== null) {
    refs.push(match[1].toLowerCase());
  }
  return refs;
}

/**
 * Extract source() calls from SQL.
 */
export function extractSources(sql: string): Array<{ source: string; table: string }> {
  const sources: Array<{ source: string; table: string }> = [];
  const sourcePattern = /\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g;
  let match;
  while ((match = sourcePattern.exec(sql)) !== null) {
    sources.push({ source: match[1], table: match[2] });
  }
  return sources;
}

/**
 * Analyze a single model for external system connections.
 */
export function analyzeModelForExternals(
  node: GraphNode,
  sql: string | null,
  allNodes: GraphNode[]
): ModelExternalAnalysis {
  const sources: ExternalDetection[] = [];
  const destinations: ExternalDetection[] = [];
  
  // Get model metadata
  const modelName = node.name.toLowerCase();
  const tags = node.metadata?.tags?.map(t => t.toLowerCase()) || [];
  
  // Extract patterns from SQL
  const macros = sql ? extractMacroNames(sql) : [];
  const columns = sql ? extractSelectColumns(sql) : [];
  const refs = sql ? extractRefs(sql) : [];
  const sqlSources = sql ? extractSources(sql) : [];
  
  // Check each external system
  for (const system of EXTERNAL_SYSTEM_PATTERNS) {
    const sourceMatches: string[] = [];
    const destMatches: string[] = [];
    
    // Check SOURCE patterns (data coming FROM this system)
    const sp = system.sourcePatterns;
    
    // Check staging model refs
    if (sp.stagingModels) {
      for (const pattern of sp.stagingModels) {
        // Check direct model name
        if (pattern.test(modelName)) {
          sourceMatches.push(`model name matches ${pattern.source}`);
        }
        // Check refs to staging models
        for (const ref of refs) {
          if (pattern.test(ref)) {
            sourceMatches.push(`refs ${ref} (matches ${pattern.source})`);
          }
        }
      }
    }
    
    // Check source() calls
    if (sp.sourceRefs && sql) {
      for (const pattern of sp.sourceRefs) {
        if (pattern.test(sql)) {
          sourceMatches.push(`SQL contains source() matching ${pattern.source}`);
        }
      }
    }
    
    // Check Fivetran schema references
    if (sp.fivetranSchemas) {
      for (const schema of sp.fivetranSchemas) {
        for (const src of sqlSources) {
          if (src.source.toUpperCase() === schema.toUpperCase()) {
            sourceMatches.push(`source('${src.source}', '${src.table}')`);
          }
        }
      }
    }
    
    // Check DESTINATION patterns (data going TO this system)
    const dp = system.destinationPatterns;
    
    // Check model name patterns
    if (dp.modelNames) {
      for (const pattern of dp.modelNames) {
        if (pattern.test(modelName)) {
          destMatches.push(`model name matches ${pattern.source}`);
        }
      }
    }
    
    // Check column name patterns
    if (dp.columnNames) {
      for (const pattern of dp.columnNames) {
        for (const col of columns) {
          if (pattern.test(col)) {
            destMatches.push(`column '${col}' matches ${pattern.source}`);
          }
        }
      }
    }
    
    // Check macro name patterns
    if (dp.macroNames) {
      for (const pattern of dp.macroNames) {
        for (const macro of macros) {
          if (pattern.test(macro)) {
            destMatches.push(`macro '${macro}' matches ${pattern.source}`);
          }
        }
      }
    }
    
    // Check tags
    if (dp.tags) {
      for (const tag of dp.tags) {
        if (tags.includes(tag.toLowerCase())) {
          destMatches.push(`tag '${tag}'`);
        }
      }
    }
    
    // Add detections if we have matches
    if (sourceMatches.length > 0) {
      sources.push({
        system,
        detectionType: "source",
        matchedPatterns: sourceMatches,
        confidence: Math.min(1, sourceMatches.length * 0.4),
      });
    }
    
    if (destMatches.length > 0) {
      destinations.push({
        system,
        detectionType: "destination",
        matchedPatterns: destMatches,
        confidence: Math.min(1, destMatches.length * 0.4),
      });
    }
  }
  
  return {
    nodeId: node.id,
    nodeName: node.name,
    sources,
    destinations,
  };
}

/**
 * Generate a unique ID for an external system node.
 */
export function getExternalSystemId(system: ExternalSystemPattern): string {
  return `external.${system.type}.${system.name.toLowerCase().replace(/[^a-z0-9]+/g, "_")}`;
}

/**
 * Create an external system node.
 */
export function createExternalNode(system: ExternalSystemPattern): GraphNode {
  return {
    id: getExternalSystemId(system),
    name: system.name,
    type: "external",
    subtype: system.subtype,
    repo: "inferred",
    metadata: {
      description: system.description,
      tags: [system.type],
    },
    semanticLayer: "external",
  };
}

/**
 * Run external inference on all models.
 * 
 * @param nodes - All graph nodes
 * @param getSqlContent - Function to retrieve SQL content for a node
 * @param onProgress - Progress callback
 */
export async function inferExternalSystems(
  nodes: GraphNode[],
  getSqlContent: (node: GraphNode) => string | null,
  onProgress?: (percent: number, message: string) => void
): Promise<ExternalInferenceResult> {
  const result: ExternalInferenceResult = {
    nodes: [],
    edges: [],
    citations: [],
    analyses: [],
  };
  
  // Track which external systems we've detected
  const detectedSystems = new Map<string, {
    system: ExternalSystemPattern;
    sourceNodes: Set<string>;
    destNodes: Set<string>;
    allMatches: string[];
  }>();
  
  onProgress?.(0, "Analyzing models for external system patterns...");
  
  // Analyze each node
  let processed = 0;
  for (const node of nodes) {
    // Skip external nodes themselves
    if (node.type === "external") continue;
    
    const sql = getSqlContent(node);
    const analysis = analyzeModelForExternals(node, sql, nodes);
    result.analyses.push(analysis);
    
    // Track source detections
    for (const source of analysis.sources) {
      const systemId = getExternalSystemId(source.system);
      if (!detectedSystems.has(systemId)) {
        detectedSystems.set(systemId, {
          system: source.system,
          sourceNodes: new Set(),
          destNodes: new Set(),
          allMatches: [],
        });
      }
      const entry = detectedSystems.get(systemId)!;
      entry.sourceNodes.add(node.id);
      entry.allMatches.push(...source.matchedPatterns);
    }
    
    // Track destination detections
    for (const dest of analysis.destinations) {
      const systemId = getExternalSystemId(dest.system);
      if (!detectedSystems.has(systemId)) {
        detectedSystems.set(systemId, {
          system: dest.system,
          sourceNodes: new Set(),
          destNodes: new Set(),
          allMatches: [],
        });
      }
      const entry = detectedSystems.get(systemId)!;
      entry.destNodes.add(node.id);
      entry.allMatches.push(...dest.matchedPatterns);
    }
    
    processed++;
    if (processed % 100 === 0 || processed === nodes.length) {
      const pct = Math.round((processed / nodes.length) * 70);
      onProgress?.(pct, `Analyzed ${processed}/${nodes.length} models`);
    }
  }
  
  onProgress?.(75, `Found ${detectedSystems.size} external systems, creating nodes...`);
  
  // Create external system nodes and edges
  const existingExternalIds = new Set(nodes.filter(n => n.type === "external").map(n => n.id));
  
  for (const [systemId, detection] of detectedSystems) {
    // Skip if already exists
    if (existingExternalIds.has(systemId)) {
      // Still create edges to existing external nodes
      for (const destNodeId of detection.destNodes) {
        result.edges.push({
          id: uuid(),
          from: destNodeId,
          to: systemId,
          type: "exposure",
          metadata: {
            transformationType: "inferred-destination",
          },
        });
      }
      continue;
    }
    
    // Create the external node
    const externalNode = createExternalNode(detection.system);
    externalNode.metadata = {
      ...externalNode.metadata,
      description: `${detection.system.description}\n\nDetected via: ${[...new Set(detection.allMatches)].slice(0, 5).join(", ")}`,
    };
    result.nodes.push(externalNode);
    
    // Add citation
    result.citations.push({
      id: uuid(),
      nodeId: systemId,
      filePath: "inferred from model patterns",
    });
    
    // Create edges: Model → External (destination)
    for (const destNodeId of detection.destNodes) {
      result.edges.push({
        id: uuid(),
        from: destNodeId,
        to: systemId,
        type: "exposure",
        metadata: {
          transformationType: "inferred-destination",
        },
      });
    }
    
    // Note: We don't create source edges here because those are already
    // captured by the ref/source dependencies in dbt. The external system
    // as a "source" is represented by the Fivetran raw tables that staging
    // models read from.
  }
  
  onProgress?.(100, `Created ${result.nodes.length} external nodes with ${result.edges.length} edges`);
  
  return result;
}

/**
 * Get a summary of external system detections for logging.
 */
export function summarizeDetections(result: ExternalInferenceResult): string {
  const bySystem = new Map<string, { sources: number; dests: number }>();
  
  for (const analysis of result.analyses) {
    for (const src of analysis.sources) {
      const name = src.system.name;
      if (!bySystem.has(name)) bySystem.set(name, { sources: 0, dests: 0 });
      bySystem.get(name)!.sources++;
    }
    for (const dest of analysis.destinations) {
      const name = dest.system.name;
      if (!bySystem.has(name)) bySystem.set(name, { sources: 0, dests: 0 });
      bySystem.get(name)!.dests++;
    }
  }
  
  const lines: string[] = [];
  for (const [name, counts] of bySystem) {
    const parts: string[] = [];
    if (counts.sources > 0) parts.push(`${counts.sources} source refs`);
    if (counts.dests > 0) parts.push(`${counts.dests} destinations`);
    if (parts.length > 0) {
      lines.push(`${name}: ${parts.join(", ")}`);
    }
  }
  
  return lines.join("; ");
}
