import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { v4 as uuid } from "uuid";
import * as yaml from "js-yaml";
import type { GraphNode, GraphEdge, Citation, NodeSubtype } from "../types";

/**
 * External system configuration schema
 * 
 * Example externals.yml:
 * ```yaml
 * externals:
 *   - name: Outreach.io Sequences
 *     type: application
 *     description: Sales engagement platform for automated email sequences
 *     url: https://app.outreach.io
 *     consumes:
 *       - mart_growth__mo_eligible_audience
 *       - mart_growth__lsw_lead_data
 *     
 *   - name: Brevo Campaigns
 *     type: application
 *     description: Email marketing platform
 *     consumes:
 *       - mart_growth__mo_eligible_audience
 *       
 *   - name: Looker Dashboard
 *     type: dashboard
 *     url: https://looker.company.com/dashboards/123
 *     consumes:
 *       - mart_bookings__line_items_final
 * ```
 */

export interface ExternalSystemConfig {
  name: string;
  type: "dashboard" | "application" | "notebook" | "ml" | "analysis" | "reverse_etl";
  description?: string;
  url?: string;
  consumes: string[]; // List of model names (short names, not FQNs)
  owner?: string;
  tags?: string[];
}

interface ExternalsConfig {
  externals: ExternalSystemConfig[];
}

export interface ExternalParseResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
  citations: Citation[];
}

/**
 * Map external system type to NodeSubtype
 */
function mapExternalTypeToSubtype(type: string): NodeSubtype {
  const subtypeMap: Record<string, NodeSubtype> = {
    dashboard: "dashboard",
    notebook: "notebook",
    analysis: "analysis",
    ml: "ml_model",
    application: "application",
    reverse_etl: "reverse_etl",
  };
  return subtypeMap[type] || "external_feed";
}

/**
 * Parse externals.yml configuration file
 */
export function parseExternalsConfig(configPath: string): ExternalsConfig | null {
  if (!existsSync(configPath)) {
    return null;
  }

  try {
    const content = readFileSync(configPath, "utf-8");
    const parsed = yaml.load(content) as ExternalsConfig;

    if (!parsed?.externals || !Array.isArray(parsed.externals)) {
      console.warn("externals.yml exists but has no 'externals' array");
      return null;
    }

    return parsed;
  } catch (error) {
    console.warn(`Failed to parse externals.yml at ${configPath}:`, error);
    return null;
  }
}

/**
 * Find externals.yml in common locations
 */
export function findExternalsConfig(basePath: string): string | null {
  const candidates = [
    join(basePath, "externals.yml"),
    join(basePath, "externals.yaml"),
    join(basePath, ".cursor", "externals.yml"),
    join(basePath, ".cursor", "externals.yaml"),
    join(basePath, "config", "externals.yml"),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

/**
 * Parse external systems from configuration and create nodes/edges
 * 
 * @param basePath - Base path to look for externals.yml
 * @param existingNodes - Existing nodes to match consumer references against
 */
export function parseExternalSystems(
  basePath: string,
  existingNodes: GraphNode[],
  onProgress?: (percent: number, message: string) => void
): ExternalParseResult {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const citations: Citation[] = [];

  onProgress?.(10, "Looking for externals.yml...");

  const configPath = findExternalsConfig(basePath);
  if (!configPath) {
    onProgress?.(100, "No externals.yml found, skipping external config");
    return { nodes, edges, citations };
  }

  onProgress?.(20, `Parsing ${configPath}...`);
  const config = parseExternalsConfig(configPath);
  if (!config) {
    onProgress?.(100, "Failed to parse externals.yml");
    return { nodes, edges, citations };
  }

  onProgress?.(30, `Found ${config.externals.length} external system definitions`);

  // Build a lookup map for existing nodes by name
  const nodesByName = new Map<string, GraphNode>();
  for (const node of existingNodes) {
    // Index by short name (lowercase for case-insensitive matching)
    nodesByName.set(node.name.toLowerCase(), node);
    // Also try without common prefixes/patterns
    const cleanName = node.name.toLowerCase().replace(/^(stg_|int_|mart_|rpt_)/, "");
    nodesByName.set(cleanName, node);
  }

  let processedCount = 0;
  for (const external of config.externals) {
    // Create a unique ID for the external system
    const externalId = `external.${external.type}.${external.name.toLowerCase().replace(/\s+/g, "_")}`;

    const node: GraphNode = {
      id: externalId,
      name: external.name,
      type: "external",
      subtype: mapExternalTypeToSubtype(external.type),
      repo: "config",
      metadata: {
        description: external.description,
        tags: external.tags,
      },
    };
    nodes.push(node);

    // Add citation to the config file
    citations.push({
      id: uuid(),
      nodeId: externalId,
      filePath: configPath,
    });

    // Create edges from consumed models to this external system
    for (const consumedModel of external.consumes) {
      // Try to find the matching node
      const lowerName = consumedModel.toLowerCase();
      const sourceNode = nodesByName.get(lowerName);

      if (sourceNode) {
        edges.push({
          id: uuid(),
          from: sourceNode.id,
          to: externalId,
          type: "exposure",
          metadata: {
            transformationType: "external-consumer",
          },
        });
      } else {
        // Create a placeholder edge with the model name as the source
        // The cross-repo linker might resolve this later
        console.warn(`External consumer "${external.name}" references unknown model: ${consumedModel}`);
      }
    }

    processedCount++;
    const progress = 30 + Math.floor((processedCount / config.externals.length) * 60);
    onProgress?.(progress, `Processed ${external.name}`);
  }

  onProgress?.(100, `Created ${nodes.length} external nodes with ${edges.length} edges`);

  return { nodes, edges, citations };
}

/**
 * Known external systems that can be auto-detected from patterns
 * These are common platforms that consume data warehouse data
 */
export const KNOWN_EXTERNAL_SYSTEMS: Array<{
  name: string;
  type: "application" | "dashboard" | "reverse_etl";
  patterns: RegExp[];
  description: string;
}> = [
  {
    name: "Outreach.io Sequences",
    type: "application",
    patterns: [/outreach/i, /sequence.*outreach/i],
    description: "Sales engagement platform for automated email sequences",
  },
  {
    name: "Brevo Campaigns",
    type: "application",
    patterns: [/brevo/i, /sendinblue/i],
    description: "Email marketing and automation platform",
  },
  {
    name: "Hightouch",
    type: "reverse_etl",
    patterns: [/hightouch/i],
    description: "Reverse ETL platform for syncing data to business tools",
  },
  {
    name: "Census",
    type: "reverse_etl",
    patterns: [/census/i],
    description: "Reverse ETL platform for operational analytics",
  },
  {
    name: "Looker",
    type: "dashboard",
    patterns: [/looker/i],
    description: "Business intelligence and data visualization platform",
  },
  {
    name: "Tableau",
    type: "dashboard",
    patterns: [/tableau/i],
    description: "Data visualization and analytics platform",
  },
  {
    name: "Salesforce",
    type: "application",
    patterns: [/salesforce/i, /sfdc/i],
    description: "Customer relationship management platform",
  },
];
