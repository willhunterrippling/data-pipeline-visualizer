import { completeJson } from "./client";
import type { GraphNode } from "../types";

// DB-ready group format
export interface DbGroupInput {
  id: string;
  name: string;
  description: string | null;
  parent_id: string | null;
  inference_reason: string | null;
  collapsed_default: number;
}

interface GroupingResult {
  groups: Array<{
    id: string;
    name: string;
    description: string;
    parentId?: string;
    inferenceReason: string;
    nodePatterns: string[]; // Patterns to match nodes to this group
  }>;
}

const GROUPING_PROMPT = `You are an expert data engineer analyzing a data pipeline graph.

Given a list of table/model nodes, infer logical groups based on:
1. Naming conventions (prefixes like stg_, int_, mart_, rpt_)
2. Domain patterns (sales, growth, finance, bookings in names)
3. Source systems (sfdc, mongo, outreach in names)
4. dbt tags if present in metadata

Output a JSON object with a "groups" array. Each group should have:
- id: unique lowercase snake_case identifier
- name: human-readable name
- description: brief description of what this group contains
- parentId: optional, for nested groups
- inferenceReason: explanation of why this group was created
- nodePatterns: array of regex patterns to match node names to this group

Example output:
{
  "groups": [
    {
      "id": "layer_staging",
      "name": "Staging",
      "description": "Raw source data with minimal transformations",
      "inferenceReason": "Models with stg_ prefix follow dbt staging convention",
      "nodePatterns": ["^stg_"]
    },
    {
      "id": "domain_sales",
      "name": "Sales",
      "description": "Sales pipeline and opportunity data",
      "parentId": "layer_marts",
      "inferenceReason": "Models contain 'sales' in name or have sales-related tags",
      "nodePatterns": ["sales", "opportunity", "sfdc"]
    }
  ]
}`;

/**
 * Use AI to infer groups from node list
 */
export async function inferGroups(nodes: GraphNode[]): Promise<DbGroupInput[]> {
  // Build node summary for AI
  const nodeSummary = nodes.slice(0, 500).map((n) => ({
    name: n.name,
    type: n.type,
    tags: n.metadata?.tags,
    schema: n.metadata?.schema,
  }));

  // Get tag frequency
  const tagCounts = new Map<string, number>();
  for (const node of nodes) {
    for (const tag of node.metadata?.tags || []) {
      tagCounts.set(tag, (tagCounts.get(tag) || 0) + 1);
    }
  }
  const topTags = [...tagCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([tag, count]) => `${tag} (${count})`);

  // Get prefix frequency
  const prefixCounts = new Map<string, number>();
  for (const node of nodes) {
    const prefix = node.name.match(/^([a-z]+_)/)?.[1];
    if (prefix) {
      prefixCounts.set(prefix, (prefixCounts.get(prefix) || 0) + 1);
    }
  }
  const topPrefixes = [...prefixCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([prefix, count]) => `${prefix} (${count})`);

  const userMessage = `Analyze these ${nodes.length} data pipeline nodes and create logical groups.

Top prefixes found: ${topPrefixes.join(", ")}
Top tags found: ${topTags.join(", ")}

Sample nodes (first 500):
${JSON.stringify(nodeSummary, null, 2)}

Create groups that would help users navigate this data pipeline. Consider both layer-based groups (staging, intermediate, marts) and domain-based groups.`;

  try {
    const result = await completeJson<GroupingResult>([
      { role: "system", content: GROUPING_PROMPT },
      { role: "user", content: userMessage },
    ]);

    // Convert to DB-ready format
    const groups: DbGroupInput[] = result.groups.map((g) => ({
      id: g.id,
      name: g.name,
      description: g.description || null,
      parent_id: g.parentId || null,
      inference_reason: g.inferenceReason || null,
      collapsed_default: 1,
    }));

    // Assign nodes to groups based on patterns
    for (const node of nodes) {
      for (const group of result.groups) {
        for (const pattern of group.nodePatterns) {
          try {
            const regex = new RegExp(pattern, "i");
            if (regex.test(node.name)) {
              node.groupId = group.id;
              break;
            }
          } catch {
            // Invalid regex, try string match
            if (node.name.toLowerCase().includes(pattern.toLowerCase())) {
              node.groupId = group.id;
              break;
            }
          }
        }
        if (node.groupId) break;
      }
    }

    return groups;
  } catch (error) {
    console.error("AI grouping failed, using fallback:", error);
    return fallbackGrouping(nodes);
  }
}

/**
 * Fallback rule-based grouping when AI fails
 */
function fallbackGrouping(nodes: GraphNode[]): DbGroupInput[] {
  const groups: DbGroupInput[] = [
    {
      id: "layer_staging",
      name: "Staging",
      description: "Raw source staging models",
      parent_id: null,
      inference_reason: "Inferred from stg_ prefix",
      collapsed_default: 1,
    },
    {
      id: "layer_intermediate",
      name: "Intermediate",
      description: "Business logic transformations",
      parent_id: null,
      inference_reason: "Inferred from int_ prefix",
      collapsed_default: 1,
    },
    {
      id: "layer_marts",
      name: "Marts",
      description: "Final business tables",
      parent_id: null,
      inference_reason: "Inferred from mart_ prefix",
      collapsed_default: 1,
    },
    {
      id: "layer_reports",
      name: "Reports",
      description: "Reporting views",
      parent_id: null,
      inference_reason: "Inferred from rpt_ prefix",
      collapsed_default: 1,
    },
    {
      id: "group_sources",
      name: "Data Sources",
      description: "External data sources and seeds",
      parent_id: null,
      inference_reason: "Contains source and seed nodes",
      collapsed_default: 1,
    },
  ];

  // Assign nodes
  for (const node of nodes) {
    if (node.type === "source" || node.type === "seed") {
      node.groupId = "group_sources";
    } else if (node.name.startsWith("stg_")) {
      node.groupId = "layer_staging";
    } else if (node.name.startsWith("int_")) {
      node.groupId = "layer_intermediate";
    } else if (node.name.startsWith("mart_")) {
      node.groupId = "layer_marts";
    } else if (node.name.startsWith("rpt_")) {
      node.groupId = "layer_reports";
    }
  }

  // Filter to groups that have nodes
  const usedGroupIds = new Set(nodes.map((n) => n.groupId).filter(Boolean));
  return groups.filter((g) => usedGroupIds.has(g.id));
}

