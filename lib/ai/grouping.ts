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

export interface GroupingProgressCallback {
  (progress: number, message: string): void;
}

// Chunking configuration
const CHUNK_SIZE = 100; // Nodes per chunk
const MAX_CHUNKS = 5;   // Limit API calls

/**
 * Split nodes into chunks for parallel processing.
 * If there are more than MAX_CHUNKS * CHUNK_SIZE nodes, sample evenly.
 */
function chunkNodes(nodes: GraphNode[]): GraphNode[][] {
  const maxNodes = MAX_CHUNKS * CHUNK_SIZE;
  
  // If we have more nodes than we can process, sample evenly
  let nodesToProcess: GraphNode[];
  if (nodes.length > maxNodes) {
    const step = Math.ceil(nodes.length / maxNodes);
    nodesToProcess = nodes.filter((_, i) => i % step === 0).slice(0, maxNodes);
  } else {
    nodesToProcess = nodes;
  }
  
  // Split into chunks
  const chunks: GraphNode[][] = [];
  for (let i = 0; i < nodesToProcess.length; i += CHUNK_SIZE) {
    chunks.push(nodesToProcess.slice(i, i + CHUNK_SIZE));
  }
  
  return chunks;
}

interface ChunkGroupResult {
  id: string;
  name: string;
  description: string;
  parentId?: string;
  inferenceReason: string;
  nodePatterns: string[];
}

/**
 * Get groups from AI for a single chunk of nodes
 */
async function getGroupsForChunk(
  chunkNodes: GraphNode[],
  topPrefixes: string[],
  topTags: string[],
  totalNodes: number
): Promise<ChunkGroupResult[]> {
  const nodeNames = chunkNodes.map((n) => n.name);
  
  const userMessage = `Analyze these ${chunkNodes.length} data pipeline nodes (from a total of ${totalNodes}) and create logical groups.

Top prefixes found across all nodes: ${topPrefixes.join(", ")}
Top tags found across all nodes: ${topTags.join(", ")}

Node names in this batch:
${JSON.stringify(nodeNames)}

Create groups that would help users navigate this data pipeline. Consider both layer-based groups (staging, intermediate, marts) and domain-based groups.`;

  const result = await completeJson<GroupingResult>([
    { role: "system", content: GROUPING_PROMPT },
    { role: "user", content: userMessage },
  ]);

  return result.groups;
}

/**
 * Merge groups from multiple chunks, deduplicating by ID and combining patterns
 */
function mergeChunkGroups(allChunkGroups: ChunkGroupResult[][]): ChunkGroupResult[] {
  const groupMap = new Map<string, ChunkGroupResult>();
  
  for (const chunkGroups of allChunkGroups) {
    for (const group of chunkGroups) {
      const normalizedId = group.id.toLowerCase().replace(/[^a-z0-9_]/g, '_');
      
      if (groupMap.has(normalizedId)) {
        // Merge patterns from duplicate group
        const existing = groupMap.get(normalizedId)!;
        const existingPatterns = new Set(existing.nodePatterns);
        for (const pattern of group.nodePatterns) {
          existingPatterns.add(pattern);
        }
        existing.nodePatterns = [...existingPatterns];
      } else {
        // Add new group with normalized ID
        groupMap.set(normalizedId, {
          ...group,
          id: normalizedId,
        });
      }
    }
  }
  
  return [...groupMap.values()];
}

/**
 * Use AI to infer groups from node list using chunked parallel requests
 */
export async function inferGroups(
  nodes: GraphNode[],
  onProgress?: GroupingProgressCallback
): Promise<DbGroupInput[]> {
  onProgress?.(0, `Analyzing ${nodes.length} nodes for grouping patterns...`);

  onProgress?.(5, "Extracting naming patterns and tags...");

  // Get tag frequency (across all nodes)
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

  // Get prefix frequency (across all nodes)
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

  onProgress?.(10, `Found prefixes: ${topPrefixes.slice(0, 4).map(p => p.split(' ')[0]).join(', ')}...`);

  // Split nodes into chunks for parallel processing
  const chunks = chunkNodes(nodes);
  onProgress?.(15, `Processing ${nodes.length} nodes in ${chunks.length} chunk(s)...`);

  const startTime = Date.now();

  try {
    onProgress?.(20, `Sending ${chunks.length} parallel AI request(s)...`);
    
    // Process all chunks in parallel
    const chunkResults = await Promise.all(
      chunks.map((chunk) => getGroupsForChunk(chunk, topPrefixes, topTags, nodes.length))
    );

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const totalGroupsBeforeMerge = chunkResults.reduce((sum, r) => sum + r.length, 0);
    onProgress?.(50, `AI responses received in ${elapsed}s, merging ${totalGroupsBeforeMerge} groups...`);

    // Merge groups from all chunks
    const mergedGroups = mergeChunkGroups(chunkResults);

    onProgress?.(60, `Merged into ${mergedGroups.length} unique groups`);

    // Convert to DB-ready format
    const groups: DbGroupInput[] = mergedGroups.map((g) => ({
      id: g.id,
      name: g.name,
      description: g.description || null,
      parent_id: g.parentId || null,
      inference_reason: g.inferenceReason || null,
      collapsed_default: 1,
    }));

    onProgress?.(70, `Assigning ${nodes.length} nodes to groups...`);

    // Assign nodes to groups based on patterns
    let assigned = 0;
    for (const node of nodes) {
      for (const group of mergedGroups) {
        for (const pattern of group.nodePatterns) {
          try {
            const regex = new RegExp(pattern, "i");
            if (regex.test(node.name)) {
              node.groupId = group.id;
              assigned++;
              break;
            }
          } catch {
            // Invalid regex, try string match
            if (node.name.toLowerCase().includes(pattern.toLowerCase())) {
              node.groupId = group.id;
              assigned++;
              break;
            }
          }
        }
        if (node.groupId) break;
      }
    }

    onProgress?.(90, `Assigned ${assigned} nodes to ${groups.length} groups`);

    return groups;
  } catch (error) {
    console.error("AI grouping failed, using fallback:", error);
    onProgress?.(50, "AI request failed, using rule-based grouping...");
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

