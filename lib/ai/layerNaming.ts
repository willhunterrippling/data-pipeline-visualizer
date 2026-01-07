/**
 * AI Layer Naming
 * 
 * Uses AI to generate meaningful names for swimlane layers based on the nodes
 * in each topological layer. This replaces the domain-based grouping approach.
 */

import { completeJson } from "./client";
import type { GraphNode, SemanticLayer } from "../types";
import type { DbLayerName } from "../db";

interface LayerNamingResult {
  layers: Array<{
    layerNumber: number;
    name: string;
    description: string;
    inferenceReason: string;
  }>;
}

const LAYER_NAMING_PROMPT = `You are an expert data engineer analyzing a data pipeline graph.

Given information about nodes in each topological layer of a data pipeline, generate meaningful names for swimlane columns. The layers are ordered left-to-right in a visualization, where:
- Lower layer numbers = upstream (sources, raw data)
- Higher layer numbers = downstream (marts, reports)

Consider:
1. The semantic layer (source, staging, intermediate, mart, report, transform)
2. Common prefixes and naming patterns
3. The sample node names provided

Output a JSON object with a "layers" array. Each layer should have:
- layerNumber: the layer number being named
- name: short, descriptive name (2-4 words max, like "Lead Enrichment Sources" or "Core Metrics")
- description: one sentence explaining what's in this layer
- inferenceReason: brief explanation of why you chose this name

Be specific and domain-aware. Avoid generic names like "Layer 1" or just "Staging". 
Look for patterns like:
- "G2/Gartner Intent Data" instead of just "Sources"
- "Sales Pipeline Staging" instead of just "Staging"  
- "Growth Metrics" instead of just "Marts"

Example output:
{
  "layers": [
    {
      "layerNumber": 0,
      "name": "Raw CRM Sources",
      "description": "Raw data from Salesforce and HubSpot CRM systems",
      "inferenceReason": "Sample nodes include sfdc_* and hubspot_* prefixes"
    },
    {
      "layerNumber": 1,
      "name": "Lead Staging",
      "description": "Cleaned and standardized lead data from multiple sources",
      "inferenceReason": "Contains stg_lead_* and stg_contact_* models"
    }
  ]
}`;

export interface LayerNamingProgressCallback {
  (progress: number, message: string): void;
}

interface LayerInfo {
  layerNumber: number;
  nodeCount: number;
  sampleNodes: string[];
  semanticDistribution: Record<SemanticLayer, number>;
  commonPrefixes: string[];
}

/**
 * Gather statistics about each layer
 */
function gatherLayerStats(nodes: GraphNode[]): LayerInfo[] {
  const layerMap = new Map<number, {
    nodes: GraphNode[];
    semanticCounts: Map<SemanticLayer, number>;
    prefixCounts: Map<string, number>;
  }>();

  // Group nodes by layer
  for (const node of nodes) {
    const layer = node.layoutLayer ?? 0;
    
    if (!layerMap.has(layer)) {
      layerMap.set(layer, {
        nodes: [],
        semanticCounts: new Map(),
        prefixCounts: new Map(),
      });
    }

    const layerData = layerMap.get(layer)!;
    layerData.nodes.push(node);

    // Count semantic layers
    const semantic = node.semanticLayer || "transform";
    layerData.semanticCounts.set(
      semantic as SemanticLayer,
      (layerData.semanticCounts.get(semantic as SemanticLayer) || 0) + 1
    );

    // Count prefixes
    const prefix = node.name.match(/^([a-z]+_)/)?.[1] || "";
    if (prefix) {
      layerData.prefixCounts.set(prefix, (layerData.prefixCounts.get(prefix) || 0) + 1);
    }
  }

  // Convert to LayerInfo array
  const layers: LayerInfo[] = [];
  for (const [layerNumber, data] of layerMap) {
    const semanticDistribution: Record<SemanticLayer, number> = {
      source: 0,
      staging: 0,
      intermediate: 0,
      mart: 0,
      report: 0,
      transform: 0,
      external: 0,
    };
    for (const [semantic, count] of data.semanticCounts) {
      semanticDistribution[semantic] = count;
    }

    // Get top prefixes
    const topPrefixes = [...data.prefixCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([prefix]) => prefix);

    // Get sample nodes (diverse selection)
    const sampleNodes = data.nodes
      .slice(0, 10)
      .map((n) => n.name);

    layers.push({
      layerNumber,
      nodeCount: data.nodes.length,
      sampleNodes,
      semanticDistribution,
      commonPrefixes: topPrefixes,
    });
  }

  return layers.sort((a, b) => a.layerNumber - b.layerNumber);
}

/**
 * Use AI to generate meaningful layer names
 */
export async function generateLayerNames(
  nodes: GraphNode[],
  onProgress?: LayerNamingProgressCallback
): Promise<DbLayerName[]> {
  onProgress?.(0, `Analyzing ${nodes.length} nodes across layers...`);

  const layerStats = gatherLayerStats(nodes);
  onProgress?.(10, `Found ${layerStats.length} layers to name`);

  if (layerStats.length === 0) {
    return [];
  }

  // Build context for AI
  const layerContext = layerStats.map((layer) => ({
    layerNumber: layer.layerNumber,
    nodeCount: layer.nodeCount,
    sampleNodes: layer.sampleNodes.slice(0, 8),
    topPrefixes: layer.commonPrefixes.slice(0, 4),
    semanticBreakdown: Object.entries(layer.semanticDistribution)
      .filter(([, count]) => count > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([semantic, count]) => `${semantic}: ${count}`),
  }));

  onProgress?.(20, "Sending layer info to AI for naming...");

  try {
    const userMessage = `Generate meaningful names for these ${layerStats.length} layers in a data pipeline visualization:

${JSON.stringify(layerContext, null, 2)}

Create names that are specific and descriptive, based on the actual node names and patterns you see. Avoid generic names.`;

    const result = await completeJson<LayerNamingResult>([
      { role: "system", content: LAYER_NAMING_PROMPT },
      { role: "user", content: userMessage },
    ]);

    onProgress?.(80, `Generated ${result.layers.length} layer names`);

    // Convert to DB format
    const dbLayerNames: DbLayerName[] = result.layers.map((layer) => {
      const stats = layerStats.find((s) => s.layerNumber === layer.layerNumber);
      return {
        layer_number: layer.layerNumber,
        name: layer.name,
        description: layer.description,
        node_count: stats?.nodeCount || 0,
        sample_nodes: JSON.stringify(stats?.sampleNodes || []),
        inference_reason: layer.inferenceReason,
      };
    });

    // Add any layers the AI missed (fallback names)
    const namedLayers = new Set(dbLayerNames.map((l) => l.layer_number));
    for (const stats of layerStats) {
      if (!namedLayers.has(stats.layerNumber)) {
        const dominantSemantic = Object.entries(stats.semanticDistribution)
          .sort((a, b) => b[1] - a[1])[0]?.[0] || "transform";

        dbLayerNames.push({
          layer_number: stats.layerNumber,
          name: `${dominantSemantic.charAt(0).toUpperCase() + dominantSemantic.slice(1)} Layer ${stats.layerNumber}`,
          description: `Contains ${stats.nodeCount} ${dominantSemantic} nodes`,
          node_count: stats.nodeCount,
          sample_nodes: JSON.stringify(stats.sampleNodes),
          inference_reason: `Fallback name based on dominant semantic layer: ${dominantSemantic}`,
        });
      }
    }

    onProgress?.(100, `Completed naming ${dbLayerNames.length} layers`);

    return dbLayerNames.sort((a, b) => a.layer_number - b.layer_number);
  } catch (error) {
    console.error("AI layer naming failed, using fallback:", error);
    onProgress?.(50, "AI request failed, using fallback layer names...");
    return fallbackLayerNames(layerStats);
  }
}

/**
 * Fallback layer naming when AI fails
 */
function fallbackLayerNames(layerStats: LayerInfo[]): DbLayerName[] {
  return layerStats.map((stats) => {
    // Find dominant semantic layer
    const dominant = Object.entries(stats.semanticDistribution)
      .sort((a, b) => b[1] - a[1])[0];

    const semanticName = dominant?.[0] || "transform";
    const capitalizedName = semanticName.charAt(0).toUpperCase() + semanticName.slice(1);

    // Try to add more specificity from prefixes
    let name = capitalizedName;
    if (stats.commonPrefixes.length > 0) {
      const prefix = stats.commonPrefixes[0].replace(/_$/, "");
      name = `${capitalizedName} (${prefix})`;
    }

    return {
      layer_number: stats.layerNumber,
      name,
      description: `Contains ${stats.nodeCount} nodes, primarily ${semanticName} layer`,
      node_count: stats.nodeCount,
      sample_nodes: JSON.stringify(stats.sampleNodes),
      inference_reason: `Rule-based: dominant semantic layer is ${semanticName}`,
    };
  });
}

