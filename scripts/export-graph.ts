/**
 * Export graph data from SQLite to JSON for static deployment.
 * Run with: npx tsx scripts/export-graph.ts
 */

import Database from "better-sqlite3";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const DB_PATH = process.env.DATABASE_PATH || join(process.cwd(), "data/pipeline.db");
const OUTPUT_PATH = join(process.cwd(), "public/graph-data.json");

interface DbNode {
  id: string;
  name: string;
  type: string;
  subtype: string | null;
  group_id: string | null;
  repo: string | null;
  metadata: string | null;
  layout_x: number | null;
  layout_y: number | null;
  layout_layer: number | null;
  semantic_layer: string | null;
  importance_score: number | null;
}

interface DbEdge {
  id: string;
  from_node: string;
  to_node: string;
  type: string;
  metadata: string | null;
}

interface DbGroup {
  id: string;
  name: string;
  description: string | null;
  parent_id: string | null;
  inference_reason: string | null;
  node_count: number;
  collapsed_default: number;
}

interface DbFlow {
  id: string;
  name: string;
  description: string | null;
  anchor_nodes: string | null;
  member_nodes: string | null;
  user_defined: number;
  inference_reason: string | null;
}

interface DbExplanation {
  node_id: string;
  summary: string | null;
  generated_at: string;
  model_used: string | null;
}

interface DbRelationalExplanation {
  node_id: string;
  anchor_id: string;
  transformation_summary: string | null;
  business_context: string | null;
  full_explanation: string | null;
  generated_at: string;
  model_used: string | null;
}

interface DbLayerName {
  layer_number: number;
  name: string;
  description: string | null;
  node_count: number | null;
  sample_nodes: string | null;
  inference_reason: string | null;
}

interface DbAnchorCandidate {
  node_id: string;
  importance_score: number;
  upstream_count: number;
  downstream_count: number;
  total_connections: number;
  reason: string | null;
}

interface ExportedData {
  nodes: DbNode[];
  edges: DbEdge[];
  groups: DbGroup[];
  flows: DbFlow[];
  explanations: DbExplanation[];
  relationalExplanations: DbRelationalExplanation[];
  layerNames: DbLayerName[];
  anchorCandidates: DbAnchorCandidate[];
  // Pre-computed adjacency lists for efficient traversal
  adjacency: {
    upstream: Record<string, string[]>;   // nodeId -> [upstream nodeIds]
    downstream: Record<string, string[]>; // nodeId -> [downstream nodeIds]
  };
  exportedAt: string;
}

function main() {
  console.log("📦 Exporting graph data to JSON...");
  console.log(`   Source: ${DB_PATH}`);
  console.log(`   Output: ${OUTPUT_PATH}`);

  const db = new Database(DB_PATH, { readonly: true });

  // Read all data
  console.log("\n📖 Reading from database...");

  const nodes = db.prepare("SELECT * FROM nodes").all() as DbNode[];
  console.log(`   Nodes: ${nodes.length}`);

  const edges = db.prepare("SELECT * FROM edges").all() as DbEdge[];
  console.log(`   Edges: ${edges.length}`);

  const groups = db.prepare("SELECT * FROM groups").all() as DbGroup[];
  console.log(`   Groups: ${groups.length}`);

  const flows = db.prepare("SELECT * FROM flows").all() as DbFlow[];
  console.log(`   Flows: ${flows.length}`);

  const explanations = db.prepare("SELECT * FROM explanations").all() as DbExplanation[];
  console.log(`   Explanations: ${explanations.length}`);

  const relationalExplanations = db.prepare("SELECT * FROM relational_explanations").all() as DbRelationalExplanation[];
  console.log(`   Relational Explanations: ${relationalExplanations.length}`);

  const layerNames = db.prepare("SELECT * FROM layer_names ORDER BY layer_number").all() as DbLayerName[];
  console.log(`   Layer Names: ${layerNames.length}`);

  const anchorCandidates = db.prepare("SELECT * FROM anchor_candidates ORDER BY importance_score DESC").all() as DbAnchorCandidate[];
  console.log(`   Anchor Candidates: ${anchorCandidates.length}`);

  db.close();

  // Build adjacency lists for efficient lineage traversal
  console.log("\n🔗 Building adjacency lists...");
  const upstream: Record<string, string[]> = {};
  const downstream: Record<string, string[]> = {};

  for (const edge of edges) {
    // Downstream: from_node -> to_node
    if (!downstream[edge.from_node]) {
      downstream[edge.from_node] = [];
    }
    downstream[edge.from_node].push(edge.to_node);

    // Upstream: to_node <- from_node
    if (!upstream[edge.to_node]) {
      upstream[edge.to_node] = [];
    }
    upstream[edge.to_node].push(edge.from_node);
  }

  // Prepare export data
  const exportData: ExportedData = {
    nodes,
    edges,
    groups,
    flows,
    explanations,
    relationalExplanations,
    layerNames,
    anchorCandidates,
    adjacency: { upstream, downstream },
    exportedAt: new Date().toISOString(),
  };

  // Ensure public directory exists
  mkdirSync(join(process.cwd(), "public"), { recursive: true });

  // Write JSON
  console.log("\n💾 Writing JSON file...");
  const jsonString = JSON.stringify(exportData);
  writeFileSync(OUTPUT_PATH, jsonString);

  const fileSizeMB = (Buffer.byteLength(jsonString) / (1024 * 1024)).toFixed(2);
  console.log(`   File size: ${fileSizeMB} MB`);

  console.log("\n✅ Export complete!");
  console.log(`   Output: ${OUTPUT_PATH}`);
}

main();

