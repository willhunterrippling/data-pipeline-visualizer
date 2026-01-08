/**
 * Export graph data from SQLite to JSON for static deployment.
 * 
 * Usage:
 *   npx tsx scripts/export-graph.ts [options]
 * 
 * Options:
 *   --with-explanations    Generate AI explanations for flow anchor/member nodes
 *   --max-members=N        Max member nodes per flow to explain (default: 50)
 *   --dry-run              Show what would be generated without making API calls
 * 
 * Environment variables:
 *   GENERATE_EXPLANATIONS=true  Same as --with-explanations
 *   DATABASE_PATH               Path to SQLite database (default: data/pipeline.db)
 *   RIPPLING_DBT_PATH           Path to dbt repo for SQL context
 *   OPENAI_API_KEY              Required for AI generation
 */

import Database from "better-sqlite3";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

// Parse command-line arguments
interface CliOptions {
  withExplanations: boolean;
  maxMembers: number;
  dryRun: boolean;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  
  return {
    withExplanations: args.includes("--with-explanations") || process.env.GENERATE_EXPLANATIONS === "true",
    maxMembers: parseInt(args.find(a => a.startsWith("--max-members="))?.split("=")[1] || "50", 10),
    dryRun: args.includes("--dry-run"),
  };
}

const options = parseArgs();

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

// Rate limiting helper
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Progress bar helper
function progressBar(current: number, total: number, width = 30): string {
  const percent = Math.round((current / total) * 100);
  const filled = Math.round((current / total) * width);
  const empty = width - filled;
  return `[${"█".repeat(filled)}${"░".repeat(empty)}] ${percent}% (${current}/${total})`;
}

/**
 * Generate explanations for all pre-defined flows.
 * This pre-caches explanations for anchor nodes and member->anchor relationships.
 */
async function generateFlowExplanations(
  db: Database.Database,
  flows: DbFlow[],
  nodes: DbNode[],
  edges: DbEdge[],
  existingExplanations: Set<string>,
  existingRelExplanations: Set<string>
): Promise<{ explanations: DbExplanation[]; relationalExplanations: DbRelationalExplanation[] }> {
  // Dynamically import AI modules (they require OpenAI API key)
  const { explainNode } = await import("../lib/ai/explain");
  const { explainRelationship, findPath } = await import("../lib/ai/relationalExplain");
  
  const nodeMap = new Map(nodes.map(n => [n.id, n]));
  const newExplanations: DbExplanation[] = [];
  const newRelExplanations: DbRelationalExplanation[] = [];
  
  // Filter to pre-defined flows only (not user-defined)
  const predefinedFlows = flows.filter(f => f.user_defined === 0);
  
  console.log(`\n🤖 Generating explanations for ${predefinedFlows.length} pre-defined flows...`);
  if (options.dryRun) {
    console.log("   (DRY RUN - no API calls will be made)\n");
  }
  
  let totalGenerated = 0;
  let totalSkipped = 0;
  
  for (const flow of predefinedFlows) {
    const anchorIds: string[] = flow.anchor_nodes ? JSON.parse(flow.anchor_nodes) : [];
    const memberIds: string[] = flow.member_nodes ? JSON.parse(flow.member_nodes) : [];
    
    if (anchorIds.length === 0) continue;
    
    const primaryAnchorId = anchorIds[0];
    const primaryAnchor = nodeMap.get(primaryAnchorId);
    
    if (!primaryAnchor) {
      console.log(`   ⚠️  Flow "${flow.name}": anchor node not found`);
      continue;
    }
    
    console.log(`\n📊 Flow: ${flow.name}`);
    console.log(`   Anchor: ${primaryAnchor.name}`);
    console.log(`   Members: ${memberIds.length} nodes`);
    
    // 1. Generate explanation for anchor nodes
    for (const anchorId of anchorIds) {
      const anchor = nodeMap.get(anchorId);
      if (!anchor) continue;
      
      if (existingExplanations.has(anchorId)) {
        console.log(`   ✓ Anchor "${anchor.name}" already explained`);
        totalSkipped++;
        continue;
      }
      
      if (options.dryRun) {
        console.log(`   [DRY RUN] Would generate explanation for anchor: ${anchor.name}`);
        continue;
      }
      
      console.log(`   → Generating explanation for anchor: ${anchor.name}...`);
      
      try {
        const graphNode = dbNodeToGraphNode(anchor);
        const explanation = await explainNode(graphNode, {
          repoPath: process.env.RIPPLING_DBT_PATH,
        });
        
        const dbExplanation: DbExplanation = {
          node_id: anchorId,
          summary: explanation,
          generated_at: new Date().toISOString(),
          model_used: process.env.OPENAI_MODEL || "gpt-4.1",
        };
        
        // Insert into database
        db.prepare(`
          INSERT OR REPLACE INTO explanations (node_id, summary, model_used)
          VALUES (?, ?, ?)
        `).run(dbExplanation.node_id, dbExplanation.summary, dbExplanation.model_used);
        
        newExplanations.push(dbExplanation);
        existingExplanations.add(anchorId);
        totalGenerated++;
        
        console.log(`     ✓ Done`);
        
        // Rate limiting
        await sleep(500);
      } catch (error) {
        console.error(`     ✗ Error: ${error instanceof Error ? error.message : "Unknown"}`);
      }
    }
    
    // 2. Generate relationship explanations for member nodes -> primary anchor
    // Limit to maxMembers most important nodes
    const membersToProcess = memberIds
      .filter(id => id !== primaryAnchorId) // Exclude anchor itself
      .slice(0, options.maxMembers);
    
    console.log(`   Processing ${membersToProcess.length} member relationships...`);
    
    let processedCount = 0;
    for (const memberId of membersToProcess) {
      const member = nodeMap.get(memberId);
      if (!member) continue;
      
      const cacheKey = `${memberId}:${primaryAnchorId}`;
      if (existingRelExplanations.has(cacheKey)) {
        totalSkipped++;
        processedCount++;
        continue;
      }
      
      if (options.dryRun) {
        processedCount++;
        if (processedCount % 10 === 0 || processedCount === membersToProcess.length) {
          console.log(`   [DRY RUN] ${progressBar(processedCount, membersToProcess.length)}`);
        }
        continue;
      }
      
      // Find path between member and anchor
      const pathInfo = findPath(
        memberId,
        primaryAnchorId,
        edges,
        (id) => nodeMap.get(id)
      );
      
      if (!pathInfo) {
        processedCount++;
        continue; // No path, skip
      }
      
      try {
        const memberNode = dbNodeToGraphNode(member);
        const anchorNode = dbNodeToGraphNode(primaryAnchor);
        
        const result = await explainRelationship(memberNode, anchorNode, pathInfo, {
          repoPath: process.env.RIPPLING_DBT_PATH,
        });
        
        const dbRelExplanation: DbRelationalExplanation = {
          node_id: memberId,
          anchor_id: primaryAnchorId,
          transformation_summary: result.transformationSummary,
          business_context: result.businessContext,
          full_explanation: result.fullExplanation,
          generated_at: new Date().toISOString(),
          model_used: process.env.OPENAI_MODEL || "gpt-4.1",
        };
        
        // Insert into database
        db.prepare(`
          INSERT OR REPLACE INTO relational_explanations 
          (node_id, anchor_id, transformation_summary, business_context, full_explanation, model_used)
          VALUES (?, ?, ?, ?, ?, ?)
        `).run(
          dbRelExplanation.node_id,
          dbRelExplanation.anchor_id,
          dbRelExplanation.transformation_summary,
          dbRelExplanation.business_context,
          dbRelExplanation.full_explanation,
          dbRelExplanation.model_used
        );
        
        newRelExplanations.push(dbRelExplanation);
        existingRelExplanations.add(cacheKey);
        totalGenerated++;
        
        // Rate limiting - slightly faster for relationship explanations
        await sleep(300);
      } catch (error) {
        // Log but continue
        if (processedCount < 3) {
          console.error(`     ✗ Error for ${member.name}: ${error instanceof Error ? error.message : "Unknown"}`);
        }
      }
      
      processedCount++;
      if (processedCount % 10 === 0 || processedCount === membersToProcess.length) {
        process.stdout.write(`\r   ${progressBar(processedCount, membersToProcess.length)}`);
      }
    }
    
    if (!options.dryRun && membersToProcess.length > 0) {
      console.log(); // New line after progress bar
    }
  }
  
  console.log(`\n✅ Explanation generation complete!`);
  console.log(`   Generated: ${totalGenerated}`);
  console.log(`   Skipped (cached): ${totalSkipped}`);
  
  return { explanations: newExplanations, relationalExplanations: newRelExplanations };
}

// Convert DbNode to GraphNode format for AI functions
interface GraphNode {
  id: string;
  name: string;
  type: string;
  subtype?: string;
  repo?: string;
  metadata?: Record<string, unknown>;
}

function dbNodeToGraphNode(dbNode: DbNode): GraphNode {
  return {
    id: dbNode.id,
    name: dbNode.name,
    type: dbNode.type,
    subtype: dbNode.subtype || undefined,
    repo: dbNode.repo || undefined,
    metadata: dbNode.metadata ? JSON.parse(dbNode.metadata) : undefined,
  };
}

async function main() {
  console.log("📦 Exporting graph data to JSON...");
  console.log(`   Source: ${DB_PATH}`);
  console.log(`   Output: ${OUTPUT_PATH}`);
  
  if (options.withExplanations) {
    console.log(`\n⚙️  Options:`);
    console.log(`   --with-explanations: enabled`);
    console.log(`   --max-members: ${options.maxMembers}`);
    console.log(`   --dry-run: ${options.dryRun}`);
  }

  // Open DB in read-write mode if generating explanations, read-only otherwise
  const db = new Database(DB_PATH, { readonly: !options.withExplanations });

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

  let explanations = db.prepare("SELECT * FROM explanations").all() as DbExplanation[];
  console.log(`   Explanations: ${explanations.length}`);

  let relationalExplanations = db.prepare("SELECT * FROM relational_explanations").all() as DbRelationalExplanation[];
  console.log(`   Relational Explanations: ${relationalExplanations.length}`);

  const layerNames = db.prepare("SELECT * FROM layer_names ORDER BY layer_number").all() as DbLayerName[];
  console.log(`   Layer Names: ${layerNames.length}`);

  const anchorCandidates = db.prepare("SELECT * FROM anchor_candidates ORDER BY importance_score DESC").all() as DbAnchorCandidate[];
  console.log(`   Anchor Candidates: ${anchorCandidates.length}`);

  // Generate explanations if requested
  if (options.withExplanations) {
    const existingExplanations = new Set(explanations.map(e => e.node_id));
    const existingRelExplanations = new Set(
      relationalExplanations.map(e => `${e.node_id}:${e.anchor_id}`)
    );
    
    const { explanations: newExpl, relationalExplanations: newRelExpl } = 
      await generateFlowExplanations(
        db,
        flows,
        nodes,
        edges,
        existingExplanations,
        existingRelExplanations
      );
    
    // Re-read explanations from DB to get all (existing + new)
    explanations = db.prepare("SELECT * FROM explanations").all() as DbExplanation[];
    relationalExplanations = db.prepare("SELECT * FROM relational_explanations").all() as DbRelationalExplanation[];
    
    console.log(`\n📊 Final explanation counts:`);
    console.log(`   Explanations: ${explanations.length} (+${newExpl.length} new)`);
    console.log(`   Relational Explanations: ${relationalExplanations.length} (+${newRelExpl.length} new)`);
  }

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

main().catch(error => {
  console.error("Export failed:", error);
  process.exit(1);
});
