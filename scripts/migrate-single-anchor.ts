/**
 * Migration script to convert flows from multi-anchor to single-anchor format.
 * 
 * This script:
 * 1. Reads all flows from the database
 * 2. Keeps only the first anchor node for each flow
 * 3. Optionally recalculates member nodes based on the single anchor
 * 
 * Usage:
 *   npx ts-node scripts/migrate-single-anchor.ts [--recalculate]
 * 
 * Options:
 *   --recalculate  Recalculate member nodes based on the single anchor (slower but ensures consistency)
 */

import { getDb, getFlows, insertFlow, getNodeById } from "../lib/db";

// Reuse the buildFlowMembers logic
function buildFlowMembers(anchorNodeId: string, depth: number = 6): string[] {
  const db = getDb();
  const edges = db.prepare("SELECT from_node, to_node FROM edges").all() as Array<{
    from_node: string;
    to_node: string;
  }>;

  // Build upstream map
  const upstreamMap = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!upstreamMap.has(edge.to_node)) {
      upstreamMap.set(edge.to_node, new Set());
    }
    upstreamMap.get(edge.to_node)!.add(edge.from_node);
  }

  // Traverse upstream from anchor
  const memberSet = new Set<string>();
  
  function getUpstream(nodeId: string, remainingDepth: number, visited: Set<string>): void {
    if (remainingDepth === 0 || visited.has(nodeId)) return;
    visited.add(nodeId);
    memberSet.add(nodeId);

    const upstream = upstreamMap.get(nodeId) || new Set();
    for (const upId of upstream) {
      getUpstream(upId, remainingDepth - 1, visited);
    }
  }

  memberSet.add(anchorNodeId);
  getUpstream(anchorNodeId, depth, new Set());

  return [...memberSet];
}

async function migrate(recalculate: boolean) {
  console.log("🔄 Starting migration to single-anchor format...\n");
  
  const flows = getFlows();
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const flow of flows) {
    const anchors: string[] = flow.anchor_nodes ? JSON.parse(flow.anchor_nodes) : [];
    
    if (anchors.length <= 1) {
      console.log(`⏭️  ${flow.name}: Already has ${anchors.length} anchor(s), skipping`);
      skipped++;
      continue;
    }

    const primaryAnchor = anchors[0];
    const anchorNode = getNodeById(primaryAnchor);
    
    if (!anchorNode) {
      console.log(`❌ ${flow.name}: Primary anchor ${primaryAnchor} not found, skipping`);
      errors++;
      continue;
    }

    let memberNodes: string[];
    if (recalculate) {
      console.log(`🔄 ${flow.name}: Recalculating members from anchor "${anchorNode.name}"...`);
      memberNodes = buildFlowMembers(primaryAnchor);
    } else {
      memberNodes = flow.member_nodes ? JSON.parse(flow.member_nodes) : [];
    }

    // Update the flow
    insertFlow({
      id: flow.id,
      name: flow.name,
      description: flow.description,
      anchor_nodes: JSON.stringify([primaryAnchor]),
      member_nodes: JSON.stringify(memberNodes),
      user_defined: flow.user_defined,
      inference_reason: flow.inference_reason,
    });

    console.log(`✅ ${flow.name}: Reduced from ${anchors.length} anchors to 1 (${anchorNode.name})${recalculate ? `, ${memberNodes.length} members` : ""}`);
    updated++;
  }

  console.log("\n📊 Migration Summary:");
  console.log(`   Updated: ${updated}`);
  console.log(`   Skipped: ${skipped}`);
  console.log(`   Errors:  ${errors}`);
  console.log(`   Total:   ${flows.length}`);
}

// Parse args
const args = process.argv.slice(2);
const recalculate = args.includes("--recalculate");

if (recalculate) {
  console.log("📝 Running with --recalculate: Member nodes will be rebuilt\n");
} else {
  console.log("📝 Running without --recalculate: Only anchor arrays will be trimmed\n");
  console.log("   (Add --recalculate to also rebuild member nodes)\n");
}

migrate(recalculate).catch(console.error);
