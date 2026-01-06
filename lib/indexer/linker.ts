import type { GraphNode, GraphEdge } from "../types";
import { v4 as uuid } from "uuid";

export interface LinkResult {
  mergedNodes: GraphNode[];
  additionalEdges: GraphEdge[];
  conflicts: Conflict[];
}

export interface Conflict {
  type: "duplicate_name" | "schema_mismatch" | "type_mismatch";
  nodeIds: string[];
  message: string;
}

/**
 * Link nodes across repos by matching FQNs and merging metadata
 */
export function linkCrossRepo(
  nodes: GraphNode[],
  edges: GraphEdge[]
): LinkResult {
  const conflicts: Conflict[] = [];
  const nodeById = new Map<string, GraphNode>();
  const nodesByName = new Map<string, GraphNode[]>();

  // First pass: group nodes by ID and name
  for (const node of nodes) {
    // By ID (FQN)
    const existing = nodeById.get(node.id);
    if (existing) {
      // Merge nodes with same FQN
      const merged = mergeNodes(existing, node);
      nodeById.set(node.id, merged);
    } else {
      nodeById.set(node.id, node);
    }

    // By short name (for conflict detection)
    const nameKey = node.name.toLowerCase();
    if (!nodesByName.has(nameKey)) {
      nodesByName.set(nameKey, []);
    }
    nodesByName.get(nameKey)!.push(node);
  }

  // Detect conflicts (same name, different FQNs)
  for (const [name, nodesWithName] of nodesByName) {
    if (nodesWithName.length > 1) {
      const uniqueFqns = new Set(nodesWithName.map((n) => n.id));
      if (uniqueFqns.size > 1) {
        conflicts.push({
          type: "duplicate_name",
          nodeIds: [...uniqueFqns],
          message: `Table "${name}" exists in multiple schemas: ${[...uniqueFqns].join(", ")}`,
        });
      }
    }
  }

  // Try to match Airflow tables to dbt models
  const additionalEdges: GraphEdge[] = [];
  const mergedNodes = [...nodeById.values()];

  // Build a lookup by short name for cross-repo matching
  const dbtNodesByName = new Map<string, GraphNode>();
  const airflowNodesByName = new Map<string, GraphNode>();

  for (const node of mergedNodes) {
    if (node.repo === "rippling-dbt") {
      dbtNodesByName.set(node.name.toLowerCase(), node);
    } else if (node.repo === "airflow-dags") {
      airflowNodesByName.set(node.name.toLowerCase(), node);
    }
  }

  // Create materialization edges where Airflow references dbt models
  for (const [name, airflowNode] of airflowNodesByName) {
    const dbtNode = dbtNodesByName.get(name);
    if (dbtNode && dbtNode.id !== airflowNode.id) {
      // Check if they could be the same table
      const airflowSchema = airflowNode.metadata?.schema?.toLowerCase();
      const dbtSchema = dbtNode.metadata?.schema?.toLowerCase();

      if (!airflowSchema || !dbtSchema || airflowSchema === dbtSchema) {
        // Create a materialization edge (dbt defines, Airflow might read)
        additionalEdges.push({
          id: uuid(),
          from: dbtNode.id,
          to: airflowNode.id,
          type: "materialization",
          metadata: {
            transformationType: "cross-repo-link",
          },
        });
      }
    }
  }

  return {
    mergedNodes,
    additionalEdges,
    conflicts,
  };
}

/**
 * Merge two nodes with the same FQN, preferring dbt metadata
 */
function mergeNodes(existing: GraphNode, incoming: GraphNode): GraphNode {
  // Prefer dbt as source of truth
  const primary = existing.repo === "rippling-dbt" ? existing : incoming;
  const secondary = existing.repo === "rippling-dbt" ? incoming : existing;

  return {
    ...primary,
    metadata: {
      ...secondary.metadata,
      ...primary.metadata,
      // Merge columns if both have them
      columns: primary.metadata?.columns || secondary.metadata?.columns,
      // Merge tags
      tags: [
        ...(primary.metadata?.tags || []),
        ...(secondary.metadata?.tags || []),
      ].filter((v, i, a) => a.indexOf(v) === i),
    },
  };
}

