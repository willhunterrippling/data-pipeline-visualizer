import { readFileSync, existsSync } from "fs";
import { join, relative } from "path";
import { v4 as uuid } from "uuid";
import type { GraphNode, GraphEdge, Citation, NodeType, NodeSubtype } from "../types";

// dbt manifest.json types (simplified)
interface DbtManifestNode {
  unique_id: string;
  name: string;
  resource_type: "model" | "seed" | "snapshot" | "test" | "source" | "analysis";
  schema: string;
  database: string;
  alias?: string;
  description?: string;
  columns?: Record<string, { name: string; description?: string; data_type?: string }>;
  config?: {
    materialized?: string;
    tags?: string[];
  };
  tags?: string[];
  original_file_path?: string;
  patch_path?: string;
  depends_on?: {
    nodes?: string[];
    macros?: string[];
  };
  refs?: Array<{ name: string; package?: string }>;
  sources?: Array<[string, string]>;
}

interface DbtManifestSource {
  unique_id: string;
  name: string;
  source_name: string;
  schema: string;
  database: string;
  description?: string;
  columns?: Record<string, { name: string; description?: string; data_type?: string }>;
  original_file_path?: string;
}

interface DbtManifest {
  nodes: Record<string, DbtManifestNode>;
  sources: Record<string, DbtManifestSource>;
  metadata: {
    project_name?: string;
    generated_at?: string;
  };
}

export interface DbtParseResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
  citations: Citation[];
}

function mapResourceTypeToNodeType(resourceType: string): NodeType {
  switch (resourceType) {
    case "model":
    case "snapshot":
      return "model";
    case "seed":
      return "seed";
    case "source":
      return "source";
    default:
      return "table";
  }
}

function mapResourceTypeToSubtype(resourceType: string): NodeSubtype {
  switch (resourceType) {
    case "model":
    case "snapshot":
      return "dbt_model";
    case "seed":
      return "dbt_seed";
    case "source":
      return "dbt_source";
    default:
      return "dbt_model";
  }
}

function buildFQN(database: string, schema: string, name: string): string {
  return `${database.toLowerCase()}.${schema.toLowerCase()}.${name.toLowerCase()}`;
}

export function parseDbtManifest(manifestPath: string, repoPath: string): DbtParseResult {
  if (!existsSync(manifestPath)) {
    throw new Error(`Manifest not found at ${manifestPath}`);
  }

  const manifest: DbtManifest = JSON.parse(readFileSync(manifestPath, "utf-8"));
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const citations: Citation[] = [];
  
  // Map unique_id to FQN for edge resolution
  const idToFqn = new Map<string, string>();

  // Parse sources first (they're referenced by models)
  for (const [sourceId, source] of Object.entries(manifest.sources || {})) {
    const fqn = buildFQN(source.database, source.schema, source.name);
    idToFqn.set(sourceId, fqn);

    const node: GraphNode = {
      id: fqn,
      name: source.name,
      type: "source",
      subtype: "dbt_source",
      repo: "rippling-dbt",
      metadata: {
        schema: source.schema,
        database: source.database,
        description: source.description,
        columns: source.columns
          ? Object.values(source.columns).map((c) => ({
              name: c.name,
              type: c.data_type || "unknown",
              description: c.description,
            }))
          : undefined,
        filePath: source.original_file_path,
      },
    };
    nodes.push(node);

    // Add citation
    if (source.original_file_path) {
      citations.push({
        id: uuid(),
        nodeId: fqn,
        filePath: join(repoPath, source.original_file_path),
      });
    }
  }

  // Parse models, seeds, snapshots
  for (const [nodeId, dbtNode] of Object.entries(manifest.nodes || {})) {
    // Skip tests and analyses
    if (dbtNode.resource_type === "test" || dbtNode.resource_type === "analysis") {
      continue;
    }

    const tableName = dbtNode.alias || dbtNode.name;
    const fqn = buildFQN(dbtNode.database, dbtNode.schema, tableName);
    idToFqn.set(nodeId, fqn);

    const node: GraphNode = {
      id: fqn,
      name: tableName,
      type: mapResourceTypeToNodeType(dbtNode.resource_type),
      subtype: mapResourceTypeToSubtype(dbtNode.resource_type),
      repo: "rippling-dbt",
      metadata: {
        schema: dbtNode.schema,
        database: dbtNode.database,
        description: dbtNode.description,
        materialization: dbtNode.config?.materialized,
        tags: [...(dbtNode.tags || []), ...(dbtNode.config?.tags || [])],
        columns: dbtNode.columns
          ? Object.values(dbtNode.columns).map((c) => ({
              name: c.name,
              type: c.data_type || "unknown",
              description: c.description,
            }))
          : undefined,
        filePath: dbtNode.original_file_path,
      },
    };
    nodes.push(node);

    // Add citation
    if (dbtNode.original_file_path) {
      citations.push({
        id: uuid(),
        nodeId: fqn,
        filePath: join(repoPath, dbtNode.original_file_path),
      });
    }
  }

  // Create edges from depends_on
  for (const [nodeId, dbtNode] of Object.entries(manifest.nodes || {})) {
    if (dbtNode.resource_type === "test" || dbtNode.resource_type === "analysis") {
      continue;
    }

    const toFqn = idToFqn.get(nodeId);
    if (!toFqn) continue;

    const dependsOn = dbtNode.depends_on?.nodes || [];
    for (const depId of dependsOn) {
      const fromFqn = idToFqn.get(depId);
      if (!fromFqn) continue;

      // Determine edge type based on dependency
      const edgeType = depId.startsWith("source.") ? "source" : "ref";

      edges.push({
        id: uuid(),
        from: fromFqn,
        to: toFqn,
        type: edgeType,
      });
    }
  }

  return { nodes, edges, citations };
}

// Helper to detect the manifest.json location
export function findManifestPath(dbtProjectPath: string): string | null {
  const candidates = [
    join(dbtProjectPath, "target", "manifest.json"),
    join(dbtProjectPath, "manifest.json"),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

// Get model SQL content for a node
export function getModelSql(repoPath: string, filePath: string): string | null {
  const fullPath = filePath.startsWith("/") ? filePath : join(repoPath, filePath);
  if (!existsSync(fullPath)) {
    return null;
  }
  return readFileSync(fullPath, "utf-8");
}

// Parse dbt_project.yml for additional metadata
export function parseDbtProject(projectPath: string): {
  name: string;
  version: string;
  modelPaths: string[];
  seedPaths: string[];
} | null {
  const projectFile = join(projectPath, "dbt_project.yml");
  if (!existsSync(projectFile)) {
    return null;
  }

  // Simple YAML parsing for the fields we need
  const content = readFileSync(projectFile, "utf-8");
  const lines = content.split("\n");
  
  let name = "unknown";
  let version = "unknown";
  const modelPaths: string[] = [];
  const seedPaths: string[] = [];

  for (const line of lines) {
    if (line.startsWith("name:")) {
      name = line.split(":")[1]?.trim().replace(/['"]/g, "") || name;
    }
    if (line.startsWith("version:")) {
      version = line.split(":")[1]?.trim().replace(/['"]/g, "") || version;
    }
  }

  return { name, version, modelPaths, seedPaths };
}

