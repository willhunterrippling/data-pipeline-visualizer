import { readFileSync, existsSync, readdirSync, statSync } from "fs";
import { join, relative, basename, dirname } from "path";
import { v4 as uuid } from "uuid";
import * as yaml from "js-yaml";
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

// dbt exposure types - for documenting downstream consumers
interface DbtExposure {
  unique_id: string;
  name: string;
  type: "dashboard" | "notebook" | "analysis" | "ml" | "application";
  owner?: {
    name?: string;
    email?: string;
  };
  depends_on?: {
    nodes?: string[];
    macros?: string[];
  };
  url?: string;
  description?: string;
  maturity?: "high" | "medium" | "low";
  original_file_path?: string;
}

interface DbtManifest {
  nodes: Record<string, DbtManifestNode>;
  sources: Record<string, DbtManifestSource>;
  exposures?: Record<string, DbtExposure>;
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

  // Parse exposures (downstream consumers like dashboards, apps, etc.)
  for (const [exposureId, exposure] of Object.entries(manifest.exposures || {})) {
    // Create a unique ID for the exposure (external system)
    const exposureFqn = `external.${exposure.type}.${exposure.name.toLowerCase().replace(/\s+/g, "_")}`;
    
    // Map exposure type to subtype
    const subtypeMap: Record<string, NodeSubtype> = {
      dashboard: "dashboard",
      notebook: "notebook",
      analysis: "analysis",
      ml: "ml_model",
      application: "application",
    };

    const node: GraphNode = {
      id: exposureFqn,
      name: exposure.name,
      type: "external",
      subtype: subtypeMap[exposure.type] || "external_feed",
      repo: "rippling-dbt",
      metadata: {
        description: exposure.description,
        filePath: exposure.original_file_path,
        tags: exposure.maturity ? [exposure.maturity] : undefined,
      },
    };
    nodes.push(node);

    // Add citation if we have a file path
    if (exposure.original_file_path) {
      citations.push({
        id: uuid(),
        nodeId: exposureFqn,
        filePath: join(repoPath, exposure.original_file_path),
      });
    }

    // Create edges FROM dependent models TO this exposure
    const dependsOn = exposure.depends_on?.nodes || [];
    for (const depId of dependsOn) {
      const fromFqn = idToFqn.get(depId);
      if (!fromFqn) continue;

      edges.push({
        id: uuid(),
        from: fromFqn,
        to: exposureFqn,
        type: "exposure", // Models flow into exposures (downstream consumers)
      });
    }
  }

  return { nodes, edges, citations };
}

// Helper to detect the manifest.json location
export function findManifestPath(dbtProjectPath: string): string | null {
  const candidates = [
    join(dbtProjectPath, "target", "manifest.json"),
    join(dbtProjectPath, "main_artifacts", "manifest.json"),
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

// ============================================================================
// FALLBACK PARSER - Parse dbt project from SQL files without manifest.json
// ============================================================================

interface DbtProjectConfig {
  name: string;
  version: string;
  modelPaths: string[];
  seedPaths: string[];
  snapshotPaths: string[];
  targetSchema: string;
  targetDatabase: string;
}

interface SourceDefinition {
  sourceName: string;
  database: string;
  schema: string;
  tables: Array<{
    name: string;
    description?: string;
    columns?: Array<{ name: string; description?: string; data_type?: string }>;
  }>;
  filePath: string;
}

interface DbtReference {
  type: "ref" | "source";
  modelName?: string;
  sourceName?: string;
  tableName?: string;
}

interface DbtModelConfig {
  materialized?: string;
  tags?: string[];
  schema?: string;
  alias?: string;
}

/**
 * Parse dbt_project.yml using proper YAML parsing
 */
export function parseDbtProjectYml(projectPath: string): DbtProjectConfig | null {
  const projectFile = join(projectPath, "dbt_project.yml");
  if (!existsSync(projectFile)) {
    return null;
  }

  try {
    const content = readFileSync(projectFile, "utf-8");
    const parsed = yaml.load(content) as Record<string, unknown>;

    // Extract model-paths (dbt 1.0+) or source-paths (legacy)
    let modelPaths = (parsed["model-paths"] || parsed["source-paths"] || ["models"]) as string[];
    if (!Array.isArray(modelPaths)) modelPaths = ["models"];

    // Extract seed-paths
    let seedPaths = (parsed["seed-paths"] || parsed["data-paths"] || ["seeds"]) as string[];
    if (!Array.isArray(seedPaths)) seedPaths = ["seeds"];

    // Extract snapshot-paths
    let snapshotPaths = (parsed["snapshot-paths"] || ["snapshots"]) as string[];
    if (!Array.isArray(snapshotPaths)) snapshotPaths = ["snapshots"];

    return {
      name: (parsed["name"] as string) || "unknown",
      version: String(parsed["version"] || "unknown"),
      modelPaths,
      seedPaths,
      snapshotPaths,
      targetSchema: (parsed["target-schema"] as string) || "public",
      targetDatabase: (parsed["target-database"] as string) || "analytics",
    };
  } catch (error) {
    console.warn("Failed to parse dbt_project.yml:", error);
    return null;
  }
}

/**
 * Recursively discover all .sql files in a directory
 */
export function discoverSqlFiles(basePath: string, modelPaths: string[]): string[] {
  const sqlFiles: string[] = [];

  function walkDir(dir: string): void {
    if (!existsSync(dir)) return;

    try {
      const entries = readdirSync(dir);
      for (const entry of entries) {
        const fullPath = join(dir, entry);
        const stat = statSync(fullPath);

        if (stat.isDirectory()) {
          walkDir(fullPath);
        } else if (entry.endsWith(".sql")) {
          sqlFiles.push(fullPath);
        }
      }
    } catch (error) {
      console.warn(`Failed to read directory ${dir}:`, error);
    }
  }

  for (const modelPath of modelPaths) {
    const fullPath = join(basePath, modelPath);
    walkDir(fullPath);
  }

  return sqlFiles;
}

/**
 * Discover and parse all sources.yml / _sources.yml files
 */
export function discoverSourcesYaml(basePath: string, modelPaths: string[]): SourceDefinition[] {
  const sources: SourceDefinition[] = [];
  const yamlFiles: string[] = [];

  function walkDir(dir: string): void {
    if (!existsSync(dir)) return;

    try {
      const entries = readdirSync(dir);
      for (const entry of entries) {
        const fullPath = join(dir, entry);
        const stat = statSync(fullPath);

        if (stat.isDirectory()) {
          walkDir(fullPath);
        } else if (
          entry === "sources.yml" ||
          entry === "_sources.yml" ||
          entry.endsWith("_sources.yml")
        ) {
          yamlFiles.push(fullPath);
        }
      }
    } catch (error) {
      console.warn(`Failed to read directory ${dir}:`, error);
    }
  }

  for (const modelPath of modelPaths) {
    walkDir(join(basePath, modelPath));
  }

  for (const yamlFile of yamlFiles) {
    const parsed = parseSourcesYaml(yamlFile);
    sources.push(...parsed);
  }

  return sources;
}

/**
 * Parse a single sources.yml file
 */
export function parseSourcesYaml(yamlPath: string): SourceDefinition[] {
  const sources: SourceDefinition[] = [];

  try {
    const content = readFileSync(yamlPath, "utf-8");
    const parsed = yaml.load(content) as { sources?: unknown[] };

    if (!parsed?.sources || !Array.isArray(parsed.sources)) {
      return sources;
    }

    for (const source of parsed.sources) {
      const s = source as Record<string, unknown>;
      const sourceName = (s.name as string) || "";
      const database = (s.database as string) || "";
      const schema = (s.schema as string) || sourceName;

      const tables: SourceDefinition["tables"] = [];
      if (Array.isArray(s.tables)) {
        for (const table of s.tables) {
          const t = table as Record<string, unknown>;
          const columns: Array<{ name: string; description?: string; data_type?: string }> = [];

          if (Array.isArray(t.columns)) {
            for (const col of t.columns) {
              const c = col as Record<string, unknown>;
              columns.push({
                name: (c.name as string) || "",
                description: c.description as string | undefined,
                data_type: c.data_type as string | undefined,
              });
            }
          }

          tables.push({
            name: (t.name as string) || "",
            description: t.description as string | undefined,
            columns: columns.length > 0 ? columns : undefined,
          });
        }
      }

      sources.push({
        sourceName,
        database,
        schema,
        tables,
        filePath: yamlPath,
      });
    }
  } catch (error) {
    console.warn(`Failed to parse sources.yml at ${yamlPath}:`, error);
  }

  return sources;
}

/**
 * Extract ref() and source() calls from SQL content
 */
export function extractDbtReferences(sql: string): DbtReference[] {
  const references: DbtReference[] = [];

  // Match {{ ref('model_name') }} or {{ ref("model_name") }}
  // Also handles whitespace variations
  const refPattern = /\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g;
  let match;
  while ((match = refPattern.exec(sql)) !== null) {
    references.push({
      type: "ref",
      modelName: match[1],
    });
  }

  // Match {{ source('source_name', 'table_name') }}
  const sourcePattern = /\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}/g;
  while ((match = sourcePattern.exec(sql)) !== null) {
    references.push({
      type: "source",
      sourceName: match[1],
      tableName: match[2],
    });
  }

  return references;
}

/**
 * Extract config() block from SQL content
 */
export function extractDbtConfig(sql: string): DbtModelConfig {
  const config: DbtModelConfig = {};

  // Match {{ config(...) }} - handle multi-line
  const configPattern = /\{\{\s*config\s*\(([\s\S]*?)\)\s*\}\}/;
  const match = configPattern.exec(sql);

  if (!match) return config;

  const configContent = match[1];

  // Extract materialized
  const materializedMatch = /materialized\s*=\s*['"]([^'"]+)['"]/i.exec(configContent);
  if (materializedMatch) {
    config.materialized = materializedMatch[1];
  }

  // Extract tags (can be a list)
  const tagsMatch = /tags\s*=\s*\[([^\]]*)\]/i.exec(configContent);
  if (tagsMatch) {
    const tagsContent = tagsMatch[1];
    const tagPattern = /['"]([^'"]+)['"]/g;
    const tags: string[] = [];
    let tagMatch;
    while ((tagMatch = tagPattern.exec(tagsContent)) !== null) {
      tags.push(tagMatch[1]);
    }
    if (tags.length > 0) {
      config.tags = tags;
    }
  }

  // Single tag
  const singleTagMatch = /tags\s*=\s*['"]([^'"]+)['"]/i.exec(configContent);
  if (singleTagMatch && !config.tags) {
    config.tags = [singleTagMatch[1]];
  }

  // Extract schema
  const schemaMatch = /schema\s*=\s*['"]([^'"]+)['"]/i.exec(configContent);
  if (schemaMatch) {
    config.schema = schemaMatch[1];
  }

  // Extract alias
  const aliasMatch = /alias\s*=\s*['"]([^'"]+)['"]/i.exec(configContent);
  if (aliasMatch) {
    config.alias = aliasMatch[1];
  }

  return config;
}

/**
 * Infer resource type from file path
 */
function inferResourceType(filePath: string, projectPath: string): "model" | "seed" | "snapshot" {
  const relativePath = relative(projectPath, filePath).toLowerCase();

  if (relativePath.includes("snapshot")) {
    return "snapshot";
  }
  if (relativePath.includes("seed")) {
    return "seed";
  }
  return "model";
}

/**
 * Main fallback parser - parse dbt project from SQL files without manifest.json
 */
export function parseDbtProjectFallback(projectPath: string): DbtParseResult {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const citations: Citation[] = [];

  // Parse project configuration
  const projectConfig = parseDbtProjectYml(projectPath);
  if (!projectConfig) {
    throw new Error(`No dbt_project.yml found at ${projectPath}`);
  }

  const { modelPaths, seedPaths, snapshotPaths, targetDatabase, targetSchema } = projectConfig;

  // Combine all paths for discovery
  const allPaths = [...modelPaths, ...seedPaths, ...snapshotPaths];

  // Discover source definitions first
  const sourceDefs = discoverSourcesYaml(projectPath, modelPaths);

  // Create source nodes
  const sourceNodeMap = new Map<string, string>(); // "source_name.table_name" -> node id
  for (const sourceDef of sourceDefs) {
    for (const table of sourceDef.tables) {
      const sourceKey = `${sourceDef.sourceName}.${table.name}`;
      const fqn = buildFQN(
        sourceDef.database || targetDatabase,
        sourceDef.schema,
        table.name
      );
      sourceNodeMap.set(sourceKey, fqn);

      const node: GraphNode = {
        id: fqn,
        name: table.name,
        type: "source",
        subtype: "dbt_source",
        repo: "rippling-dbt",
        metadata: {
          schema: sourceDef.schema,
          database: sourceDef.database || targetDatabase,
          description: table.description,
          columns: table.columns?.map((c) => ({
            name: c.name,
            type: c.data_type || "unknown",
            description: c.description,
          })),
          filePath: relative(projectPath, sourceDef.filePath),
        },
      };
      nodes.push(node);

      citations.push({
        id: uuid(),
        nodeId: fqn,
        filePath: sourceDef.filePath,
      });
    }
  }

  // Discover all SQL files
  const sqlFiles = discoverSqlFiles(projectPath, allPaths);

  // First pass: create all model nodes
  const modelNodeMap = new Map<string, string>(); // model name -> node id
  const modelFileMap = new Map<string, { filePath: string; sql: string; config: DbtModelConfig }>();

  for (const sqlFile of sqlFiles) {
    const modelName = basename(sqlFile, ".sql");
    const sql = readFileSync(sqlFile, "utf-8");
    const config = extractDbtConfig(sql);
    const resourceType = inferResourceType(sqlFile, projectPath);

    // Determine schema (from config or default based on file path)
    const relPath = relative(projectPath, sqlFile);
    const pathParts = dirname(relPath).split("/");
    const inferredSchema = config.schema || pathParts[pathParts.length - 1] || targetSchema;

    const tableName = config.alias || modelName;
    const fqn = buildFQN(targetDatabase, inferredSchema, tableName);

    modelNodeMap.set(modelName, fqn);
    modelFileMap.set(modelName, { filePath: sqlFile, sql, config });

    const node: GraphNode = {
      id: fqn,
      name: tableName,
      type: mapResourceTypeToNodeType(resourceType),
      subtype: mapResourceTypeToSubtype(resourceType),
      repo: "rippling-dbt",
      metadata: {
        schema: inferredSchema,
        database: targetDatabase,
        materialization: config.materialized,
        tags: config.tags,
        filePath: relPath,
      },
    };
    nodes.push(node);

    citations.push({
      id: uuid(),
      nodeId: fqn,
      filePath: sqlFile,
    });
  }

  // Second pass: create edges from refs and sources
  for (const [modelName, { sql }] of modelFileMap.entries()) {
    const toFqn = modelNodeMap.get(modelName);
    if (!toFqn) continue;

    const references = extractDbtReferences(sql);

    for (const ref of references) {
      if (ref.type === "ref" && ref.modelName) {
        const fromFqn = modelNodeMap.get(ref.modelName);
        if (fromFqn) {
          edges.push({
            id: uuid(),
            from: fromFqn,
            to: toFqn,
            type: "ref",
          });
        } else {
          // Create placeholder node for unknown ref
          const placeholderFqn = buildFQN(targetDatabase, targetSchema, ref.modelName);
          if (!nodes.some((n) => n.id === placeholderFqn)) {
            nodes.push({
              id: placeholderFqn,
              name: ref.modelName,
              type: "model",
              subtype: "dbt_model",
              repo: "rippling-dbt",
              metadata: {
                schema: targetSchema,
                database: targetDatabase,
                description: "(Referenced model - source file not found)",
              },
            });
            modelNodeMap.set(ref.modelName, placeholderFqn);
          }
          edges.push({
            id: uuid(),
            from: placeholderFqn,
            to: toFqn,
            type: "ref",
          });
        }
      } else if (ref.type === "source" && ref.sourceName && ref.tableName) {
        const sourceKey = `${ref.sourceName}.${ref.tableName}`;
        let fromFqn = sourceNodeMap.get(sourceKey);

        if (!fromFqn) {
          // Create placeholder source node
          fromFqn = buildFQN(targetDatabase, ref.sourceName, ref.tableName);
          if (!nodes.some((n) => n.id === fromFqn)) {
            nodes.push({
              id: fromFqn,
              name: ref.tableName,
              type: "source",
              subtype: "dbt_source",
              repo: "rippling-dbt",
              metadata: {
                schema: ref.sourceName,
                database: targetDatabase,
                description: "(Referenced source - definition not found in sources.yml)",
              },
            });
            sourceNodeMap.set(sourceKey, fromFqn);
          }
        }

        edges.push({
          id: uuid(),
          from: fromFqn,
          to: toFqn,
          type: "source",
        });
      }
    }
  }

  return { nodes, edges, citations };
}

