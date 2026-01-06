// Node types
export type NodeType = "table" | "view" | "model" | "source" | "seed" | "external";
export type NodeSubtype = "dbt_model" | "dbt_source" | "dbt_seed" | "airflow_table" | "snowflake_native" | "external_feed";

export interface GraphNode {
  id: string;
  name: string;
  type: NodeType;
  subtype?: NodeSubtype;
  groupId?: string;
  repo?: string;
  metadata?: NodeMetadata;
}

export interface NodeMetadata {
  columns?: ColumnInfo[];
  tags?: string[];
  materialization?: string;
  schedule?: string;
  description?: string;
  schema?: string;
  database?: string;
  filePath?: string;
  lineStart?: number;
  lineEnd?: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  description?: string;
}

// Edge types
export type EdgeType = "ref" | "source" | "sql_dependency" | "dag_edge" | "materialization";

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  type: EdgeType;
  metadata?: EdgeMetadata;
}

export interface EdgeMetadata {
  sqlSnippet?: string;
  transformationType?: string;
}

// Group types
export interface GraphGroup {
  id: string;
  name: string;
  description?: string;
  parentId?: string;
  inferenceReason?: string;
  nodeCount: number;
  collapsedDefault: boolean;
}

// Flow types
export interface GraphFlow {
  id: string;
  name: string;
  description?: string;
  anchorNodes: string[];
  memberNodes: string[];
  userDefined: boolean;
  inferenceReason?: string;
}

// Citation types
export interface Citation {
  id: string;
  nodeId?: string;
  edgeId?: string;
  filePath: string;
  startLine?: number;
  endLine?: number;
  snippet?: string;
}

// Explanation types
export interface Explanation {
  nodeId: string;
  summary: string;
  generatedAt: string;
  modelUsed: string;
}

// Job/Progress types
export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface JobState {
  id: string;
  status: JobStatus;
  stage?: string;
  stageProgress: number;
  message?: string;
  error?: string;
  startedAt: string;
  updatedAt: string;
}

export const INDEXING_STAGES = [
  { id: "dbt_compile", name: "Compiling dbt project", startPct: 0, endPct: 10 },
  { id: "parse_manifest", name: "Parsing dbt manifest", startPct: 10, endPct: 25 },
  { id: "parse_airflow", name: "Parsing Airflow DAGs", startPct: 25, endPct: 40 },
  { id: "parse_sql", name: "Extracting SQL dependencies", startPct: 40, endPct: 55 },
  { id: "snowflake_metadata", name: "Fetching Snowflake metadata", startPct: 55, endPct: 70 },
  { id: "cross_repo_link", name: "Linking cross-repo entities", startPct: 70, endPct: 80 },
  { id: "ai_grouping", name: "AI: Inferring groups", startPct: 80, endPct: 90 },
  { id: "ai_flows", name: "AI: Proposing flows", startPct: 90, endPct: 95 },
  { id: "precompute_explanations", name: "Pre-computing explanations", startPct: 95, endPct: 100 },
] as const;

export type IndexingStageId = typeof INDEXING_STAGES[number]["id"];

// Full graph artifact
export interface GraphArtifact {
  nodes: GraphNode[];
  edges: GraphEdge[];
  groups: GraphGroup[];
  flows: GraphFlow[];
  generatedAt: string;
}

