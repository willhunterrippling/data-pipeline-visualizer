export type NodeType =
  | "table"
  | "view"
  | "model"
  | "source"
  | "external";

export interface GraphNode {
  id: string;
  name: string;
  type: NodeType;
  group?: string;
  subgroup?: string;
  metadata?: Record<string, any>;
}

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  type: "transform" | "orchestration";
  metadata?: Record<string, any>;
}

export interface GraphArtifact {
  nodes: GraphNode[];
  edges: GraphEdge[];
  flows: {
    name: string;
    nodeIds: string[];
  }[];
  generatedAt: string;
}
