import { GraphArtifact } from "@/lib/graph/types";

export async function GET() {
  const graph: GraphArtifact = {
    nodes: [],
    edges: [],
    flows: [],
    generatedAt: new Date().toISOString(),
  };

  return Response.json(graph);
}
