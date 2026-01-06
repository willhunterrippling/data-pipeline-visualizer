import { complete } from "./client";
import type { GraphNode, Citation } from "../types";
import { getModelSql } from "../indexer/dbtParser";

const EXPLAIN_PROMPT = `You are an expert data engineer explaining a data pipeline table to a colleague.

Given a table/model and its context, provide a clear, concise explanation that:
1. States what this table represents in plain English
2. Explains where the data comes from (sources)
3. Describes key transformations or business logic
4. Notes any important caveats or gotchas

Keep the explanation under 150 words. Use "straight answer" style - lead with the key insight.`;

/**
 * Generate a plain-English explanation for a node
 */
export async function explainNode(
  node: GraphNode,
  options?: {
    upstream?: GraphNode[];
    downstream?: GraphNode[];
    citations?: Citation[];
    sqlContent?: string;
    repoPath?: string;
  }
): Promise<string> {
  // Get SQL content if we have a file path
  let sql = options?.sqlContent;
  if (!sql && node.metadata?.filePath && options?.repoPath) {
    sql = getModelSql(options.repoPath, node.metadata.filePath) || undefined;
  }

  // Build context
  const context: string[] = [];
  
  context.push(`Table: ${node.name}`);
  context.push(`Type: ${node.type}${node.subtype ? ` (${node.subtype})` : ""}`);
  
  if (node.metadata?.description) {
    context.push(`Description from code: ${node.metadata.description}`);
  }
  
  if (node.metadata?.schema) {
    context.push(`Schema: ${node.metadata.schema}`);
  }
  
  if (node.metadata?.materialization) {
    context.push(`Materialization: ${node.metadata.materialization}`);
  }
  
  if (node.metadata?.tags?.length) {
    context.push(`Tags: ${node.metadata.tags.join(", ")}`);
  }
  
  if (options?.upstream?.length) {
    context.push(`Upstream tables: ${options.upstream.map((n) => n.name).join(", ")}`);
  }
  
  if (options?.downstream?.length) {
    context.push(`Downstream tables: ${options.downstream.map((n) => n.name).join(", ")}`);
  }
  
  if (node.metadata?.columns?.length) {
    const colSummary = node.metadata.columns
      .slice(0, 10)
      .map((c) => `${c.name} (${c.type})`)
      .join(", ");
    context.push(`Key columns: ${colSummary}`);
  }
  
  if (sql) {
    // Include first 1000 chars of SQL
    context.push(`SQL (truncated):\n${sql.substring(0, 1000)}`);
  }

  const userMessage = `Explain this data pipeline table:

${context.join("\n")}

Provide a brief, clear explanation of what this table does and why it matters.`;

  try {
    const explanation = await complete([
      { role: "system", content: EXPLAIN_PROMPT },
      { role: "user", content: userMessage },
    ], {
      maxTokens: 500,
    });

    return explanation.trim();
  } catch (error) {
    console.error("Failed to generate explanation:", error);
    return generateFallbackExplanation(node);
  }
}

/**
 * Generate fallback explanation when AI fails
 */
function generateFallbackExplanation(node: GraphNode): string {
  const parts: string[] = [];

  // Infer purpose from name
  if (node.name.startsWith("stg_")) {
    parts.push(`Staging model that loads raw data from ${node.name.replace("stg_", "").split("__")[0]} source.`);
  } else if (node.name.startsWith("int_")) {
    const domain = node.name.replace("int_", "").split("__")[0];
    parts.push(`Intermediate model in the ${domain} domain that applies business logic transformations.`);
  } else if (node.name.startsWith("mart_")) {
    const domain = node.name.replace("mart_", "").split("__")[0];
    parts.push(`Mart table for ${domain} analytics, ready for consumption by BI tools and analysts.`);
  } else if (node.name.startsWith("rpt_")) {
    parts.push(`Report view designed for specific business reporting needs.`);
  } else if (node.type === "source") {
    parts.push(`External data source providing raw data to the pipeline.`);
  } else {
    parts.push(`Data table in the pipeline.`);
  }

  if (node.metadata?.description) {
    parts.push(node.metadata.description);
  }

  if (node.metadata?.materialization) {
    parts.push(`Materialized as ${node.metadata.materialization}.`);
  }

  return parts.join(" ");
}

/**
 * Batch generate explanations for key nodes
 */
export async function batchExplainNodes(
  nodes: GraphNode[],
  options?: {
    repoPath?: string;
    onProgress?: (completed: number, total: number) => void;
  }
): Promise<Map<string, string>> {
  const explanations = new Map<string, string>();
  
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    try {
      const explanation = await explainNode(node, { repoPath: options?.repoPath });
      explanations.set(node.id, explanation);
    } catch {
      explanations.set(node.id, generateFallbackExplanation(node));
    }
    
    options?.onProgress?.(i + 1, nodes.length);
  }

  return explanations;
}

