import { complete } from "./client";
import type { GraphNode } from "../types";
import { getModelSql } from "../indexer/dbtParser";

const RELATIONAL_EXPLAIN_PROMPT = `You are an expert data engineer explaining how two tables in a data pipeline relate to each other.

Given a source table/model and an anchor (target) table, along with the path between them, provide a clear, concise explanation that covers:

1. **Data Flow**: What data flows from one to the other and through which intermediate tables
2. **Transformation**: Key transformations that happen along the path
3. **Business Context**: Why this relationship matters for the anchor table's purpose

Keep the explanation under 150 words. Use "straight answer" style - lead with the key relationship insight.
Reference table names using backticks like \`table_name\` so they can be clickable in the UI.`;

export interface PathInfo {
  direction: "upstream" | "downstream";
  hops: number;
  intermediateNodes: GraphNode[];
}

export interface RelationalExplanationResult {
  fullExplanation: string;
  transformationSummary: string;
  businessContext: string;
}

/**
 * Generate an explanation of how a node relates to an anchor node
 */
export async function explainRelationship(
  node: GraphNode,
  anchor: GraphNode,
  pathInfo: PathInfo,
  options?: {
    repoPath?: string;
  }
): Promise<RelationalExplanationResult> {
  const context: string[] = [];

  // Basic info about both nodes
  context.push(`Source table: \`${node.name}\` (${node.type}${node.subtype ? `, ${node.subtype}` : ""})`);
  context.push(`Anchor table: \`${anchor.name}\` (${anchor.type}${anchor.subtype ? `, ${anchor.subtype}` : ""})`);
  context.push("");

  // Path info
  context.push(`Relationship: \`${node.name}\` is ${pathInfo.hops} hop${pathInfo.hops !== 1 ? "s" : ""} ${pathInfo.direction} of \`${anchor.name}\``);

  // Intermediate nodes
  if (pathInfo.intermediateNodes.length > 0) {
    const intermediateNames = pathInfo.intermediateNodes.map((n) => `\`${n.name}\``).join(" → ");
    if (pathInfo.direction === "upstream") {
      context.push(`Path: \`${node.name}\` → ${intermediateNames} → \`${anchor.name}\``);
    } else {
      context.push(`Path: \`${anchor.name}\` → ${intermediateNames} → \`${node.name}\``);
    }
  } else {
    if (pathInfo.direction === "upstream") {
      context.push(`Path: \`${node.name}\` → \`${anchor.name}\` (direct connection)`);
    } else {
      context.push(`Path: \`${anchor.name}\` → \`${node.name}\` (direct connection)`);
    }
  }
  context.push("");

  // Add metadata if available
  if (node.metadata?.description) {
    context.push(`Source description: ${node.metadata.description}`);
  }
  if (anchor.metadata?.description) {
    context.push(`Anchor description: ${anchor.metadata.description}`);
  }

  // Add column info if available
  if (node.metadata?.columns?.length) {
    const colNames = node.metadata.columns.slice(0, 5).map((c) => c.name).join(", ");
    context.push(`Source key columns: ${colNames}`);
  }
  if (anchor.metadata?.columns?.length) {
    const colNames = anchor.metadata.columns.slice(0, 5).map((c) => c.name).join(", ");
    context.push(`Anchor key columns: ${colNames}`);
  }

  // Get SQL content for the anchor (shows how it uses the source node)
  // This is the most important SQL to understand the relationship
  if (options?.repoPath && anchor.metadata?.filePath) {
    const anchorSql = getModelSql(options.repoPath, anchor.metadata.filePath);
    if (anchorSql) {
      context.push("");
      context.push(`Anchor SQL (how ${anchor.name} uses its dependencies):`);
      context.push("```sql");
      // Truncate to first 1500 chars to stay within token limits
      context.push(anchorSql.substring(0, 1500));
      if (anchorSql.length > 1500) context.push("... (truncated)");
      context.push("```");
    }
  }

  // If there are intermediate nodes, get SQL for the first one to show transformation chain
  if (options?.repoPath && pathInfo.intermediateNodes.length > 0) {
    const firstIntermediate = pathInfo.intermediateNodes[0];
    if (firstIntermediate.metadata?.filePath) {
      const intermediateSql = getModelSql(options.repoPath, firstIntermediate.metadata.filePath);
      if (intermediateSql) {
        context.push("");
        context.push(`Intermediate transformation (${firstIntermediate.name}):`);
        context.push("```sql");
        // Shorter truncation for intermediate
        context.push(intermediateSql.substring(0, 800));
        if (intermediateSql.length > 800) context.push("... (truncated)");
        context.push("```");
      }
    }
  }

  const userMessage = `Explain the relationship between these two tables in a data pipeline:

${context.join("\n")}

Provide:
1. A description of how data flows and transforms (2-3 sentences)
2. Why this relationship matters for the anchor table (1-2 sentences)`;

  try {
    const explanation = await complete(
      [
        { role: "system", content: RELATIONAL_EXPLAIN_PROMPT },
        { role: "user", content: userMessage },
      ],
      {
        maxTokens: 600,
      }
    );

    // Parse the explanation into parts (best effort)
    const parts = parseExplanationParts(explanation.trim(), node, anchor, pathInfo);

    return parts;
  } catch (error) {
    console.error("Failed to generate relational explanation:", error);
    return generateFallbackRelationalExplanation(node, anchor, pathInfo);
  }
}

/**
 * Parse the AI response into structured parts
 */
function parseExplanationParts(
  fullExplanation: string,
  node: GraphNode,
  anchor: GraphNode,
  pathInfo: PathInfo
): RelationalExplanationResult {
  // Extract transformation summary (look for transformation-related content)
  // This is a best-effort extraction from the full explanation
  let transformationSummary = "";
  let businessContext = "";

  const lines = fullExplanation.split("\n").filter((l) => l.trim());

  // Try to identify different sections
  for (const line of lines) {
    const lowerLine = line.toLowerCase();
    if (
      lowerLine.includes("transform") ||
      lowerLine.includes("aggregat") ||
      lowerLine.includes("join") ||
      lowerLine.includes("filter") ||
      lowerLine.includes("enrich")
    ) {
      if (!transformationSummary) {
        transformationSummary = line.trim();
      }
    }
    if (
      lowerLine.includes("matter") ||
      lowerLine.includes("important") ||
      lowerLine.includes("enable") ||
      lowerLine.includes("purpose") ||
      lowerLine.includes("business")
    ) {
      if (!businessContext) {
        businessContext = line.trim();
      }
    }
  }

  // Fallbacks if extraction didn't work
  if (!transformationSummary) {
    transformationSummary = pathInfo.intermediateNodes.length > 0
      ? `Data flows through ${pathInfo.intermediateNodes.length} intermediate table${pathInfo.intermediateNodes.length !== 1 ? "s" : ""}`
      : "Direct data dependency";
  }

  if (!businessContext) {
    businessContext = pathInfo.direction === "upstream"
      ? `Provides source data for \`${anchor.name}\``
      : `Consumes data produced by \`${anchor.name}\``;
  }

  return {
    fullExplanation,
    transformationSummary,
    businessContext,
  };
}

/**
 * Generate fallback explanation when AI fails
 */
function generateFallbackRelationalExplanation(
  node: GraphNode,
  anchor: GraphNode,
  pathInfo: PathInfo
): RelationalExplanationResult {
  let fullExplanation: string;
  let transformationSummary: string;
  let businessContext: string;

  if (pathInfo.direction === "upstream") {
    // Node feeds into anchor
    if (pathInfo.intermediateNodes.length > 0) {
      const intermediateNames = pathInfo.intermediateNodes.map((n) => `\`${n.name}\``).join(", ");
      fullExplanation = `\`${node.name}\` is a ${pathInfo.hops}-hop upstream dependency of \`${anchor.name}\`. Data flows through ${intermediateNames} before reaching the anchor table.`;
      transformationSummary = `Data passes through ${pathInfo.intermediateNodes.length} intermediate transformation${pathInfo.intermediateNodes.length !== 1 ? "s" : ""}`;
    } else {
      fullExplanation = `\`${node.name}\` is a direct upstream dependency of \`${anchor.name}\`. Data flows directly from this table to the anchor.`;
      transformationSummary = "Direct data feed with no intermediate transformations";
    }

    // Infer business context from node type
    if (node.type === "source" || node.name.startsWith("stg_")) {
      businessContext = `Provides raw or staged source data that \`${anchor.name}\` depends on`;
    } else if (node.name.startsWith("int_")) {
      businessContext = `Provides intermediate business logic transformations used by \`${anchor.name}\``;
    } else {
      businessContext = `Contributes data that feeds into \`${anchor.name}\``;
    }
  } else {
    // Node consumes from anchor
    if (pathInfo.intermediateNodes.length > 0) {
      const intermediateNames = pathInfo.intermediateNodes.map((n) => `\`${n.name}\``).join(", ");
      fullExplanation = `\`${node.name}\` is a ${pathInfo.hops}-hop downstream consumer of \`${anchor.name}\`. Data flows through ${intermediateNames} before reaching this table.`;
      transformationSummary = `Data is transformed through ${pathInfo.intermediateNodes.length} intermediate step${pathInfo.intermediateNodes.length !== 1 ? "s" : ""}`;
    } else {
      fullExplanation = `\`${node.name}\` is a direct downstream consumer of \`${anchor.name}\`. It directly uses data from the anchor table.`;
      transformationSummary = "Direct consumption with no intermediate transformations";
    }

    // Infer business context from node type
    if (node.name.startsWith("mart_") || node.name.startsWith("rpt_")) {
      businessContext = `Uses \`${anchor.name}\` data for analytics or reporting`;
    } else {
      businessContext = `Depends on data produced by \`${anchor.name}\``;
    }
  }

  return {
    fullExplanation,
    transformationSummary,
    businessContext,
  };
}
