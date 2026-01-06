import { Parser } from "node-sql-parser";

const parser = new Parser();

export interface SqlParseResult {
  sourceTables: string[];
  targetTables: string[];
  columns: ColumnReference[];
}

export interface ColumnReference {
  table: string;
  column: string;
  alias?: string;
}

/**
 * Parse SQL and extract table dependencies
 * Returns source tables (FROM/JOIN) and target tables (CREATE/INSERT/MERGE)
 */
export function parseSql(sql: string): SqlParseResult {
  const sourceTables: string[] = [];
  const targetTables: string[] = [];
  const columns: ColumnReference[] = [];

  try {
    // Try to parse with Snowflake dialect
    const ast = parser.astify(sql, { database: "Snowflake" });
    const statements = Array.isArray(ast) ? ast : [ast];

    for (const stmt of statements) {
      extractTables(stmt, sourceTables, targetTables);
      extractColumns(stmt, columns);
    }
  } catch {
    // Fall back to regex-based extraction
    return extractTablesRegex(sql);
  }

  return {
    sourceTables: [...new Set(sourceTables)],
    targetTables: [...new Set(targetTables)],
    columns,
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractTables(node: any, sources: string[], targets: string[]): void {
  if (!node || typeof node !== "object") return;

  // Handle different statement types
  if (node.type === "select" || node.type === "union") {
    // Extract FROM clause
    if (node.from) {
      for (const fromItem of Array.isArray(node.from) ? node.from : [node.from]) {
        if (fromItem.table) {
          const tableName = buildTableName(fromItem);
          if (tableName) sources.push(tableName);
        }
        // Recurse into subqueries
        if (fromItem.expr) {
          extractTables(fromItem.expr, sources, targets);
        }
      }
    }

    // Extract JOIN clauses
    if (node.join) {
      for (const joinItem of Array.isArray(node.join) ? node.join : [node.join]) {
        if (joinItem.table) {
          const tableName = buildTableName(joinItem);
          if (tableName) sources.push(tableName);
        }
      }
    }

    // Recurse into subqueries in WHERE, etc.
    if (node.where) extractTables(node.where, sources, targets);
    if (node.columns) {
      for (const col of node.columns) {
        if (col.expr) extractTables(col.expr, sources, targets);
      }
    }
  }

  // Handle CREATE TABLE/VIEW
  if (node.type === "create") {
    if (node.table) {
      const tableName = buildTableName(node);
      if (tableName) targets.push(tableName);
    }
    // The source is in the AS SELECT
    if (node.as) {
      extractTables(node.as, sources, targets);
    }
  }

  // Handle INSERT
  if (node.type === "insert") {
    if (node.table) {
      const tableName = buildTableName(node);
      if (tableName) targets.push(tableName);
    }
    // Source from VALUES or SELECT
    if (node.values) {
      for (const val of node.values) {
        extractTables(val, sources, targets);
      }
    }
  }

  // Handle MERGE
  if (node.type === "merge") {
    if (node.target) {
      const tableName = buildTableName({ table: node.target });
      if (tableName) targets.push(tableName);
    }
    if (node.source) {
      const tableName = buildTableName({ table: node.source });
      if (tableName) sources.push(tableName);
    }
  }

  // Handle UNION
  if (node.type === "union" || node._next) {
    if (node._next) {
      extractTables(node._next, sources, targets);
    }
  }

  // Recurse into arrays
  if (Array.isArray(node)) {
    for (const item of node) {
      extractTables(item, sources, targets);
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildTableName(node: any): string | null {
  if (!node.table) return null;

  const parts: string[] = [];
  if (node.db) parts.push(node.db);
  if (node.schema) parts.push(node.schema);
  parts.push(node.table);

  return parts.join(".").toLowerCase();
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractColumns(node: any, columns: ColumnReference[]): void {
  // Column extraction for lineage - simplified for MVP
  if (!node || typeof node !== "object") return;

  if (node.type === "select" && node.columns) {
    for (const col of node.columns) {
      if (col.expr && col.expr.type === "column_ref") {
        columns.push({
          table: col.expr.table || "",
          column: col.expr.column || "",
          alias: col.as || undefined,
        });
      }
    }
  }
}

/**
 * Fallback regex-based extraction when parser fails
 */
function extractTablesRegex(sql: string): SqlParseResult {
  const sourceTables: string[] = [];
  const targetTables: string[] = [];

  // Normalize SQL
  const normalized = sql
    .replace(/--.*$/gm, "") // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, "") // Remove block comments
    .replace(/\s+/g, " ") // Normalize whitespace
    .toUpperCase();

  // Match FROM and JOIN clauses
  const fromPattern = /(?:FROM|JOIN)\s+([A-Z0-9_]+(?:\.[A-Z0-9_]+){0,2})/gi;
  let match;
  while ((match = fromPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!table.startsWith("(") && !sourceTables.includes(table)) {
      sourceTables.push(table);
    }
  }

  // Match CREATE TABLE/VIEW
  const createPattern = /CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_]+(?:\.[A-Z0-9_]+){0,2})/gi;
  while ((match = createPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!targetTables.includes(table)) {
      targetTables.push(table);
    }
  }

  // Match INSERT INTO
  const insertPattern = /INSERT\s+(?:INTO|OVERWRITE)\s+([A-Z0-9_]+(?:\.[A-Z0-9_]+){0,2})/gi;
  while ((match = insertPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!targetTables.includes(table)) {
      targetTables.push(table);
    }
  }

  // Match MERGE INTO
  const mergePattern = /MERGE\s+INTO\s+([A-Z0-9_]+(?:\.[A-Z0-9_]+){0,2})/gi;
  while ((match = mergePattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!targetTables.includes(table)) {
      targetTables.push(table);
    }
  }

  return {
    sourceTables,
    targetTables,
    columns: [],
  };
}

/**
 * Extract column-level lineage from SQL
 * Returns mapping of output columns to their source columns
 */
export function extractColumnLineage(
  sql: string
): Map<string, { sourceTable: string; sourceColumn: string }[]> {
  const lineage = new Map<string, { sourceTable: string; sourceColumn: string }[]>();

  try {
    const ast = parser.astify(sql, { database: "Snowflake" });
    const stmt = Array.isArray(ast) ? ast[0] : ast;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if ((stmt as any).type === "select" && (stmt as any).columns) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      for (const col of (stmt as any).columns) {
        const outputName = col.as || (col.expr?.column as string) || "*";
        const sources: { sourceTable: string; sourceColumn: string }[] = [];

        // Extract source columns from expression
        extractColumnSources(col.expr, sources);

        if (sources.length > 0) {
          lineage.set(outputName, sources);
        }
      }
    }
  } catch {
    // Column lineage extraction failed, return empty map
  }

  return lineage;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractColumnSources(
  expr: any,
  sources: { sourceTable: string; sourceColumn: string }[]
): void {
  if (!expr || typeof expr !== "object") return;

  if (expr.type === "column_ref") {
    sources.push({
      sourceTable: expr.table || "",
      sourceColumn: expr.column || "",
    });
  }

  // Recurse into function arguments, binary expressions, etc.
  if (expr.args) {
    for (const arg of Array.isArray(expr.args) ? expr.args : [expr.args]) {
      extractColumnSources(arg, sources);
    }
  }
  if (expr.left) extractColumnSources(expr.left, sources);
  if (expr.right) extractColumnSources(expr.right, sources);
  if (expr.expr) extractColumnSources(expr.expr, sources);
}

