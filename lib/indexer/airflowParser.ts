import { readFileSync, readdirSync, statSync, existsSync } from "fs";
import { join, relative, basename, dirname } from "path";
import { v4 as uuid } from "uuid";
import type { GraphNode, GraphEdge, Citation } from "../types";

export interface AirflowParseResult {
  nodes: GraphNode[];
  edges: GraphEdge[];
  citations: Citation[];
  externalSystems: ExternalSystemDetection[];
}

interface DagInfo {
  dagId: string;
  filePath: string;
  tasks: TaskInfo[];
  sqlFiles: string[];
}

interface TaskInfo {
  taskId: string;
  operator: string;
  sqlPath?: string;
  upstreamTasks: string[];
}

// External system detection from DAG patterns
export interface ExternalSystemDetection {
  name: string;
  type: "application" | "dashboard" | "reverse_etl";
  detectedFrom: string; // file path
  operator?: string;
  consumesFrom: string[]; // table names that feed into this
}

// Known external operators and patterns
const EXTERNAL_OPERATOR_PATTERNS: Array<{
  pattern: RegExp;
  name: string;
  type: "application" | "dashboard" | "reverse_etl";
}> = [
  { pattern: /OutreachOperator/i, name: "Outreach.io Sequences", type: "application" },
  { pattern: /BrevoOperator|SendinblueOperator/i, name: "Brevo Campaigns", type: "application" },
  { pattern: /HightouchOperator/i, name: "Hightouch", type: "reverse_etl" },
  { pattern: /CensusOperator/i, name: "Census", type: "reverse_etl" },
  { pattern: /S3ToExternalOperator|S3Export/i, name: "S3 Export", type: "application" },
  { pattern: /SalesforceOperator|SFDCOperator/i, name: "Salesforce", type: "application" },
  { pattern: /SlackWebhookOperator/i, name: "Slack Notifications", type: "application" },
  { pattern: /EmailOperator|SendEmailOperator/i, name: "Email Notifications", type: "application" },
];

// Patterns that indicate pushing data to external APIs
const EXTERNAL_API_PATTERNS: Array<{
  pattern: RegExp;
  name: string;
  type: "application" | "reverse_etl";
}> = [
  { pattern: /requests\.post\s*\([^)]*outreach/i, name: "Outreach.io API", type: "application" },
  { pattern: /requests\.post\s*\([^)]*brevo|sendinblue/i, name: "Brevo API", type: "application" },
  { pattern: /api\.outreach\.io/i, name: "Outreach.io API", type: "application" },
  { pattern: /api\.brevo\.com|api\.sendinblue\.com/i, name: "Brevo API", type: "application" },
  { pattern: /hightouch.*sync|sync.*hightouch/i, name: "Hightouch Sync", type: "reverse_etl" },
  { pattern: /census.*sync|sync.*census/i, name: "Census Sync", type: "reverse_etl" },
  { pattern: /push_to_sfdc|salesforce.*push|push.*salesforce/i, name: "Salesforce Push", type: "application" },
  { pattern: /export_to_looker|looker.*export/i, name: "Looker Export", type: "dashboard" },
];

// Extract table references from SQL
function extractTablesFromSql(sql: string): { tables: string[]; creates: string[] } {
  const tables: string[] = [];
  const creates: string[] = [];

  // Normalize SQL
  const normalized = sql
    .replace(/--.*$/gm, "") // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, "") // Remove block comments
    .replace(/\s+/g, " ") // Normalize whitespace
    .toUpperCase();

  // Match FROM and JOIN clauses
  const fromPattern = /(?:FROM|JOIN)\s+([A-Z0-9_]+\.?[A-Z0-9_]+\.?[A-Z0-9_]+)/gi;
  let match;
  while ((match = fromPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!table.startsWith("(") && !tables.includes(table)) {
      tables.push(table);
    }
  }

  // Match CREATE TABLE/VIEW
  const createPattern = /CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_]+\.?[A-Z0-9_]+\.?[A-Z0-9_]+)/gi;
  while ((match = createPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!creates.includes(table)) {
      creates.push(table);
    }
  }

  // Match INSERT INTO
  const insertPattern = /INSERT\s+(?:INTO|OVERWRITE)\s+([A-Z0-9_]+\.?[A-Z0-9_]+\.?[A-Z0-9_]+)/gi;
  while ((match = insertPattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!creates.includes(table)) {
      creates.push(table);
    }
  }

  // Match MERGE INTO
  const mergePattern = /MERGE\s+INTO\s+([A-Z0-9_]+\.?[A-Z0-9_]+\.?[A-Z0-9_]+)/gi;
  while ((match = mergePattern.exec(normalized)) !== null) {
    const table = match[1].toLowerCase();
    if (!creates.includes(table)) {
      creates.push(table);
    }
  }

  return { tables, creates };
}

/**
 * Detect external systems from a Python DAG file
 */
function detectExternalSystems(filePath: string, content: string): ExternalSystemDetection[] {
  const detections: ExternalSystemDetection[] = [];
  const seenSystems = new Set<string>();

  // Check for external operators
  for (const { pattern, name, type } of EXTERNAL_OPERATOR_PATTERNS) {
    if (pattern.test(content) && !seenSystems.has(name)) {
      seenSystems.add(name);
      detections.push({
        name,
        type,
        detectedFrom: filePath,
        operator: pattern.source,
        consumesFrom: [],
      });
    }
  }

  // Check for external API patterns
  for (const { pattern, name, type } of EXTERNAL_API_PATTERNS) {
    if (pattern.test(content) && !seenSystems.has(name)) {
      seenSystems.add(name);
      detections.push({
        name,
        type,
        detectedFrom: filePath,
        consumesFrom: [],
      });
    }
  }

  // Try to extract what tables are being read for these external pushes
  // Look for patterns like: SELECT * FROM table, df.read_table('table'), etc.
  if (detections.length > 0) {
    // Find table references in the file that might be consumed
    const tablePatterns = [
      /read_table\s*\(\s*['"]([^'"]+)['"]/gi,
      /query\s*=\s*['"]\s*SELECT.*FROM\s+([A-Z0-9_]+\.?[A-Z0-9_]+\.?[A-Z0-9_]+)/gi,
      /table_name\s*=\s*['"]([^'"]+)['"]/gi,
      /source_table\s*=\s*['"]([^'"]+)['"]/gi,
    ];

    const tables: string[] = [];
    for (const pattern of tablePatterns) {
      let match;
      while ((match = pattern.exec(content)) !== null) {
        const table = match[1].toLowerCase();
        if (!tables.includes(table)) {
          tables.push(table);
        }
      }
    }

    // Add found tables to all detections
    for (const detection of detections) {
      detection.consumesFrom = tables;
    }
  }

  return detections;
}

// Parse a Python DAG file to extract basic info
function parseDagFile(filePath: string): DagInfo | null {
  try {
    const content = readFileSync(filePath, "utf-8");
    const fileName = basename(filePath, ".py");

    // Extract DAG ID
    const dagIdMatch = content.match(/dag_id\s*=\s*['"]([\w_-]+)['"]/);
    const dagId = dagIdMatch?.[1] || fileName;

    // Extract SQL file references
    const sqlFiles: string[] = [];
    const sqlPathPattern = /(?:get_sql_query_from_path|file_name)\s*=\s*['"]([^'"]+\.sql)['"]/g;
    let sqlMatch: RegExpExecArray | null;
    while ((sqlMatch = sqlPathPattern.exec(content)) !== null) {
      sqlFiles.push(sqlMatch[1]);
    }

    // Also look for module_folder references
    const modulePattern = /module_folder\s*=\s*['"]([^'"]+)['"]/g;
    const modules: string[] = [];
    let modMatch: RegExpExecArray | null;
    while ((modMatch = modulePattern.exec(content)) !== null) {
      modules.push(modMatch[1]);
    }

    // Extract task IDs and operators
    const tasks: TaskInfo[] = [];
    const taskPattern = /(\w+)\s*=\s*(SnowflakeOperator|PythonOperator|DummyOperator|BashOperator)/g;
    let taskMatch: RegExpExecArray | null;
    while ((taskMatch = taskPattern.exec(content)) !== null) {
      tasks.push({
        taskId: taskMatch[1],
        operator: taskMatch[2],
        upstreamTasks: [],
      });
    }

    // Extract task dependencies (>> operator)
    const depPattern = /(\w+)\s*>>\s*(\w+)/g;
    let depMatch: RegExpExecArray | null;
    while ((depMatch = depPattern.exec(content)) !== null) {
      const downstream = tasks.find((t) => t.taskId === depMatch![2]);
      if (downstream) {
        downstream.upstreamTasks.push(depMatch[1]);
      }
    }

    return {
      dagId,
      filePath,
      tasks,
      sqlFiles,
    };
  } catch {
    return null;
  }
}

// Recursively find all SQL files in a directory
function findSqlFiles(dir: string): string[] {
  const results: string[] = [];

  if (!existsSync(dir)) return results;

  const entries = readdirSync(dir);
  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);

    if (stat.isDirectory()) {
      results.push(...findSqlFiles(fullPath));
    } else if (entry.endsWith(".sql")) {
      results.push(fullPath);
    }
  }

  return results;
}

// Recursively find all DAG Python files
function findDagFiles(dir: string): string[] {
  const results: string[] = [];

  if (!existsSync(dir)) return results;

  const entries = readdirSync(dir);
  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);

    if (stat.isDirectory()) {
      results.push(...findDagFiles(fullPath));
    } else if (entry.endsWith(".py") && !entry.startsWith("__")) {
      results.push(fullPath);
    }
  }

  return results;
}

export async function parseAirflowDags(
  airflowPath: string,
  onProgress?: (percent: number, message: string) => void
): Promise<AirflowParseResult> {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const citations: Citation[] = [];
  const externalSystems: ExternalSystemDetection[] = [];
  const nodeMap = new Map<string, GraphNode>();
  const seenExternalSystems = new Set<string>();

  const dagsDir = join(airflowPath, "airflow_dags", "dags");
  const resourcesDir = join(airflowPath, "airflow_dags", "resources");

  onProgress?.(10, "Finding DAG files...");

  // Find all DAG files
  const dagFiles = findDagFiles(dagsDir);
  onProgress?.(20, `Found ${dagFiles.length} DAG files`);

  // Find all SQL files
  const sqlFiles = findSqlFiles(resourcesDir);
  onProgress?.(30, `Found ${sqlFiles.length} SQL files`);

  // Parse SQL files to extract table references
  let sqlProcessed = 0;
  for (const sqlPath of sqlFiles) {
    try {
      const sql = readFileSync(sqlPath, "utf-8");
      const { tables, creates } = extractTablesFromSql(sql);

      // Create nodes for tables created by this SQL
      for (const tableName of creates) {
        const fqn = normalizeFqn(tableName);
        if (!nodeMap.has(fqn)) {
          const node: GraphNode = {
            id: fqn,
            name: tableName.split(".").pop() || tableName,
            type: "table",
            subtype: "airflow_table",
            repo: "airflow-dags",
            metadata: {
              filePath: relative(airflowPath, sqlPath),
            },
          };
          nodeMap.set(fqn, node);
          nodes.push(node);

          // Add citation
          citations.push({
            id: uuid(),
            nodeId: fqn,
            filePath: sqlPath,
          });
        }

        // Create edges from source tables
        for (const sourceTable of tables) {
          const sourceFqn = normalizeFqn(sourceTable);
          
          // Create source node if doesn't exist
          if (!nodeMap.has(sourceFqn)) {
            const sourceNode: GraphNode = {
              id: sourceFqn,
              name: sourceTable.split(".").pop() || sourceTable,
              type: "table",
              subtype: "airflow_table",
              repo: "airflow-dags",
            };
            nodeMap.set(sourceFqn, sourceNode);
            nodes.push(sourceNode);
          }

          // Create edge
          edges.push({
            id: uuid(),
            from: sourceFqn,
            to: fqn,
            type: "sql_dependency",
            metadata: {
              sqlSnippet: sql.substring(0, 200),
            },
          });
        }
      }
    } catch {
      // Skip files that can't be parsed
    }

    sqlProcessed++;
    if (sqlProcessed % 50 === 0) {
      onProgress?.(30 + Math.floor((sqlProcessed / sqlFiles.length) * 60), `Parsed ${sqlProcessed}/${sqlFiles.length} SQL files`);
    }
  }

  // Parse DAG files for orchestration edges and external system detection
  onProgress?.(90, "Parsing DAG dependencies and detecting external systems...");

  for (const dagFile of dagFiles) {
    // Read the file content for external system detection
    let dagContent = "";
    try {
      dagContent = readFileSync(dagFile, "utf-8");
    } catch {
      continue;
    }

    const dagInfo = parseDagFile(dagFile);
    if (!dagInfo) continue;

    // Add DAG citation to relevant nodes
    for (const sqlFile of dagInfo.sqlFiles) {
      // Try to find nodes created by this SQL
      const matchingNodes = nodes.filter((n) => 
        n.metadata?.filePath?.includes(sqlFile)
      );
      
      for (const node of matchingNodes) {
        citations.push({
          id: uuid(),
          nodeId: node.id,
          filePath: dagFile,
        });
      }
    }

    // Detect external systems in this DAG
    const detected = detectExternalSystems(dagFile, dagContent);
    for (const ext of detected) {
      // Deduplicate by system name
      if (!seenExternalSystems.has(ext.name)) {
        seenExternalSystems.add(ext.name);
        externalSystems.push(ext);
      } else {
        // Merge table references for existing system
        const existing = externalSystems.find(e => e.name === ext.name);
        if (existing) {
          for (const table of ext.consumesFrom) {
            if (!existing.consumesFrom.includes(table)) {
              existing.consumesFrom.push(table);
            }
          }
        }
      }
    }
  }

  const externalCount = externalSystems.length;
  onProgress?.(100, `Parsed ${nodes.length} tables, detected ${externalCount} external systems`);

  return { nodes, edges, citations, externalSystems };
}

// Normalize table names to FQN format
function normalizeFqn(tableName: string): string {
  const parts = tableName.toLowerCase().split(".");
  
  // If already 3 parts, return as-is
  if (parts.length === 3) {
    return parts.join(".");
  }
  
  // If 2 parts (schema.table), assume default database
  if (parts.length === 2) {
    return `prod_rippling_dwh.${parts[0]}.${parts[1]}`;
  }
  
  // If 1 part, assume default db.schema
  return `prod_rippling_dwh.public.${parts[0]}`;
}

