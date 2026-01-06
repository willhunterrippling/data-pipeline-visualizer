import snowflake from "snowflake-sdk";

export interface SnowflakeConfig {
  account: string;
  username: string;
  authenticator: "externalbrowser" | "snowflake" | "oauth";
  warehouse: string;
  database: string;
  role?: string;
}

export interface ColumnMetadata {
  name: string;
  type: string;
  nullable: boolean;
  comment?: string;
}

export interface TableMetadata {
  database: string;
  schema: string;
  name: string;
  type: "TABLE" | "VIEW" | "EXTERNAL TABLE";
  columns: ColumnMetadata[];
  comment?: string;
  ddl?: string;
}

let connection: snowflake.Connection | null = null;

/**
 * Create a Snowflake connection with SSO (externalbrowser) auth
 */
export async function connect(config: SnowflakeConfig): Promise<snowflake.Connection> {
  if (connection) {
    return connection;
  }

  return new Promise((resolve, reject) => {
    const conn = snowflake.createConnection({
      account: config.account,
      username: config.username,
      authenticator: config.authenticator.toUpperCase(),
      warehouse: config.warehouse,
      database: config.database,
      role: config.role,
      clientSessionKeepAlive: true,
    });

    conn.connect((err, conn) => {
      if (err) {
        reject(new Error(`Snowflake connection failed: ${err.message}`));
      } else {
        connection = conn;
        resolve(conn);
      }
    });
  });
}

/**
 * Execute a query and return results
 */
export async function executeQuery<T = Record<string, unknown>>(
  sql: string
): Promise<T[]> {
  if (!connection) {
    throw new Error("Not connected to Snowflake");
  }

  return new Promise((resolve, reject) => {
    connection!.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(new Error(`Query failed: ${err.message}`));
        } else {
          resolve((rows || []) as T[]);
        }
      },
    });
  });
}

/**
 * Get list of schemas in a database
 */
export async function getSchemas(database: string): Promise<string[]> {
  const rows = await executeQuery<{ SCHEMA_NAME: string }>(
    `SHOW SCHEMAS IN DATABASE "${database}"`
  );
  return rows.map((r) => r.SCHEMA_NAME);
}

/**
 * Get list of tables/views in a schema
 */
export async function getTables(
  database: string,
  schema: string
): Promise<{ name: string; type: string }[]> {
  const rows = await executeQuery<{ TABLE_NAME: string; TABLE_TYPE: string }>(
    `SELECT TABLE_NAME, TABLE_TYPE 
     FROM "${database}".INFORMATION_SCHEMA.TABLES 
     WHERE TABLE_SCHEMA = '${schema}'
     ORDER BY TABLE_NAME`
  );
  return rows.map((r) => ({ name: r.TABLE_NAME, type: r.TABLE_TYPE }));
}

/**
 * Get column metadata for a table
 */
export async function getColumns(
  database: string,
  schema: string,
  table: string
): Promise<ColumnMetadata[]> {
  const rows = await executeQuery<{
    COLUMN_NAME: string;
    DATA_TYPE: string;
    IS_NULLABLE: string;
    COMMENT: string | null;
  }>(
    `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
     FROM "${database}".INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
     ORDER BY ORDINAL_POSITION`
  );

  return rows.map((r) => ({
    name: r.COLUMN_NAME,
    type: r.DATA_TYPE,
    nullable: r.IS_NULLABLE === "YES",
    comment: r.COMMENT || undefined,
  }));
}

/**
 * Get DDL for a table or view
 */
export async function getDDL(
  database: string,
  schema: string,
  table: string,
  objectType: "TABLE" | "VIEW" = "TABLE"
): Promise<string | null> {
  try {
    const rows = await executeQuery<{ "GET_DDL(...)": string }>(
      `SELECT GET_DDL('${objectType}', '"${database}"."${schema}"."${table}"')`
    );
    return rows[0]?.["GET_DDL(...)"] || null;
  } catch {
    return null;
  }
}

/**
 * Get view definition
 */
export async function getViewDefinition(
  database: string,
  schema: string,
  view: string
): Promise<string | null> {
  const rows = await executeQuery<{ VIEW_DEFINITION: string | null }>(
    `SELECT VIEW_DEFINITION
     FROM "${database}".INFORMATION_SCHEMA.VIEWS
     WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${view}'`
  );
  return rows[0]?.VIEW_DEFINITION || null;
}

/**
 * Get full metadata for a table including columns and DDL
 */
export async function getTableMetadata(
  database: string,
  schema: string,
  table: string
): Promise<TableMetadata | null> {
  // Get basic table info
  const tableRows = await executeQuery<{
    TABLE_NAME: string;
    TABLE_TYPE: string;
    COMMENT: string | null;
  }>(
    `SELECT TABLE_NAME, TABLE_TYPE, COMMENT
     FROM "${database}".INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'`
  );

  if (tableRows.length === 0) {
    return null;
  }

  const tableInfo = tableRows[0];
  const columns = await getColumns(database, schema, table);

  let ddl: string | null = null;
  if (tableInfo.TABLE_TYPE === "VIEW") {
    ddl = await getViewDefinition(database, schema, table);
  } else {
    ddl = await getDDL(database, schema, table, "TABLE");
  }

  return {
    database,
    schema,
    name: tableInfo.TABLE_NAME,
    type: tableInfo.TABLE_TYPE as TableMetadata["type"],
    columns,
    comment: tableInfo.COMMENT || undefined,
    ddl: ddl || undefined,
  };
}

/**
 * Batch fetch metadata for multiple tables
 */
export async function batchGetMetadata(
  tables: Array<{ database: string; schema: string; table: string }>
): Promise<Map<string, TableMetadata>> {
  const results = new Map<string, TableMetadata>();

  // Batch by database/schema for efficiency
  const grouped = new Map<string, Array<{ database: string; schema: string; table: string }>>();
  for (const t of tables) {
    const key = `${t.database}.${t.schema}`;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(t);
  }

  for (const [, batch] of grouped) {
    // Get all columns for this schema in one query
    const { database, schema } = batch[0];
    const tableNames = batch.map((t) => t.table);
    const tableNamesStr = tableNames.map((n) => `'${n}'`).join(",");

    const columnRows = await executeQuery<{
      TABLE_NAME: string;
      COLUMN_NAME: string;
      DATA_TYPE: string;
      IS_NULLABLE: string;
      COMMENT: string | null;
    }>(
      `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
       FROM "${database}".INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME IN (${tableNamesStr})
       ORDER BY TABLE_NAME, ORDINAL_POSITION`
    );

    const tableRows = await executeQuery<{
      TABLE_NAME: string;
      TABLE_TYPE: string;
      COMMENT: string | null;
    }>(
      `SELECT TABLE_NAME, TABLE_TYPE, COMMENT
       FROM "${database}".INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME IN (${tableNamesStr})`
    );

    // Group columns by table
    const columnsByTable = new Map<string, ColumnMetadata[]>();
    for (const row of columnRows) {
      if (!columnsByTable.has(row.TABLE_NAME)) {
        columnsByTable.set(row.TABLE_NAME, []);
      }
      columnsByTable.get(row.TABLE_NAME)!.push({
        name: row.COLUMN_NAME,
        type: row.DATA_TYPE,
        nullable: row.IS_NULLABLE === "YES",
        comment: row.COMMENT || undefined,
      });
    }

    // Build metadata for each table
    for (const tableRow of tableRows) {
      const fqn = `${database}.${schema}.${tableRow.TABLE_NAME}`.toLowerCase();
      results.set(fqn, {
        database,
        schema,
        name: tableRow.TABLE_NAME,
        type: tableRow.TABLE_TYPE as TableMetadata["type"],
        columns: columnsByTable.get(tableRow.TABLE_NAME) || [],
        comment: tableRow.COMMENT || undefined,
      });
    }
  }

  return results;
}

/**
 * Disconnect from Snowflake
 */
export function disconnect(): Promise<void> {
  return new Promise((resolve) => {
    if (connection) {
      connection.destroy((err) => {
        if (err) {
          console.error("Error disconnecting from Snowflake:", err);
        }
        connection = null;
        resolve();
      });
    } else {
      resolve();
    }
  });
}

/**
 * Check if connected
 */
export function isConnected(): boolean {
  return connection !== null && connection.isUp();
}

