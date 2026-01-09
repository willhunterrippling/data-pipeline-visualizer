import { NextResponse } from "next/server";
import { connect, getSchemas, disconnect } from "@/lib/snowflake/client";
import { getSnowflakeConfig, hasSnowflakeCredentials } from "@/lib/indexer/snowflakeMetadata";

export async function GET() {
  // Check if credentials are configured
  if (!hasSnowflakeCredentials()) {
    return NextResponse.json(
      { error: "Snowflake credentials not configured" },
      { status: 400 }
    );
  }

  const config = getSnowflakeConfig();

  try {
    // Connect to Snowflake
    await connect(config);

    // Get all schemas
    const allSchemas = await getSchemas(config.database);

    // Filter out system schemas and dev/test schemas
    const filteredSchemas = allSchemas.filter((schema) => {
      const upper = schema.toUpperCase();
      // Exclude system schemas
      if (upper === "INFORMATION_SCHEMA" || upper === "PUBLIC") {
        return false;
      }
      // Exclude dev/test schemas
      if (upper.includes("_DEV") || upper.includes("_TEST")) {
        return false;
      }
      return true;
    });

    // Sort alphabetically
    filteredSchemas.sort();

    await disconnect();

    return NextResponse.json({
      database: config.database,
      schemas: filteredSchemas,
      totalCount: filteredSchemas.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json(
      { error: `Failed to fetch schemas: ${message}` },
      { status: 500 }
    );
  }
}

