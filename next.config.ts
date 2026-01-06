import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  // Mark native modules as external for server-side only
  serverExternalPackages: ["snowflake-sdk", "better-sqlite3"],
};

export default nextConfig;
