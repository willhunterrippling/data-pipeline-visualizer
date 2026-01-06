/**
 * Simple OpenAI API test script
 * Run with: source .env.local && npx tsx scripts/test-openai.ts
 * Or: export $(cat .env.local | xargs) && npx tsx scripts/test-openai.ts
 */

import * as fs from "fs";
import OpenAI from "openai";

// Manually load .env.local
try {
  const envContent = fs.readFileSync(".env.local", "utf-8");
  for (const line of envContent.split("\n")) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith("#")) {
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx > 0) {
        const key = trimmed.substring(0, eqIdx);
        let value = trimmed.substring(eqIdx + 1);
        // Remove quotes if present
        if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
          value = value.slice(1, -1);
        }
        process.env[key] = value;
      }
    }
  }
} catch {
  console.log("Could not load .env.local, using existing env");
}

async function testOpenAI() {
  console.log("=== OpenAI API Test ===\n");

  // Check environment
  const apiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL || "gpt-5.2";

  console.log("API Key set:", !!apiKey);
  console.log("API Key prefix:", apiKey?.substring(0, 10) + "...");
  console.log("Model:", model);
  console.log("");

  if (!apiKey) {
    console.error("❌ OPENAI_API_KEY is not set!");
    process.exit(1);
  }

  const client = new OpenAI({ apiKey });

  try {
    console.log("Making test request...\n");

    const response = await client.chat.completions.create({
      model,
      messages: [
        { role: "user", content: "Reply with exactly: {\"test\": true}" },
      ],
      max_completion_tokens: 50,
    });

    console.log("✅ API call succeeded!\n");
    console.log("Response structure:");
    console.log("  - choices:", response.choices?.length);
    console.log("  - finish_reason:", response.choices?.[0]?.finish_reason);
    console.log("  - content length:", response.choices?.[0]?.message?.content?.length);
    console.log("  - content:", response.choices?.[0]?.message?.content);
    console.log("  - usage:", response.usage);
  } catch (error: unknown) {
    console.error("❌ API call failed!\n");
    console.error("Error type:", (error as Error).constructor.name);
    console.error("Error message:", (error as Error).message);
    if ((error as { status?: number }).status) {
      console.error("Status code:", (error as { status: number }).status);
    }
  }
}

testOpenAI();

