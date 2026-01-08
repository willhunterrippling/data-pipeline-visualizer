import OpenAI from "openai";

let openai: OpenAI | null = null;

// Default timeout for API calls (55 seconds - slightly less than typical serverless timeout)
const API_TIMEOUT_MS = 55000;

function getClient(): OpenAI {
  if (!openai) {
    openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      timeout: API_TIMEOUT_MS,
    });
  }
  return openai;
}

const DEFAULT_MODEL = "gpt-5.2";

// Model pricing per 1M tokens (USD)
const MODEL_PRICING: Record<string, { input: number; output: number }> = {
  "gpt-5.2": { input: 1.75, output: 14 },
  "gpt-4.1": { input: 2, output: 8 },
  "gpt-4.1-mini": { input: 0.4, output: 1.6 },
  "gpt-4.1-nano": { input: 0.1, output: 0.4 },
  "o1": { input: 15, output: 60 },
  "o3": { input: 10, output: 40 },
  "o3-mini": { input: 1.1, output: 4.4 },
  "o4-mini": { input: 1.1, output: 4.4 },
};

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  model: string;
}

export interface UsageStats {
  totalInputTokens: number;
  totalOutputTokens: number;
  totalCalls: number;
  estimatedCostUsd: number;
}

// Session-level usage tracking
let sessionUsage: TokenUsage[] = [];

export function resetUsageTracking(): void {
  sessionUsage = [];
}

export function getUsageStats(): UsageStats {
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let estimatedCostUsd = 0;

  for (const usage of sessionUsage) {
    totalInputTokens += usage.inputTokens;
    totalOutputTokens += usage.outputTokens;

    const pricing = MODEL_PRICING[usage.model] || MODEL_PRICING["gpt-5.2"];
    estimatedCostUsd += (usage.inputTokens / 1_000_000) * pricing.input;
    estimatedCostUsd += (usage.outputTokens / 1_000_000) * pricing.output;
  }

  return {
    totalInputTokens,
    totalOutputTokens,
    totalCalls: sessionUsage.length,
    estimatedCostUsd,
  };
}

export interface ChatMessage {
  role: "user" | "assistant" | "system";
  content: string;
}

/**
 * Send a completion request to OpenAI
 */
export async function complete(
  messages: ChatMessage[],
  options?: {
    model?: string;
    maxTokens?: number;
    temperature?: number;
  }
): Promise<string> {
  const client = getClient();
  const model = options?.model || process.env.OPENAI_MODEL || DEFAULT_MODEL;

  // o1 models don't support system messages, so we prepend to user message
  const isO1 = model.startsWith("o1") || model.startsWith("o3");
  
  let processedMessages = messages;
  if (isO1) {
    // Extract system message and prepend to first user message
    const systemMessage = messages.find((m) => m.role === "system");
    const otherMessages = messages.filter((m) => m.role !== "system");
    
    if (systemMessage && otherMessages.length > 0 && otherMessages[0].role === "user") {
      processedMessages = [
        {
          role: "user",
          content: `${systemMessage.content}\n\n${otherMessages[0].content}`,
        },
        ...otherMessages.slice(1),
      ];
    } else {
      processedMessages = otherMessages;
    }
  }

  try {
    const response = await client.chat.completions.create({
      model,
      messages: processedMessages.map((m) => ({
        role: m.role === "system" && isO1 ? "user" : m.role,
        content: m.content,
      })),
      max_completion_tokens: options?.maxTokens || 4096,
    });

    // Track token usage
    if (response.usage) {
      sessionUsage.push({
        inputTokens: response.usage.prompt_tokens,
        outputTokens: response.usage.completion_tokens,
        model,
      });
    }

    return response.choices[0]?.message?.content || "";
  } catch (error) {
    // Check if it's a timeout error
    if (error instanceof Error && (error.name === 'AbortError' || error.message.includes('timeout'))) {
      throw new Error(`OpenAI API request timed out after ${API_TIMEOUT_MS / 1000}s`);
    }
    throw error;
  }
}

/**
 * Generate a structured response with JSON output
 */
export async function completeJson<T>(
  messages: ChatMessage[],
  options?: {
    model?: string;
    maxTokens?: number;
  }
): Promise<T> {
  const response = await complete(messages, options);
  
  // Extract JSON from response (may be wrapped in markdown code blocks)
  const jsonMatch = response.match(/```(?:json)?\s*([\s\S]*?)```/) || [null, response];
  const jsonStr = jsonMatch[1]?.trim() || response.trim();
  
  try {
    return JSON.parse(jsonStr) as T;
  } catch {
    // Try to find JSON object in the response
    const objectMatch = jsonStr.match(/\{[\s\S]*\}/);
    if (objectMatch) {
      return JSON.parse(objectMatch[0]) as T;
    }
    throw new Error(`Failed to parse JSON response: ${jsonStr.substring(0, 200)}`);
  }
}

