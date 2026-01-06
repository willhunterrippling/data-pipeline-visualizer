import OpenAI from "openai";

let openai: OpenAI | null = null;

function getClient(): OpenAI {
  if (!openai) {
    openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
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
  // #region agent log
  fetch('http://127.0.0.1:7242/ingest/8b88715b-ceb9-4841-8612-e3ab766e87ab',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'client.ts:88',message:'complete() called',data:{model,hasApiKey:!!process.env.OPENAI_API_KEY,envModel:process.env.OPENAI_MODEL},timestamp:Date.now(),sessionId:'debug-session',hypothesisId:'B,C'})}).catch(()=>{});
  // #endregion

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

  const response = await client.chat.completions.create({
    model,
    messages: processedMessages.map((m) => ({
      role: m.role === "system" && isO1 ? "user" : m.role,
      content: m.content,
    })),
    max_completion_tokens: options?.maxTokens || 4096,
  });

  // #region agent log
  fetch('http://127.0.0.1:7242/ingest/8b88715b-ceb9-4841-8612-e3ab766e87ab',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'client.ts:120',message:'API response received',data:{hasChoices:!!response.choices,choicesLength:response.choices?.length,hasMessage:!!response.choices?.[0]?.message,contentLength:response.choices?.[0]?.message?.content?.length,contentPreview:response.choices?.[0]?.message?.content?.substring(0,200),finishReason:response.choices?.[0]?.finish_reason},timestamp:Date.now(),sessionId:'debug-session',hypothesisId:'A,D'})}).catch(()=>{});
  // #endregion

  // Track token usage
  if (response.usage) {
    sessionUsage.push({
      inputTokens: response.usage.prompt_tokens,
      outputTokens: response.usage.completion_tokens,
      model,
    });
  }

  return response.choices[0]?.message?.content || "";
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
  // #region agent log
  fetch('http://127.0.0.1:7242/ingest/8b88715b-ceb9-4841-8612-e3ab766e87ab',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'client.ts:147',message:'completeJson parsing',data:{responseLength:response.length,responsePreview:response.substring(0,300),jsonMatchFound:!!jsonMatch[1],jsonStrLength:jsonStr.length,jsonStrPreview:jsonStr.substring(0,200)},timestamp:Date.now(),sessionId:'debug-session',hypothesisId:'A,E'})}).catch(()=>{});
  // #endregion
  
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

