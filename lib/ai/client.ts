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

const DEFAULT_MODEL = "o1";

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

  const response = await client.chat.completions.create({
    model,
    messages: processedMessages.map((m) => ({
      role: m.role === "system" && isO1 ? "user" : m.role,
      content: m.content,
    })),
    max_completion_tokens: options?.maxTokens || 4096,
  });

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

