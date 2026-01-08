/**
 * Agent loop implementation for the Pipeline Chat feature.
 * Uses OpenAI function calling to execute tools and gather context.
 */

import OpenAI from "openai";
import type {
  ChatMessage,
  ChatContext,
  AgentResponse,
  ProposedAction,
  ToolCallLog,
} from "@/lib/types";
import { TOOL_DEFINITIONS, executeTool } from "./tools";
import { generateSystemPrompt, formatToolResult } from "./prompts";

// ============================================================================
// Configuration
// ============================================================================

const MAX_TOOL_CALLS = 10; // Prevent runaway loops
const API_TIMEOUT_MS = 55000;
const DEFAULT_MODEL = "gpt-4.1"; // Use a model that supports function calling well

let openaiClient: OpenAI | null = null;

function getClient(): OpenAI {
  if (!openaiClient) {
    openaiClient = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      timeout: API_TIMEOUT_MS,
    });
  }
  return openaiClient;
}

// ============================================================================
// Types
// ============================================================================

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null;
  tool_calls?: Array<{
    id: string;
    type: "function";
    function: {
      name: string;
      arguments: string;
    };
  }>;
  tool_call_id?: string;
}

// ============================================================================
// Agent Implementation
// ============================================================================

/**
 * Parse actions from the assistant's response.
 * Actions are embedded as a JSON block at the end of the message.
 */
function parseActions(content: string): {
  cleanContent: string;
  actions: ProposedAction[];
} {
  // Look for actions block at the end of the message
  const actionsMatch = content.match(/```actions\s*([\s\S]*?)```\s*$/);

  if (!actionsMatch) {
    return { cleanContent: content, actions: [] };
  }

  const cleanContent = content.replace(/```actions\s*[\s\S]*?```\s*$/, "").trim();

  try {
    const actions = JSON.parse(actionsMatch[1].trim()) as ProposedAction[];
    return { cleanContent, actions };
  } catch {
    console.error("Failed to parse actions from response");
    return { cleanContent: content, actions: [] };
  }
}

/**
 * Convert our ChatMessage format to OpenAI message format.
 */
function convertToOpenAIMessages(
  systemPrompt: string,
  conversationHistory: ChatMessage[],
  currentMessage: string
): OpenAIMessage[] {
  const messages: OpenAIMessage[] = [
    { role: "system", content: systemPrompt },
  ];

  // Add conversation history (only user and assistant messages)
  for (const msg of conversationHistory) {
    if (msg.role === "user" || msg.role === "assistant") {
      messages.push({
        role: msg.role,
        content: msg.content,
      });
    }
  }

  // Add current user message
  messages.push({
    role: "user",
    content: currentMessage,
  });

  return messages;
}

/**
 * Run the agent loop with tool execution.
 */
export async function runAgent(
  message: string,
  conversationHistory: ChatMessage[],
  context: ChatContext
): Promise<AgentResponse> {
  const client = getClient();
  const model = process.env.OPENAI_CHAT_MODEL || DEFAULT_MODEL;
  const systemPrompt = generateSystemPrompt(context);

  const messages = convertToOpenAIMessages(
    systemPrompt,
    conversationHistory,
    message
  );

  const toolCallLogs: ToolCallLog[] = [];
  let totalToolCalls = 0;

  // Agent loop - keep calling until we get a final response or hit the limit
  while (totalToolCalls < MAX_TOOL_CALLS) {
    const response = await client.chat.completions.create({
      model,
      messages: messages as OpenAI.Chat.Completions.ChatCompletionMessageParam[],
      tools: TOOL_DEFINITIONS,
      tool_choice: "auto",
      max_tokens: 4096,
    });

    const choice = response.choices[0];
    const assistantMessage = choice.message;

    // Add assistant message to conversation
    messages.push({
      role: "assistant",
      content: assistantMessage.content,
      tool_calls: assistantMessage.tool_calls?.map((tc) => ({
        id: tc.id,
        type: tc.type as "function",
        function: tc.function,
      })),
    });

    // If no tool calls, we're done
    if (!assistantMessage.tool_calls || assistantMessage.tool_calls.length === 0) {
      const content = assistantMessage.content || "";
      const { cleanContent, actions } = parseActions(content);

      return {
        message: cleanContent,
        actions: actions.length > 0 ? actions : undefined,
        toolCalls: toolCallLogs.length > 0 ? toolCallLogs : undefined,
      };
    }

    // Execute tool calls
    for (const toolCall of assistantMessage.tool_calls) {
      totalToolCalls++;

      const toolName = toolCall.function.name;
      let args: Record<string, unknown>;

      try {
        args = JSON.parse(toolCall.function.arguments);
      } catch {
        args = {};
      }

      const startTime = Date.now();
      let result: unknown;
      let resultStr: string;

      try {
        result = executeTool(toolName, args);
        resultStr = formatToolResult(toolName, result);
      } catch (error) {
        resultStr = `Tool "${toolName}" failed: ${error instanceof Error ? error.message : "Unknown error"}`;
      }

      const durationMs = Date.now() - startTime;

      // Log the tool call
      toolCallLogs.push({
        toolName,
        args,
        result: summarizeToolResult(result),
        durationMs,
      });

      // Add tool result to messages
      messages.push({
        role: "tool",
        tool_call_id: toolCall.id,
        content: resultStr,
      });
    }
  }

  // Hit max tool calls - return what we have
  return {
    message:
      "I've gathered a lot of information but reached my limit for tool calls. Based on what I've found so far, let me provide what I can.",
    toolCalls: toolCallLogs,
  };
}

/**
 * Summarize a tool result for the tool call log.
 */
function summarizeToolResult(result: unknown): string {
  if (result === null || result === undefined) {
    return "No results";
  }

  if (Array.isArray(result)) {
    if (result.length === 0) {
      return "Empty array";
    }
    return `${result.length} results`;
  }

  if (typeof result === "object") {
    const keys = Object.keys(result as object);
    if (keys.length === 0) {
      return "Empty object";
    }
    // For node details, show the name
    if ("name" in (result as object)) {
      return `Found: ${(result as { name: string }).name}`;
    }
    return `Object with ${keys.length} properties`;
  }

  return String(result).substring(0, 100);
}

/**
 * Simple completion without tools (for clarifying questions).
 */
export async function simpleComplete(
  message: string,
  conversationHistory: ChatMessage[],
  context: ChatContext
): Promise<string> {
  const client = getClient();
  const model = process.env.OPENAI_CHAT_MODEL || DEFAULT_MODEL;
  const systemPrompt = generateSystemPrompt(context);

  const messages = convertToOpenAIMessages(
    systemPrompt,
    conversationHistory,
    message
  );

  const response = await client.chat.completions.create({
    model,
    messages: messages as OpenAI.Chat.Completions.ChatCompletionMessageParam[],
    max_tokens: 2048,
  });

  return response.choices[0]?.message?.content || "";
}
