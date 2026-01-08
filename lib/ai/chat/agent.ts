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
  ChatStreamEvent,
} from "@/lib/types";
import { TOOL_DEFINITIONS, executeTool } from "./tools";
import { generateSystemPrompt, formatToolResult } from "./prompts";

// ============================================================================
// Configuration
// ============================================================================

const MAX_TOOL_CALLS = 15; // Prevent runaway loops, but allow enough for complex lineage questions
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
        function: (tc as { function: { name: string; arguments: string } }).function,
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

      const tcFunc = (toolCall as { function: { name: string; arguments: string } }).function;
      const toolName = tcFunc.name;
      let args: Record<string, unknown>;

      try {
        args = JSON.parse(tcFunc.arguments);
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

// ============================================================================
// Streaming Agent Implementation
// ============================================================================

/**
 * Get a human-readable description of what a tool does.
 */
function getToolDescription(toolName: string, args: Record<string, unknown>): string {
  switch (toolName) {
    case "searchNodes":
      return `Searching for "${args.query}"...`;
    case "getNodeDetails":
      return `Getting details for ${args.nodeId}...`;
    case "getUpstreamLineage":
      return `Tracing upstream sources of ${args.nodeId}...`;
    case "getDownstreamLineage":
      return `Finding downstream consumers of ${args.nodeId}...`;
    case "findFlowsContaining":
      return `Finding flows containing ${args.nodeId}...`;
    case "getFlowDetails":
      return `Getting flow details for ${args.flowId}...`;
    case "listFlows":
      return "Listing all available flows...";
    case "searchByColumn":
      return `Searching for column "${args.columnName}"...`;
    case "getGraphStats":
      return "Getting pipeline statistics...";
    case "searchSqlContent":
      return `Searching SQL code for "${args.pattern}"...`;
    default:
      return `Running ${toolName}...`;
  }
}

/**
 * Generate a brief reasoning/planning explanation before executing tools.
 * This gives the user insight into what the agent is thinking.
 */
async function* generateReasoning(
  client: OpenAI,
  model: string,
  userQuestion: string,
  toolCalls: Array<{ name: string; args: Record<string, unknown> }>,
  phase: "planning" | "reflecting" = "planning"
): AsyncGenerator<ChatStreamEvent, void, unknown> {
  // Build a description of what tools are being called
  const toolDescriptions = toolCalls.map((tc) => {
    switch (tc.name) {
      case "searchNodes":
        return `search for "${tc.args.query}"`;
      case "getNodeDetails":
        return `get details about ${tc.args.nodeId}`;
      case "getUpstreamLineage":
        return `trace upstream data sources`;
      case "getDownstreamLineage":
        return `find downstream consumers`;
      case "findFlowsContaining":
        return `find related flows`;
      case "getFlowDetails":
        return `get flow information`;
      case "listFlows":
        return `list available flows`;
      case "searchByColumn":
        return `search for column "${tc.args.columnName}"`;
      case "getGraphStats":
        return `get pipeline statistics`;
      case "searchSqlContent":
        return `search SQL code for "${tc.args.pattern}"`;
      default:
        return tc.name;
    }
  }).join(", ");

  let reasoningPrompt: string;
  
  if (phase === "planning") {
    reasoningPrompt = `You are explaining your thought process to a user. They asked: "${userQuestion}"

You've decided to: ${toolDescriptions}

In 1-2 SHORT sentences, explain your reasoning - what you're looking for and why. Be conversational and specific. Don't use phrases like "I will" or "I'm going to" - instead describe what you're thinking about or investigating.

Example good responses:
- "Looking for tables related to leads to understand the data flow. I'll trace the lineage to see where this data originates."
- "The no_longer_at_account field could be in several places - checking the mart layer first since that's where business logic typically lives."
- "Searching for the mech outreach pipeline to identify its data sources and dependencies."`;
  } else {
    reasoningPrompt = `You are explaining your thought process to a user. They asked: "${userQuestion}"

You just gathered more information by: ${toolDescriptions}

In 1-2 SHORT sentences, explain what you're learning and what you're investigating next. Be conversational. Focus on what the new information tells you.

Example good responses:
- "Found the staging table - now tracing back to see where this data originates from."
- "The mart model references several upstream tables. Checking the transformation logic to understand how the field is calculated."
- "These results show a connection to Salesforce data. Looking at the source definition to confirm."`;
  }

  try {
    const stream = await client.chat.completions.create({
      model,
      messages: [{ role: "user", content: reasoningPrompt }],
      max_tokens: 150,
      stream: true,
    });

    let fullReasoning = "";
    
    for await (const chunk of stream) {
      const delta = chunk.choices[0]?.delta?.content || "";
      if (delta) {
        fullReasoning += delta;
        yield {
          type: "reasoning",
          content: fullReasoning,
          isPartial: true,
        };
      }
    }

    // Yield final reasoning
    if (fullReasoning) {
      yield {
        type: "reasoning",
        content: fullReasoning.trim(),
        isPartial: false,
      };
    }
  } catch (error) {
    // If reasoning fails, just continue without it
    console.error("Failed to generate reasoning:", error);
  }
}

/**
 * Generate a reflection after tool execution to evaluate if we can answer the question.
 * Returns the reflection text which will be streamed to the user.
 */
async function* generateReflection(
  client: OpenAI,
  model: string,
  userQuestion: string,
  toolResults: ToolCallLog[]
): AsyncGenerator<ChatStreamEvent, void, unknown> {
  // Summarize what tools were used and what was found
  const resultsSummary = toolResults.map((tc) => {
    const toolName = tc.toolName;
    const result = tc.result;
    switch (toolName) {
      case "searchNodes":
        return `Searched and found: ${result}`;
      case "getNodeDetails":
        return `Got details: ${result}`;
      case "getUpstreamLineage":
        return `Traced upstream: ${result}`;
      case "getDownstreamLineage":
        return `Found downstream: ${result}`;
      default:
        return `${toolName}: ${result}`;
    }
  }).join("\n");

  const reflectionPrompt = `You are a data pipeline expert reflecting on your investigation. 

User's question: "${userQuestion}"

What you found:
${resultsSummary}

In 1-2 SHORT sentences, summarize what you learned and whether you have enough information to answer the question confidently. If you need more information, briefly mention what's missing.

Be conversational. Examples:
- "Found the source table and traced the lineage. I can now explain how the data flows."
- "Located the field in the mart layer, but I should check the upstream transformation to understand how it's calculated."
- "The search returned multiple matches. I need to look at a specific model to give a complete answer."`;

  try {
    const stream = await client.chat.completions.create({
      model,
      messages: [{ role: "user", content: reflectionPrompt }],
      max_tokens: 150,
      stream: true,
    });

    let fullReflection = "";
    
    for await (const chunk of stream) {
      const delta = chunk.choices[0]?.delta?.content || "";
      if (delta) {
        fullReflection += delta;
        yield {
          type: "reasoning",
          content: fullReflection,
          isPartial: true,
        };
      }
    }

    // Yield final reflection
    if (fullReflection) {
      yield {
        type: "reasoning",
        content: fullReflection.trim(),
        isPartial: false,
      };
    }
  } catch (error) {
    console.error("Failed to generate reflection:", error);
  }
}

/**
 * Run the agent loop with streaming events.
 * Yields ChatStreamEvent objects as the agent processes.
 */
export async function* runAgentStream(
  message: string,
  conversationHistory: ChatMessage[],
  context: ChatContext
): AsyncGenerator<ChatStreamEvent, void, unknown> {
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
  let isFirstToolBatch = true;

  // Initial thinking event
  yield {
    type: "thinking",
    message: "Analyzing your question...",
  };

  try {
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
          function: (tc as { function: { name: string; arguments: string } }).function,
        })),
      });

      // If no tool calls, we're done - stream the final response
      if (!assistantMessage.tool_calls || assistantMessage.tool_calls.length === 0) {
        const content = assistantMessage.content || "";
        const { cleanContent, actions } = parseActions(content);

        // Yield the final message
        yield {
          type: "message",
          content: cleanContent,
          isPartial: false,
        };

        // Yield actions if any
        if (actions.length > 0) {
          yield {
            type: "actions",
            actions,
          };
        }

        // Done!
        yield {
          type: "done",
          toolCalls: toolCallLogs,
        };

        return;
      }

      // Parse all tool calls first
      const parsedToolCalls = assistantMessage.tool_calls.map((toolCall) => {
        const tcFunc = (toolCall as { function: { name: string; arguments: string } }).function;
        let args: Record<string, unknown>;
        try {
          args = JSON.parse(tcFunc.arguments);
        } catch {
          args = {};
        }
        return {
          id: toolCall.id,
          name: tcFunc.name,
          args,
        };
      });

      // Generate reasoning before tool calls
      // First batch: explain what we're planning to do
      // Subsequent batches: explain what we learned and what we're investigating next
      if (isFirstToolBatch) {
        isFirstToolBatch = false;
        yield* generateReasoning(client, model, message, parsedToolCalls, "planning");
      } else {
        // For subsequent batches, explain what we're doing next
        yield* generateReasoning(client, model, message, parsedToolCalls, "reflecting");
      }

      // Track tool calls for this batch (for reflection)
      const batchToolLogs: ToolCallLog[] = [];

      // Execute tool calls with streaming events
      for (const toolCall of parsedToolCalls) {
        totalToolCalls++;

        const toolName = toolCall.name;
        const args = toolCall.args;
        const toolCallId = toolCall.id;

        // Yield tool start event
        yield {
          type: "tool_start",
          toolName,
          args,
          toolCallId,
        };

        const startTime = Date.now();
        let result: unknown;
        let resultStr: string;

        try {
          result = executeTool(toolName, args);
          resultStr = formatToolResult(toolName, result);
        } catch (error) {
          result = null;
          resultStr = `Tool "${toolName}" failed: ${error instanceof Error ? error.message : "Unknown error"}`;
        }

        const durationMs = Date.now() - startTime;
        const summarizedResult = summarizeToolResult(result);

        const toolLog: ToolCallLog = {
          toolName,
          args,
          result: summarizedResult,
          durationMs,
        };

        // Log the tool call
        toolCallLogs.push(toolLog);
        batchToolLogs.push(toolLog);

        // Yield tool result event
        yield {
          type: "tool_result",
          toolCallId,
          toolName,
          result: summarizedResult,
          durationMs,
        };

        // Add tool result to messages
        messages.push({
          role: "tool",
          tool_call_id: toolCallId,
          content: resultStr,
        });
      }

      // After processing tools, generate reflection on what we learned
      // This helps the model (and user) understand if we have enough info
      yield {
        type: "thinking",
        message: "Analyzing results...",
      };
      
      yield* generateReflection(client, model, message, batchToolLogs);
    }

    // Hit max tool calls - generate a final answer using all gathered context
    
    try {
      // Force the model to generate a final response without more tool calls
      const finalResponse = await client.chat.completions.create({
        model,
        messages: [
          ...messages as OpenAI.Chat.Completions.ChatCompletionMessageParam[],
          {
            role: "user",
            content: "You've gathered enough information. Please provide your final answer now based on everything you've found. Do not request more tool calls.",
          },
        ],
        max_tokens: 4096,
        stream: true,
      });
      
      let fullMessage = "";
      for await (const chunk of finalResponse) {
        const delta = chunk.choices[0]?.delta?.content || "";
        if (delta) {
          fullMessage += delta;
          yield {
            type: "message",
            content: fullMessage,
            isPartial: true,
          };
        }
      }
      
      // Yield final complete message
      const { cleanContent, actions } = parseActions(fullMessage);
      yield {
        type: "message",
        content: cleanContent,
        isPartial: false,
      };
      
      if (actions.length > 0) {
        yield {
          type: "actions",
          actions,
        };
      }
    } catch (finalError) {
      // If final synthesis fails, fall back to generic message
      yield {
        type: "message",
        content: "I've gathered a lot of information but reached my limit for tool calls. Unfortunately, I couldn't synthesize a final answer. Please try asking a more specific question.",
        isPartial: false,
      };
    }

    yield {
      type: "done",
      toolCalls: toolCallLogs,
    };
  } catch (error) {
    // Yield error event
    yield {
      type: "error",
      error: error instanceof Error ? error.message : "An unknown error occurred",
    };
  }
}
