import { NextRequest, NextResponse } from "next/server";
import { v4 as uuid } from "uuid";
import { runAgent } from "@/lib/ai/chat/agent";
import type { ChatRequest, ChatResponse, ChatMessage, ChatContext } from "@/lib/types";

export const maxDuration = 60; // Allow up to 60 seconds for agent to complete

/**
 * POST /api/chat
 * 
 * Process a chat message through the pipeline agent.
 * 
 * Request body:
 * - message: string - The user's message
 * - conversationHistory: ChatMessage[] - Previous messages in the conversation
 * - context: ChatContext - Current state (anchor, flow)
 * 
 * Response:
 * - response: AgentResponse - The agent's response with message, actions, and tool calls
 * - messageId: string - Unique ID for this response message
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as ChatRequest;
    
    const { message, conversationHistory, context } = body;

    // Validate required fields
    if (!message || typeof message !== "string") {
      return NextResponse.json(
        { error: "Message is required" },
        { status: 400 }
      );
    }

    // Validate conversation history
    if (conversationHistory && !Array.isArray(conversationHistory)) {
      return NextResponse.json(
        { error: "conversationHistory must be an array" },
        { status: 400 }
      );
    }

    // Ensure context has proper defaults
    const safeContext: ChatContext = {
      currentAnchorId: context?.currentAnchorId,
      currentFlowId: context?.currentFlowId,
      currentFlowName: context?.currentFlowName,
    };

    // Sanitize conversation history
    const safeHistory: ChatMessage[] = (conversationHistory || []).map((msg) => ({
      id: msg.id || uuid(),
      role: msg.role,
      content: msg.content,
      timestamp: msg.timestamp || new Date().toISOString(),
      actions: msg.actions,
      toolCalls: msg.toolCalls,
    }));

    // Run the agent
    const agentResponse = await runAgent(message, safeHistory, safeContext);

    const response: ChatResponse = {
      response: agentResponse,
      messageId: uuid(),
    };

    return NextResponse.json(response);
  } catch (error) {
    console.error("Chat API error:", error);

    // Handle specific error types
    if (error instanceof Error) {
      if (error.message.includes("timeout")) {
        return NextResponse.json(
          { error: "Request timed out. Please try a simpler question." },
          { status: 504 }
        );
      }

      if (error.message.includes("API key")) {
        return NextResponse.json(
          { error: "OpenAI API key not configured" },
          { status: 500 }
        );
      }
    }

    return NextResponse.json(
      { error: "Failed to process chat message" },
      { status: 500 }
    );
  }
}
