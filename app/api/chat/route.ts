import { NextRequest, NextResponse } from "next/server";
import { v4 as uuid } from "uuid";
import { runAgent, runAgentStream } from "@/lib/ai/chat/agent";
import type { ChatRequest, ChatResponse, ChatMessage, ChatContext, ChatStreamEvent } from "@/lib/types";

export const maxDuration = 60; // Allow up to 60 seconds for agent to complete

/**
 * Encode a ChatStreamEvent as a Server-Sent Event string.
 */
function encodeSSE(event: ChatStreamEvent): string {
  return `data: ${JSON.stringify(event)}\n\n`;
}

/**
 * POST /api/chat
 * 
 * Process a chat message through the pipeline agent.
 * 
 * Supports two modes:
 * 1. Streaming (Accept: text/event-stream) - Returns SSE stream with real-time events
 * 2. Non-streaming (default) - Returns JSON response after completion
 * 
 * Request body:
 * - message: string - The user's message
 * - conversationHistory: ChatMessage[] - Previous messages in the conversation
 * - context: ChatContext - Current state (anchor, flow)
 * - stream?: boolean - Force streaming mode (alternative to Accept header)
 * 
 * Response (non-streaming):
 * - response: AgentResponse - The agent's response with message, actions, and tool calls
 * - messageId: string - Unique ID for this response message
 * 
 * Response (streaming):
 * - Server-Sent Events stream with ChatStreamEvent objects
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as ChatRequest & { stream?: boolean };
    
    const { message, conversationHistory, context, stream: forceStream } = body;

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

    // Check if streaming is requested
    const acceptHeader = request.headers.get("accept") || "";
    const wantsStream = forceStream || acceptHeader.includes("text/event-stream");

    if (wantsStream) {
      // Streaming mode - return SSE stream
      const encoder = new TextEncoder();
      
      const stream = new ReadableStream({
        async start(controller) {
          let isClosed = false;
          
          const safeEnqueue = (data: Uint8Array) => {
            if (!isClosed) {
              try {
                controller.enqueue(data);
              } catch {
                isClosed = true;
              }
            }
          };
          
          const safeClose = () => {
            if (!isClosed) {
              try {
                controller.close();
                isClosed = true;
              } catch {
                isClosed = true;
              }
            }
          };
          
          try {
            const agentStream = runAgentStream(message, safeHistory, safeContext);
            
            for await (const event of agentStream) {
              const sseData = encodeSSE(event);
              safeEnqueue(encoder.encode(sseData));
              if (isClosed) break; // Stop iterating if stream was closed
            }
            
            safeClose();
          } catch (error) {
            console.error("Streaming error:", error);
            
            // Send error event before closing
            const errorEvent: ChatStreamEvent = {
              type: "error",
              error: error instanceof Error ? error.message : "An unknown error occurred",
            };
            safeEnqueue(encoder.encode(encodeSSE(errorEvent)));
            safeClose();
          }
        },
      });

      return new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          "Connection": "keep-alive",
        },
      });
    }

    // Non-streaming mode - return JSON response
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
