"use client";

import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { v4 as uuid } from "uuid";
import type {
  ChatMessage,
  ChatContext,
  ProposedAction,
  GraphNode,
  ToolCallLog,
  ChatStreamEvent,
} from "@/lib/types";

// ============================================================================
// Types
// ============================================================================

interface PipelineChatProps {
  context: ChatContext;
  onAction: (action: ProposedAction) => void;
  allNodes?: GraphNode[];
}

// Timeline item - either reasoning text or a tool call
type TimelineItem = 
  | { type: "reasoning"; content: string }
  | { type: "tool"; toolCall: ToolCallLog };

// Streaming state for the current assistant response
interface StreamingState {
  isStreaming: boolean;
  thinkingMessage: string;
  // Current reasoning being streamed
  currentReasoning: string;
  currentReasoningComplete: boolean;
  // Timeline of completed items (reasoning + tool calls interleaved)
  timeline: TimelineItem[];
  // For tracking pending tool calls
  pendingToolCalls: Map<string, { toolName: string; args: Record<string, unknown>; startTime: number }>;
  messageContent: string;
  actions: ProposedAction[];
}

// Extended message type to include timeline
interface ExtendedChatMessage extends ChatMessage {
  timeline?: TimelineItem[];
}

// ============================================================================
// Markdown Content Renderer
// ============================================================================

interface MarkdownContentProps {
  content: string;
  allNodes?: GraphNode[];
  onNodeClick?: (nodeId: string) => void;
}

function MarkdownContent({ content, allNodes, onNodeClick }: MarkdownContentProps) {
  const rendered = useMemo(() => {
    const paragraphs = content.split(/\n\n+/);

    return paragraphs.map((paragraph, pIdx) => {
      const processInline = (text: string): React.ReactNode[] => {
        const parts: React.ReactNode[] = [];
        let key = 0;
        const pattern = /(\*\*(.+?)\*\*|\*(.+?)\*|`([^`]+)`)/g;
        let lastIndex = 0;
        let match;

        while ((match = pattern.exec(text)) !== null) {
          if (match.index > lastIndex) {
            parts.push(text.slice(lastIndex, match.index));
          }

          if (match[2]) {
            parts.push(
              <strong key={key++} className="font-semibold text-white">
                {match[2]}
              </strong>
            );
          } else if (match[3]) {
            parts.push(
              <em key={key++} className="italic">
                {match[3]}
              </em>
            );
          } else if (match[4]) {
            const codeText = match[4];
            const matchingNode = allNodes?.find(
              (n) => n.name === codeText || n.id === codeText
            );

            if (matchingNode && onNodeClick) {
              parts.push(
                <button
                  key={key++}
                  onClick={() => onNodeClick(matchingNode.id)}
                  className="px-1.5 py-0.5 rounded bg-emerald-500/20 font-mono text-xs text-emerald-300 hover:bg-emerald-500/30 cursor-pointer transition-colors"
                >
                  {codeText}
                </button>
              );
            } else {
              parts.push(
                <code
                  key={key++}
                  className="px-1.5 py-0.5 rounded bg-white/10 font-mono text-xs text-cyan-300"
                >
                  {codeText}
                </code>
              );
            }
          }

          lastIndex = pattern.lastIndex;
        }

        if (lastIndex < text.length) {
          parts.push(text.slice(lastIndex));
        }

        return parts.length > 0 ? parts : [text];
      };

      if (paragraph.match(/^[-*]\s/m)) {
        const items = paragraph.split(/\n/).filter((line) => line.trim());
        return (
          <ul key={pIdx} className="list-disc list-inside space-y-1 mb-3">
            {items.map((item, iIdx) => (
              <li key={iIdx} className="text-white/80">
                {processInline(item.replace(/^[-*]\s+/, ""))}
              </li>
            ))}
          </ul>
        );
      }

      const lines = paragraph.split(/\n/);
      return (
        <p key={pIdx} className="mb-3 last:mb-0 text-white/80 leading-relaxed">
          {lines.map((line, lIdx) => (
            <span key={lIdx}>
              {processInline(line)}
              {lIdx < lines.length - 1 && <br />}
            </span>
          ))}
        </p>
      );
    });
  }, [content, allNodes, onNodeClick]);

  return <div className="text-sm">{rendered}</div>;
}

// ============================================================================
// Action Button Component
// ============================================================================

interface ActionButtonProps {
  action: ProposedAction;
  onClick: () => void;
}

function ActionButton({ action, onClick }: ActionButtonProps) {
  const getIcon = () => {
    switch (action.type) {
      case "navigate_to_node":
        return (
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
          </svg>
        );
      case "set_anchor":
        return (
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122" />
          </svg>
        );
      case "select_flow":
        return (
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
          </svg>
        );
      case "create_flow":
        return (
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
        );
      default:
        return null;
    }
  };

  const getColorClass = () => {
    switch (action.type) {
      case "set_anchor":
        return "bg-purple-500/20 hover:bg-purple-500/30 border-purple-500/30 text-purple-300";
      case "select_flow":
        return "bg-cyan-500/20 hover:bg-cyan-500/30 border-cyan-500/30 text-cyan-300";
      case "create_flow":
        return "bg-emerald-500/20 hover:bg-emerald-500/30 border-emerald-500/30 text-emerald-300";
      default:
        return "bg-white/10 hover:bg-white/20 border-white/20 text-white/80";
    }
  };

  return (
    <button
      onClick={onClick}
      className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm transition-colors ${getColorClass()}`}
    >
      {getIcon()}
      {action.label}
    </button>
  );
}

// ============================================================================
// Tool Display Helpers
// ============================================================================

function getToolDisplayName(toolName: string): string {
  switch (toolName) {
    case "searchNodes": return "Search";
    case "getNodeDetails": return "Read";
    case "getUpstreamLineage": return "Upstream";
    case "getDownstreamLineage": return "Downstream";
    case "findFlowsContaining": return "Find Flows";
    case "getFlowDetails": return "Flow Details";
    case "listFlows": return "List Flows";
    case "searchByColumn": return "Column Search";
    case "getGraphStats": return "Stats";
    default: return toolName;
  }
}

function getToolTarget(toolCall: ToolCallLog): string {
  const { toolName, args } = toolCall;
  switch (toolName) {
    case "searchNodes":
      return `"${args.query}"`;
    case "getNodeDetails":
    case "getUpstreamLineage":
    case "getDownstreamLineage":
    case "findFlowsContaining": {
      const nodeId = args.nodeId as string;
      // Extract just the model name from full ID
      const parts = nodeId.split(".");
      return parts[parts.length - 1] || nodeId;
    }
    case "getFlowDetails":
      return args.flowId as string;
    case "searchByColumn":
      return `"${args.columnName}"`;
    default:
      return "";
  }
}

// ============================================================================
// Collapsible Section Component (Cursor-like)
// ============================================================================

interface CollapsibleSectionProps {
  title: string;
  icon: React.ReactNode;
  isExpanded: boolean;
  onToggle: () => void;
  children: React.ReactNode;
  isStreaming?: boolean;
  badge?: string;
}

function CollapsibleSection({ 
  title, 
  icon, 
  isExpanded, 
  onToggle, 
  children, 
  isStreaming,
  badge 
}: CollapsibleSectionProps) {
  return (
    <div className="mb-2">
      <button
        onClick={onToggle}
        className="flex items-center gap-2 text-xs text-white/50 hover:text-white/70 transition-colors w-full text-left py-1"
      >
        <svg 
          className={`w-3 h-3 transition-transform duration-200 ${isExpanded ? "rotate-90" : ""}`} 
          fill="none" 
          viewBox="0 0 24 24" 
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        {icon}
        <span className="font-medium">{title}</span>
        {badge && (
          <span className="text-white/30">{badge}</span>
        )}
        {isStreaming && (
          <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse ml-1" />
        )}
      </button>
      
      <div 
        className={`overflow-hidden transition-all duration-200 ${
          isExpanded ? "max-h-[500px] opacity-100" : "max-h-0 opacity-0"
        }`}
      >
        <div className="pl-5 pt-1">
          {children}
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Thinking Timeline (Cursor-like - interleaved reasoning + tool calls)
// ============================================================================

interface ThinkingTimelineProps {
  timeline: TimelineItem[];
  currentReasoning: string;
  isStreaming: boolean;
  defaultExpanded?: boolean;
}

function ThinkingTimeline({ 
  timeline, 
  currentReasoning, 
  isStreaming, 
  defaultExpanded = true 
}: ThinkingTimelineProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);

  // Count tool calls for the title
  const toolCount = timeline.filter(item => item.type === "tool").length;

  // Auto-collapse when streaming completes
  useEffect(() => {
    if (!isStreaming && !currentReasoning && timeline.length > 0) {
      const timer = setTimeout(() => setIsExpanded(false), 800);
      return () => clearTimeout(timer);
    }
  }, [isStreaming, currentReasoning, timeline.length]);

  if (timeline.length === 0 && !currentReasoning && !isStreaming) return null;

  // Build title based on content
  const title = toolCount > 0 
    ? `Thinking · ${toolCount} ${toolCount === 1 ? "tool" : "tools"} used`
    : "Thinking";

  return (
    <CollapsibleSection
      title={title}
      icon={
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
        </svg>
      }
      isExpanded={isExpanded}
      onToggle={() => setIsExpanded(!isExpanded)}
      isStreaming={isStreaming}
    >
      <div className="space-y-1.5">
        {timeline.map((item, idx) => (
          <div key={idx}>
            {item.type === "reasoning" ? (
              <p className="text-xs text-white/60 leading-relaxed py-0.5">
                {item.content}
              </p>
            ) : (
              <div className="flex items-center gap-2 text-xs py-0.5 text-white/50 bg-white/5 rounded px-2 -mx-2">
                <span className="text-amber-400/70 font-medium min-w-[70px]">
                  {getToolDisplayName(item.toolCall.toolName)}
                </span>
                <span className="text-white/40 truncate flex-1">
                  {getToolTarget(item.toolCall)}
                </span>
                <span className="text-emerald-400/60 flex-shrink-0">
                  {item.toolCall.result}
                </span>
                <span className="text-white/20 flex-shrink-0">
                  {item.toolCall.durationMs}ms
                </span>
              </div>
            )}
          </div>
        ))}
        
        {/* Current reasoning being streamed */}
        {currentReasoning && (
          <p className="text-xs text-white/60 leading-relaxed py-0.5">
            {currentReasoning}
            {isStreaming && <span className="animate-pulse">▊</span>}
          </p>
        )}
        
        {/* Initial state - no timeline yet */}
        {timeline.length === 0 && !currentReasoning && isStreaming && (
          <p className="text-xs text-white/60 leading-relaxed py-0.5">
            Analyzing your question...<span className="animate-pulse">▊</span>
          </p>
        )}
      </div>
    </CollapsibleSection>
  );
}

// ============================================================================
// Streaming Indicator
// ============================================================================

interface StreamingIndicatorProps {
  message: string;
}

function StreamingIndicator({ message }: StreamingIndicatorProps) {
  return (
    <div className="flex items-center gap-2 text-white/50 py-2">
      <div className="w-3 h-3 border-2 border-white/20 border-t-cyan-400 rounded-full animate-spin" />
      <span className="text-xs">{message}</span>
    </div>
  );
}

// ============================================================================
// Message Component
// ============================================================================

interface MessageProps {
  message: ExtendedChatMessage;
  allNodes?: GraphNode[];
  onAction: (action: ProposedAction) => void;
  onNodeClick?: (nodeId: string) => void;
}

function Message({ message, allNodes, onAction, onNodeClick }: MessageProps) {
  const isUser = message.role === "user";
  const [timelineExpanded, setTimelineExpanded] = useState(false);

  if (isUser) {
    return (
      <div className="flex justify-end">
        <div className="max-w-[85%] rounded-2xl px-4 py-3 bg-cyan-600/30 text-white">
          <MarkdownContent content={message.content} allNodes={allNodes} onNodeClick={onNodeClick} />
        </div>
      </div>
    );
  }

  // Count tools in timeline
  const toolCount = message.timeline?.filter(item => item.type === "tool").length || 0;
  const hasTimeline = message.timeline && message.timeline.length > 0;

  return (
    <div className="flex justify-start">
      <div className="max-w-[85%] rounded-2xl px-4 py-3 bg-white/5 border border-white/10">
        {/* Thinking timeline (interleaved reasoning + tools) */}
        {hasTimeline && (
          <CollapsibleSection
            title={toolCount > 0 ? `Thinking · ${toolCount} ${toolCount === 1 ? "tool" : "tools"} used` : "Thinking"}
            icon={
              <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
            }
            isExpanded={timelineExpanded}
            onToggle={() => setTimelineExpanded(!timelineExpanded)}
          >
            <div className="space-y-1.5">
              {message.timeline!.map((item, idx) => (
                <div key={idx}>
                  {item.type === "reasoning" ? (
                    <p className="text-xs text-white/60 leading-relaxed py-0.5">
                      {item.content}
                    </p>
                  ) : (
                    <div className="flex items-center gap-2 text-xs py-0.5 text-white/50 bg-white/5 rounded px-2 -mx-2">
                      <span className="text-amber-400/70 font-medium min-w-[70px]">
                        {getToolDisplayName(item.toolCall.toolName)}
                      </span>
                      <span className="text-white/40 truncate flex-1">
                        {getToolTarget(item.toolCall)}
                      </span>
                      <span className="text-emerald-400/60 flex-shrink-0">
                        {item.toolCall.result}
                      </span>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </CollapsibleSection>
        )}

        {/* Message content */}
        <MarkdownContent content={message.content} allNodes={allNodes} onNodeClick={onNodeClick} />

        {/* Action buttons */}
        {message.actions && message.actions.length > 0 && (
          <div className="mt-3 pt-3 border-t border-white/10 flex flex-wrap gap-2">
            {message.actions.map((action, idx) => (
              <ActionButton key={idx} action={action} onClick={() => onAction(action)} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// Streaming Message Component
// ============================================================================

interface StreamingMessageProps {
  streamingState: StreamingState;
  allNodes?: GraphNode[];
  onNodeClick?: (nodeId: string) => void;
}

function StreamingMessage({ streamingState, allNodes, onNodeClick }: StreamingMessageProps) {
  const { 
    thinkingMessage, 
    currentReasoning,
    currentReasoningComplete,
    timeline, 
    messageContent 
  } = streamingState;

  const hasTimeline = timeline.length > 0 || currentReasoning;
  const showTimeline = hasTimeline || (!currentReasoningComplete);
  const showIndicator = !messageContent && !hasTimeline && timeline.length === 0;

  return (
    <div className="flex justify-start">
      <div className="max-w-[85%] rounded-2xl px-4 py-3 bg-white/5 border border-white/10">
        {/* Interleaved thinking timeline */}
        {showTimeline && (
          <ThinkingTimeline 
            timeline={timeline}
            currentReasoning={currentReasoning}
            isStreaming={!messageContent}
          />
        )}

        {/* Loading indicator when nothing else to show */}
        {showIndicator && (
          <StreamingIndicator message={thinkingMessage || "Thinking..."} />
        )}

        {/* Message content */}
        {messageContent && (
          <MarkdownContent content={messageContent} allNodes={allNodes} onNodeClick={onNodeClick} />
        )}
      </div>
    </div>
  );
}

// ============================================================================
// Main Chat Component
// ============================================================================

export default function PipelineChat({
  context,
  onAction,
  allNodes,
}: PipelineChatProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<ExtendedChatMessage[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  
  const [streamingState, setStreamingState] = useState<StreamingState>({
    isStreaming: false,
    thinkingMessage: "",
    currentReasoning: "",
    currentReasoningComplete: false,
    timeline: [],
    pendingToolCalls: new Map(),
    messageContent: "",
    actions: [],
  });

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, streamingState.timeline, streamingState.messageContent, streamingState.currentReasoning]);

  useEffect(() => {
    if (isOpen) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [isOpen]);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        return;
      }
      if (e.key === "?" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setIsOpen((prev) => !prev);
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const processStreamEvent = useCallback((event: ChatStreamEvent) => {
    switch (event.type) {
      case "thinking":
        setStreamingState((prev) => ({
          ...prev,
          thinkingMessage: event.message,
        }));
        break;

      case "reasoning":
        setStreamingState((prev) => {
          // If we're starting a new reasoning phase and have a completed one, save to timeline
          if (event.isPartial && prev.currentReasoning && prev.currentReasoningComplete) {
            return {
              ...prev,
              timeline: [...prev.timeline, { type: "reasoning", content: prev.currentReasoning }],
              currentReasoning: event.content,
              currentReasoningComplete: false,
            };
          }
          
          return {
            ...prev,
            currentReasoning: event.content,
            currentReasoningComplete: !event.isPartial,
          };
        });
        break;

      case "tool_start":
        setStreamingState((prev) => {
          const newPending = new Map(prev.pendingToolCalls);
          newPending.set(event.toolCallId, {
            toolName: event.toolName,
            args: event.args,
            startTime: Date.now(),
          });
          return {
            ...prev,
            pendingToolCalls: newPending,
          };
        });
        break;

      case "tool_result":
        setStreamingState((prev) => {
          const newPending = new Map(prev.pendingToolCalls);
          newPending.delete(event.toolCallId);
          
          const newToolCall: ToolCallLog = {
            toolName: event.toolName,
            args: prev.pendingToolCalls.get(event.toolCallId)?.args || {},
            result: event.result,
            durationMs: event.durationMs,
          };
          
          // First, finalize any current reasoning before adding tool
          let newTimeline = prev.timeline;
          if (prev.currentReasoning && prev.currentReasoningComplete) {
            newTimeline = [...newTimeline, { type: "reasoning" as const, content: prev.currentReasoning }];
          }
          
          // Add tool call to timeline
          newTimeline = [...newTimeline, { type: "tool" as const, toolCall: newToolCall }];
          
          return {
            ...prev,
            pendingToolCalls: newPending,
            timeline: newTimeline,
            // Clear reasoning once added to timeline
            currentReasoning: prev.currentReasoningComplete ? "" : prev.currentReasoning,
            currentReasoningComplete: false,
          };
        });
        break;

      case "message":
        setStreamingState((prev) => ({
          ...prev,
          messageContent: event.content,
          thinkingMessage: "",
        }));
        break;

      case "actions":
        setStreamingState((prev) => ({
          ...prev,
          actions: event.actions,
        }));
        break;

      case "error":
        setError(event.error);
        setStreamingState((prev) => ({
          ...prev,
          isStreaming: false,
        }));
        break;

      case "done":
        setStreamingState((prev) => {
          const { messageContent, actions, timeline, currentReasoning } = prev;
          
          // Build final timeline - add any remaining reasoning
          let finalTimeline = [...timeline];
          if (currentReasoning) {
            finalTimeline.push({ type: "reasoning" as const, content: currentReasoning });
          }
          
          queueMicrotask(() => {
            const finalMessage: ExtendedChatMessage = {
              id: uuid(),
              role: "assistant",
              content: messageContent,
              timestamp: new Date().toISOString(),
              actions: actions.length > 0 ? actions : undefined,
              timeline: finalTimeline.length > 0 ? finalTimeline : undefined,
            };
            setMessages((msgs) => [...msgs, finalMessage]);
          });
          
          return {
            isStreaming: false,
            thinkingMessage: "",
            currentReasoning: "",
            currentReasoningComplete: false,
            timeline: [],
            pendingToolCalls: new Map(),
            messageContent: "",
            actions: [],
          };
        });
        break;
    }
  }, []);

  const sendMessage = useCallback(async () => {
    if (!inputValue.trim() || streamingState.isStreaming) return;

    const userMessage: ExtendedChatMessage = {
      id: uuid(),
      role: "user",
      content: inputValue.trim(),
      timestamp: new Date().toISOString(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInputValue("");
    setError(null);
    
    setStreamingState({
      isStreaming: true,
      thinkingMessage: "Analyzing your question...",
      currentReasoning: "",
      currentReasoningComplete: false,
      timeline: [],
      pendingToolCalls: new Map(),
      messageContent: "",
      actions: [],
    });

    abortControllerRef.current = new AbortController();

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { 
          "Content-Type": "application/json",
          "Accept": "text/event-stream",
        },
        body: JSON.stringify({
          message: userMessage.content,
          conversationHistory: messages,
          context,
          stream: true,
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        throw new Error(errData.error || "Failed to get response");
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error("No response body");
      }

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const eventData = JSON.parse(line.slice(6)) as ChatStreamEvent;
              processStreamEvent(eventData);
            } catch (e) {
              console.error("Failed to parse SSE event:", e);
            }
          }
        }
      }
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      console.error("Chat error:", err);
      setError(err instanceof Error ? err.message : "Something went wrong");
      setStreamingState((prev) => ({
        ...prev,
        isStreaming: false,
      }));
    }
  }, [inputValue, streamingState.isStreaming, messages, context, processStreamEvent]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      e.stopPropagation();
      sendMessage();
    } else if (e.key === "Escape") {
      e.stopPropagation();
      setIsOpen(false);
    }
  };

  const handleNodeClick = useCallback(
    (nodeId: string) => {
      onAction({
        type: "navigate_to_node",
        label: "Navigate",
        payload: { nodeId },
      });
    },
    [onAction]
  );

  const clearConversation = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    setMessages([]);
    setError(null);
    setStreamingState({
      isStreaming: false,
      thinkingMessage: "",
      currentReasoning: "",
      currentReasoningComplete: false,
      timeline: [],
      pendingToolCalls: new Map(),
      messageContent: "",
      actions: [],
    });
  };

  return (
    <>
      {!isOpen && (
        <button
          onClick={() => setIsOpen(true)}
          className="fixed bottom-4 left-4 z-40 px-4 py-2.5 rounded-lg shadow-lg flex items-center gap-2 transition-all bg-gradient-to-br from-cyan-500 to-emerald-500 hover:from-cyan-400 hover:to-emerald-400"
          title="Toggle Chat (?)"
        >
          <svg className="w-5 h-5 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
          </svg>
          <span className="text-sm font-medium text-black">Ask AI</span>
        </button>
      )}

      <div
        className={`fixed bottom-0 left-0 w-1/2 z-30 transition-transform duration-300 ${
          isOpen ? "translate-y-0" : "translate-y-full"
        }`}
      >
        <div className="bg-[#0f0f14] border-t border-white/10 shadow-2xl">
          <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-emerald-500 flex items-center justify-center">
                <svg className="w-4 h-4 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                </svg>
              </div>
              <div>
                <h3 className="font-medium text-white">Pipeline Assistant</h3>
                <p className="text-xs text-white/50">Ask questions about your data pipeline</p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {(messages.length > 0 || streamingState.isStreaming) && (
                <button
                  onClick={clearConversation}
                  className="p-2 hover:bg-white/10 rounded-lg transition-colors text-white/50 hover:text-white/80"
                  title="Clear conversation"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                  </svg>
                </button>
              )}
              <button
                onClick={() => setIsOpen(false)}
                className="p-2 hover:bg-white/10 rounded-lg transition-colors"
              >
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>
            </div>
          </div>

          <div className="h-80 overflow-y-auto p-4 space-y-4">
            {messages.length === 0 && !streamingState.isStreaming && (
              <div className="text-center py-8">
                <div className="text-4xl mb-3">💬</div>
                <p className="text-white/60 text-sm">
                  Ask me anything about your data pipeline!
                </p>
                <div className="mt-4 space-y-2 text-left max-w-md mx-auto">
                  <p className="text-xs text-white/40">Example questions:</p>
                  <button
                    onClick={() => setInputValue("What data sources feed into the mech outreach pipeline?")}
                    className="block w-full text-left text-sm text-white/60 hover:text-white/80 hover:bg-white/5 px-3 py-2 rounded-lg transition-colors"
                  >
                    "What data sources feed into the mech outreach pipeline?"
                  </button>
                  <button
                    onClick={() => setInputValue("How does the no_longer_at_account field get populated?")}
                    className="block w-full text-left text-sm text-white/60 hover:text-white/80 hover:bg-white/5 px-3 py-2 rounded-lg transition-colors"
                  >
                    "How does the no_longer_at_account field get populated?"
                  </button>
                  <button
                    onClick={() => setInputValue("What happens to leads from the job changes clay table?")}
                    className="block w-full text-left text-sm text-white/60 hover:text-white/80 hover:bg-white/5 px-3 py-2 rounded-lg transition-colors"
                  >
                    "What happens to leads from the job changes clay table?"
                  </button>
                </div>
              </div>
            )}

            {messages.map((msg) => (
              <Message
                key={msg.id}
                message={msg}
                allNodes={allNodes}
                onAction={onAction}
                onNodeClick={handleNodeClick}
              />
            ))}

            {streamingState.isStreaming && (
              <StreamingMessage
                streamingState={streamingState}
                allNodes={allNodes}
                onNodeClick={handleNodeClick}
              />
            )}

            {error && (
              <div className="flex justify-center">
                <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-4 py-2 text-red-400 text-sm">
                  {error}
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          <div className="p-4 border-t border-white/10">
            <div className="flex gap-2">
              <textarea
                ref={inputRef}
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask about your data pipeline..."
                rows={1}
                className="flex-1 px-4 py-3 bg-white/5 border border-white/10 rounded-xl text-sm placeholder-white/40 focus:outline-none focus:border-cyan-500/50 resize-none"
                style={{ minHeight: "44px", maxHeight: "120px" }}
              />
              <button
                onClick={sendMessage}
                disabled={!inputValue.trim() || streamingState.isStreaming}
                className="px-4 py-3 bg-gradient-to-r from-cyan-500 to-emerald-500 hover:from-cyan-400 hover:to-emerald-400 disabled:opacity-50 disabled:cursor-not-allowed rounded-xl transition-all"
              >
                <svg className="w-5 h-5 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
                </svg>
              </button>
            </div>
            <div className="mt-2 text-xs text-white/30 text-center">
              Press <kbd className="px-1.5 py-0.5 bg-white/10 rounded">Enter</kbd> to send ·
              <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">Shift+Enter</kbd> for new line ·
              <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">?</kbd> to toggle
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
