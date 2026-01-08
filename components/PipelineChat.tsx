"use client";

import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { v4 as uuid } from "uuid";
import type {
  ChatMessage,
  ChatContext,
  ChatResponse,
  ProposedAction,
  GraphNode,
} from "@/lib/types";

// ============================================================================
// Types
// ============================================================================

interface PipelineChatProps {
  context: ChatContext;
  onAction: (action: ProposedAction) => void;
  allNodes?: GraphNode[];
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
      // Process inline formatting
      const processInline = (text: string): React.ReactNode[] => {
        const parts: React.ReactNode[] = [];
        let key = 0;

        // Match **bold**, *italic*, `code`
        const pattern = /(\*\*(.+?)\*\*|\*(.+?)\*|`([^`]+)`)/g;
        let lastIndex = 0;
        let match;

        while ((match = pattern.exec(text)) !== null) {
          // Add text before match
          if (match.index > lastIndex) {
            parts.push(text.slice(lastIndex, match.index));
          }

          if (match[2]) {
            // Bold
            parts.push(
              <strong key={key++} className="font-semibold text-white">
                {match[2]}
              </strong>
            );
          } else if (match[3]) {
            // Italic
            parts.push(
              <em key={key++} className="italic">
                {match[3]}
              </em>
            );
          } else if (match[4]) {
            // Code - check if it's a node name
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

        // Add remaining text
        if (lastIndex < text.length) {
          parts.push(text.slice(lastIndex));
        }

        return parts.length > 0 ? parts : [text];
      };

      // Handle lists
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

      // Regular paragraph
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
// Message Component
// ============================================================================

interface MessageProps {
  message: ChatMessage;
  allNodes?: GraphNode[];
  onAction: (action: ProposedAction) => void;
  onNodeClick?: (nodeId: string) => void;
}

function Message({ message, allNodes, onAction, onNodeClick }: MessageProps) {
  const isUser = message.role === "user";

  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
      <div
        className={`max-w-[85%] rounded-2xl px-4 py-3 ${
          isUser
            ? "bg-cyan-600/30 text-white"
            : "bg-white/5 border border-white/10"
        }`}
      >
        <MarkdownContent
          content={message.content}
          allNodes={allNodes}
          onNodeClick={onNodeClick}
        />

        {/* Tool calls indicator */}
        {message.toolCalls && message.toolCalls.length > 0 && (
          <div className="mt-2 pt-2 border-t border-white/10">
            <div className="text-xs text-white/40 flex items-center gap-1">
              <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
              {message.toolCalls.length} tool{message.toolCalls.length > 1 ? "s" : ""} used
            </div>
          </div>
        )}

        {/* Action buttons */}
        {message.actions && message.actions.length > 0 && (
          <div className="mt-3 pt-3 border-t border-white/10 flex flex-wrap gap-2">
            {message.actions.map((action, idx) => (
              <ActionButton
                key={idx}
                action={action}
                onClick={() => onAction(action)}
              />
            ))}
          </div>
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
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Scroll to bottom when messages change
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Focus input when drawer opens
  useEffect(() => {
    if (isOpen) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [isOpen]);

  // Keyboard shortcut to toggle chat
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't trigger if in any input or textarea
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement
      ) {
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

  // Handle sending a message
  const sendMessage = useCallback(async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage: ChatMessage = {
      id: uuid(),
      role: "user",
      content: inputValue.trim(),
      timestamp: new Date().toISOString(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInputValue("");
    setIsLoading(true);
    setError(null);

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: userMessage.content,
          conversationHistory: messages,
          context,
        }),
      });

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        throw new Error(errData.error || "Failed to get response");
      }

      const data: ChatResponse = await response.json();

      const assistantMessage: ChatMessage = {
        id: data.messageId,
        role: "assistant",
        content: data.response.message,
        timestamp: new Date().toISOString(),
        actions: data.response.actions,
        toolCalls: data.response.toolCalls,
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      console.error("Chat error:", err);
      setError(err instanceof Error ? err.message : "Something went wrong");
    } finally {
      setIsLoading(false);
    }
  }, [inputValue, isLoading, messages, context]);

  // Handle input keydown
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

  // Handle node click from markdown content
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

  // Clear conversation
  const clearConversation = () => {
    setMessages([]);
    setError(null);
  };

  return (
    <>
      {/* Toggle Button - only visible when drawer is closed */}
      {!isOpen && (
        <button
          onClick={() => setIsOpen(true)}
          className="fixed bottom-4 left-1/2 -translate-x-1/2 z-40 px-4 py-2.5 rounded-lg shadow-lg flex items-center gap-2 transition-all bg-gradient-to-br from-cyan-500 to-emerald-500 hover:from-cyan-400 hover:to-emerald-400"
          title="Toggle Chat (?)"
        >
          <svg className="w-5 h-5 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
          </svg>
          <span className="text-sm font-medium text-black">Ask AI</span>
        </button>
      )}

      {/* Chat Drawer */}
      <div
        className={`fixed bottom-0 left-0 right-0 z-30 transition-transform duration-300 ${
          isOpen ? "translate-y-0" : "translate-y-full"
        }`}
      >
        <div className="bg-[#0f0f14] border-t border-white/10 shadow-2xl">
          {/* Header */}
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
              {messages.length > 0 && (
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

          {/* Messages */}
          <div className="h-80 overflow-y-auto p-4 space-y-4">
            {messages.length === 0 && (
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

            {isLoading && (
              <div className="flex justify-start">
                <div className="bg-white/5 border border-white/10 rounded-2xl px-4 py-3">
                  <div className="flex items-center gap-2 text-white/60">
                    <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                    <span className="text-sm">Thinking...</span>
                  </div>
                </div>
              </div>
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

          {/* Input */}
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
                disabled={!inputValue.trim() || isLoading}
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
