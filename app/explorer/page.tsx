"use client";

import { useEffect, useState, useCallback, useRef, Suspense, useMemo } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import FlowSelector from "@/components/FlowSelector";
import SearchBar from "@/components/SearchBar";
import DepthControl from "@/components/DepthControl";
import CreateFlowModal from "@/components/CreateFlowModal";
import type { GraphNode, GraphEdge, GraphGroup, GraphFlow } from "@/lib/types";
import type { GraphExplorerRef } from "@/components/GraphExplorer";

// Simple markdown renderer for explanations
function MarkdownContent({ content }: { content: string }) {
  const rendered = useMemo(() => {
    // Split into paragraphs
    const paragraphs = content.split(/\n\n+/);
    
    return paragraphs.map((paragraph, pIdx) => {
      // Process inline formatting
      const processInline = (text: string): React.ReactNode[] => {
        const parts: React.ReactNode[] = [];
        let remaining = text;
        let key = 0;
        
        // Match patterns: **bold**, *italic*, `code`, and plain text
        const pattern = /(\*\*(.+?)\*\*|\*(.+?)\*|`([^`]+)`)/g;
        let lastIndex = 0;
        let match;
        
        while ((match = pattern.exec(text)) !== null) {
          // Add plain text before match
          if (match.index > lastIndex) {
            parts.push(text.slice(lastIndex, match.index));
          }
          
          if (match[2]) {
            // Bold: **text**
            parts.push(
              <strong key={key++} className="font-semibold text-white">
                {match[2]}
              </strong>
            );
          } else if (match[3]) {
            // Italic: *text*
            parts.push(
              <em key={key++} className="italic">
                {match[3]}
              </em>
            );
          } else if (match[4]) {
            // Code: `text`
            parts.push(
              <code
                key={key++}
                className="px-1.5 py-0.5 rounded bg-white/10 font-mono text-xs text-emerald-300"
              >
                {match[4]}
              </code>
            );
          }
          
          lastIndex = pattern.lastIndex;
        }
        
        // Add remaining plain text
        if (lastIndex < text.length) {
          parts.push(text.slice(lastIndex));
        }
        
        return parts.length > 0 ? parts : [text];
      };
      
      // Handle line breaks within paragraph
      const lines = paragraph.split(/\n/);
      
      return (
        <p key={pIdx} className="mb-3 last:mb-0">
          {lines.map((line, lIdx) => (
            <span key={lIdx}>
              {processInline(line)}
              {lIdx < lines.length - 1 && <br />}
            </span>
          ))}
        </p>
      );
    });
  }, [content]);
  
  return <div className="text-sm text-white/80 leading-relaxed">{rendered}</div>;
}

// Dynamically import GraphExplorer to avoid SSR issues with Cytoscape
const GraphExplorer = dynamic(() => import("@/components/GraphExplorer"), {
  ssr: false,
  loading: () => (
    <div className="w-full h-full flex items-center justify-center">
      <div className="text-white/60">Loading graph...</div>
    </div>
  ),
});

interface SidePanelData {
  node: GraphNode;
  explanation?: string;
  upstream: GraphNode[];
  downstream: GraphNode[];
  isLoadingExplanation: boolean;
}

function ExplorerContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  
  const graphRef = useRef<GraphExplorerRef>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  
  const [nodes, setNodes] = useState<GraphNode[]>([]);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [groups, setGroups] = useState<GraphGroup[]>([]);
  const [flows, setFlows] = useState<GraphFlow[]>([]);
  const [selectedFlow, setSelectedFlow] = useState<string | undefined>();
  const [sidePanel, setSidePanel] = useState<SidePanelData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [neighborhoodDepth, setNeighborhoodDepth] = useState(3);
  const [showCreateFlow, setShowCreateFlow] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);
  const [canGoBack, setCanGoBack] = useState(false);

  // Track if we can go back in history
  useEffect(() => {
    setCanGoBack(window.history.length > 1);
  }, [sidePanel]);

  // Handle go back navigation
  const handleGoBack = useCallback(() => {
    router.back();
  }, [router]);

  // Show toast notification
  const showToast = useCallback((message: string, type: "success" | "error" = "success") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }, []);

  // Update URL when flow changes
  const updateUrl = useCallback((flowId?: string, nodeId?: string) => {
    const params = new URLSearchParams();
    if (flowId) params.set("flow", flowId);
    if (nodeId) params.set("node", nodeId);
    const newUrl = params.toString() ? `?${params.toString()}` : "/explorer";
    router.push(newUrl, { scroll: false });
  }, [router]);

  // Handle flow selection
  const handleFlowSelect = useCallback((flowId: string | undefined) => {
    setSelectedFlow(flowId);
    updateUrl(flowId, sidePanel?.node.id);
  }, [updateUrl, sidePanel]);

  // Track if initial load is complete to prevent re-fetching on URL changes
  const hasLoadedRef = useRef(false);

  // Load graph data - only once on mount, not on URL changes
  useEffect(() => {
    // Skip if already loaded - URL changes shouldn't refetch data
    if (hasLoadedRef.current) {
      return;
    }

    async function loadGraph() {
      try {
        const res = await fetch("/api/graph");
        if (!res.ok) throw new Error("Failed to load graph");
        
        const data = await res.json();
        hasLoadedRef.current = true;
        setNodes(data.nodes);
        setEdges(data.edges);
        setGroups(data.groups);
        setFlows(data.flows);

        // Restore state from URL
        const flowId = searchParams.get("flow");
        const nodeId = searchParams.get("node");
        
        if (flowId && data.flows.some((f: GraphFlow) => f.id === flowId)) {
          setSelectedFlow(flowId);
        } else if (!flowId) {
          // Default to "Mechanized Outreach" flow if no flow specified
          const defaultFlow = data.flows.find((f: GraphFlow) => 
            f.name.toLowerCase().includes("mechanized outreach")
          );
          if (defaultFlow) {
            setSelectedFlow(defaultFlow.id);
          }
        }
        
        // Focus node from URL after graph loads
        if (nodeId) {
          setTimeout(() => {
            const node = data.nodes.find((n: GraphNode) => n.id === nodeId);
            if (node) {
              handleNodeSelect(node);
              graphRef.current?.focusNode(nodeId);
            }
          }, 500);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setIsLoading(false);
      }
    }
    loadGraph();
  }, [searchParams]);

  // Handle node selection
  const handleNodeSelect = useCallback(async (node: GraphNode | null) => {
    if (!node) {
      setSidePanel(null);
      updateUrl(selectedFlow);
      return;
    }

    // Update URL
    updateUrl(selectedFlow, node.id);

    // Show loading state IMMEDIATELY so user sees feedback
    setSidePanel({
      node: node,
      explanation: undefined,
      upstream: [],
      downstream: [],
      isLoadingExplanation: true,
    });

    // Fetch node details
    try {
      const res = await fetch(`/api/node/${encodeURIComponent(node.id)}`);
      if (!res.ok) throw new Error("Failed to load node");
      
      const data = await res.json();
      setSidePanel({
        node: data.node,
        explanation: data.explanation,
        upstream: data.upstream,
        downstream: data.downstream,
        isLoadingExplanation: !data.explanation,
      });

      // If no explanation cached, generate one
      if (!data.explanation) {
        const explainRes = await fetch("/api/explain", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ nodeId: node.id }),
        });
        
        if (explainRes.ok) {
          const explainData = await explainRes.json();
          setSidePanel((prev) =>
            prev
              ? { ...prev, explanation: explainData.explanation, isLoadingExplanation: false }
              : null
          );
        }
      }
    } catch (err) {
      console.error("Failed to load node details:", err);
    }
  }, [selectedFlow, updateUrl]);

  // Handle node double-click (column lineage)
  const handleNodeDoubleClick = useCallback((_node: GraphNode) => {
    // TODO: Open column lineage modal in the future
    // For now, the single-click side panel provides the detail view
  }, []);

  // Handle search selection - focus on node in graph
  const handleSearchSelect = useCallback((node: GraphNode) => {
    handleNodeSelect(node);
    setTimeout(() => {
      graphRef.current?.focusNode(node.id);
    }, 100);
  }, [handleNodeSelect]);

  // Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't trigger if in an input
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        if (e.key === "Escape") {
          (e.target as HTMLElement).blur();
        }
        return;
      }

      switch (e.key) {
        case "Escape":
          setSidePanel(null);
          updateUrl(selectedFlow);
          break;
        case "/":
          e.preventDefault();
          searchInputRef.current?.focus();
          break;
        case "f":
        case "F":
          graphRef.current?.fitToScreen();
          break;
        case "[":
          if (window.history.length > 1) {
            router.back();
          }
          break;
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedFlow, updateUrl, router]);

  // Handle browser back/forward navigation
  useEffect(() => {
    const handlePopState = () => {
      const params = new URLSearchParams(window.location.search);
      const nodeId = params.get("node");
      if (nodeId) {
        const node = nodes.find(n => n.id === nodeId);
        if (node) {
          // Directly set side panel without triggering URL update
          fetch(`/api/node/${encodeURIComponent(node.id)}`)
            .then(res => res.json())
            .then(data => {
              setSidePanel({
                node: data.node,
                explanation: data.explanation,
                upstream: data.upstream,
                downstream: data.downstream,
                isLoadingExplanation: !data.explanation,
              });
            });
          graphRef.current?.focusNode(nodeId);
        }
      } else {
        setSidePanel(null);
        graphRef.current?.deselectAll();
      }
    };

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, [nodes]);

  // Re-index handler
  const handleReindex = useCallback(async () => {
    try {
      showToast("Starting re-index...");
      const res = await fetch("/api/ingest", { method: "POST" });
      if (!res.ok) throw new Error("Failed to start re-index");
      
      const { jobId } = await res.json();
      router.push(`/?jobId=${jobId}`);
    } catch (err) {
      showToast("Failed to start re-index", "error");
    }
  }, [router, showToast]);

  // Reset handler
  const handleReset = useCallback(async () => {
    setIsResetting(true);
    try {
      const res = await fetch("/api/reset", { method: "POST" });
      if (res.ok) {
        showToast("All data cleared. Redirecting...");
        setTimeout(() => router.push("/"), 500);
      } else {
        showToast("Failed to reset database", "error");
      }
    } catch (err) {
      showToast("Failed to reset", "error");
    } finally {
      setIsResetting(false);
      setShowResetConfirm(false);
    }
  }, [router, showToast]);

  if (isLoading) {
    return (
      <div className="h-screen bg-[#0a0a0f] flex items-center justify-center">
        <div className="text-white/60">Loading graph data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="h-screen bg-[#0a0a0f] flex items-center justify-center">
        <div className="text-center space-y-4">
          <div className="text-red-400">{error}</div>
          <a href="/" className="text-cyan-400 hover:underline">
            Go back to index
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-[#0a0a0f] flex flex-col overflow-hidden">
      {/* Toast Notification */}
      {toast && (
        <div
          className={`fixed top-4 right-4 z-50 px-4 py-2 rounded-lg shadow-lg animate-slide-up ${
            toast.type === "error"
              ? "bg-red-500/90 text-white"
              : "bg-emerald-500/90 text-black"
          }`}
        >
          {toast.message}
        </div>
      )}

      {/* Header */}
      <header className="border-b border-white/10 bg-[#0a0a0f]/80 backdrop-blur-sm z-10">
        <div className="px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <a href="/" className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-emerald-400 to-cyan-500 flex items-center justify-center">
                <svg className="w-4 h-4 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <span className="font-semibold">Pipeline Explorer</span>
            </a>
            
            <div className="h-6 w-px bg-white/20" />

            {canGoBack && (
              <button
                onClick={handleGoBack}
                className="p-2 hover:bg-white/10 rounded-lg transition-colors"
                title="Go back ([)"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                </svg>
              </button>
            )}
            
            <FlowSelector
              flows={flows}
              selectedFlow={selectedFlow}
              onSelect={handleFlowSelect}
            />
            
            <button
              onClick={() => setShowCreateFlow(true)}
              className="px-3 py-2 text-sm bg-white/10 hover:bg-white/20 rounded-lg transition-colors"
            >
              + New Flow
            </button>
          </div>

          <div className="flex items-center gap-4">
            <SearchBar onSelect={handleSearchSelect} ref={searchInputRef} />
            <DepthControl depth={neighborhoodDepth} onChange={setNeighborhoodDepth} />
            
            <div className="text-sm text-white/40">
              {nodes.length} nodes · {edges.length} edges
            </div>

            <button
              onClick={handleReindex}
              className="px-3 py-2 text-sm text-white/60 hover:text-white hover:bg-white/10 rounded-lg transition-colors"
              title="Re-index repositories"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
              </svg>
            </button>

            <button
              onClick={() => setShowResetConfirm(true)}
              className="px-3 py-2 text-sm text-red-400/60 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors"
              title="Reset all data"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
          </div>
        </div>
      </header>

      {/* Main content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Graph - min-w-0 allows flex item to shrink below content size */}
        <div className="flex-1 min-w-0 relative overflow-hidden">
          <GraphExplorer
            ref={graphRef}
            nodes={nodes}
            edges={edges}
            groups={groups}
            flows={flows}
            selectedFlow={selectedFlow}
            onNodeSelect={handleNodeSelect}
            onNodeDoubleClick={handleNodeDoubleClick}
          />
          
        </div>

        {/* Side Panel */}
        {sidePanel && (
          <div className="w-96 border-l border-white/10 bg-[#0f0f14] overflow-y-auto">
            <div className="p-4 space-y-6">
              {/* Header */}
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <span
                      className={`text-xs px-2 py-0.5 rounded ${
                        sidePanel.node.type === "source"
                          ? "bg-blue-500/20 text-blue-300"
                          : sidePanel.node.type === "model"
                          ? "bg-emerald-500/20 text-emerald-300"
                          : "bg-purple-500/20 text-purple-300"
                      }`}
                    >
                      {sidePanel.node.type}
                    </span>
                    {sidePanel.node.subtype && (
                      <span className="text-xs text-white/40">{sidePanel.node.subtype}</span>
                    )}
                  </div>
                  <h2 className="text-lg font-semibold mt-1">{sidePanel.node.name}</h2>
                  <p className="text-xs text-white/40 font-mono mt-0.5 break-all">
                    {sidePanel.node.id}
                  </p>
                </div>
                <button
                  onClick={() => {
                    setSidePanel(null);
                    updateUrl(selectedFlow);
                  }}
                  className="p-1 hover:bg-white/10 rounded transition-colors"
                  title="Close (Esc)"
                >
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              {/* Copy Link Button */}
              <button
                onClick={() => {
                  navigator.clipboard.writeText(window.location.href);
                  showToast("Link copied to clipboard");
                }}
                className="w-full px-3 py-2 text-sm bg-white/5 hover:bg-white/10 rounded-lg transition-colors flex items-center justify-center gap-2"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z" />
                </svg>
                Share Link
              </button>

              {/* Explanation */}
              <div>
                <h3 className="text-sm font-medium text-white/60 mb-2">Explanation</h3>
                {sidePanel.isLoadingExplanation ? (
                  <div className="flex items-center gap-2 text-sm text-white/40">
                    <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                    Generating explanation...
                  </div>
                ) : sidePanel.explanation ? (
                  <MarkdownContent content={sidePanel.explanation} />
                ) : (
                  <p className="text-sm text-white/40 italic">No explanation available.</p>
                )}
              </div>

              {/* Metadata */}
              {sidePanel.node.metadata && (
                <div>
                  <h3 className="text-sm font-medium text-white/60 mb-2">Details</h3>
                  <dl className="text-sm space-y-1">
                    {sidePanel.node.metadata.schema && (
                      <div className="flex justify-between">
                        <dt className="text-white/40">Schema</dt>
                        <dd className="font-mono">{sidePanel.node.metadata.schema}</dd>
                      </div>
                    )}
                    {sidePanel.node.metadata.materialization && (
                      <div className="flex justify-between">
                        <dt className="text-white/40">Materialization</dt>
                        <dd>{sidePanel.node.metadata.materialization}</dd>
                      </div>
                    )}
                    {sidePanel.node.metadata.tags && sidePanel.node.metadata.tags.length > 0 && (
                      <div>
                        <dt className="text-white/40 mb-1">Tags</dt>
                        <dd className="flex flex-wrap gap-1">
                          {sidePanel.node.metadata.tags.map((tag) => (
                            <span
                              key={tag}
                              className="text-xs bg-white/10 px-2 py-0.5 rounded"
                            >
                              {tag}
                            </span>
                          ))}
                        </dd>
                      </div>
                    )}
                  </dl>
                </div>
              )}

              {/* Upstream */}
              {sidePanel.upstream.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-white/60 mb-2">
                    Upstream ({sidePanel.upstream.length})
                  </h3>
                  <div className="space-y-1 max-h-40 overflow-y-auto">
                    {sidePanel.upstream.map((node) => (
                      <button
                        key={node.id}
                        onClick={() => {
                          handleNodeSelect(node);
                          graphRef.current?.focusNode(node.id);
                        }}
                        className="w-full text-left px-2 py-1.5 rounded hover:bg-white/10 text-sm transition-colors"
                      >
                        <span className="text-emerald-400">←</span>{" "}
                        <span className="text-white/80">{node.name}</span>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Downstream */}
              {sidePanel.downstream.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-white/60 mb-2">
                    Downstream ({sidePanel.downstream.length})
                  </h3>
                  <div className="space-y-1 max-h-40 overflow-y-auto">
                    {sidePanel.downstream.map((node) => (
                      <button
                        key={node.id}
                        onClick={() => {
                          handleNodeSelect(node);
                          graphRef.current?.focusNode(node.id);
                        }}
                        className="w-full text-left px-2 py-1.5 rounded hover:bg-white/10 text-sm transition-colors"
                      >
                        <span className="text-cyan-400">→</span>{" "}
                        <span className="text-white/80">{node.name}</span>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Columns */}
              {sidePanel.node.metadata?.columns && sidePanel.node.metadata.columns.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-white/60 mb-2">
                    Columns ({sidePanel.node.metadata.columns.length})
                  </h3>
                  <div className="space-y-1 max-h-60 overflow-y-auto font-mono text-xs">
                    {sidePanel.node.metadata.columns.map((col) => (
                      <div
                        key={col.name}
                        className="flex justify-between py-1 px-2 hover:bg-white/5 rounded cursor-pointer"
                        title={col.description || undefined}
                      >
                        <span className="text-white/80">{col.name}</span>
                        <span className="text-white/40">{col.type}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* File Citation */}
              {sidePanel.node.metadata?.filePath && (
                <div>
                  <h3 className="text-sm font-medium text-white/60 mb-2">Source</h3>
                  <code className="text-xs bg-white/5 px-2 py-1 rounded block break-all">
                    {sidePanel.node.metadata.filePath}
                  </code>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Create Flow Modal */}
      <CreateFlowModal
        isOpen={showCreateFlow}
        onClose={() => setShowCreateFlow(false)}
        onCreated={(newFlow) => {
          // Refresh flows
          fetch("/api/flows")
            .then((res) => res.json())
            .then((data) => {
              setFlows(data.flows);
              setSelectedFlow(newFlow.id);
              showToast(`Created flow "${newFlow.name}" with ${newFlow.memberCount} nodes`);
            });
        }}
      />

      {/* Reset Confirmation Modal */}
      {showResetConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-[#1a1a24] rounded-xl border border-white/10 p-6 max-w-md w-full mx-4 shadow-2xl">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-full bg-red-500/20 flex items-center justify-center">
                <svg className="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold">Reset All Data?</h3>
            </div>
            <p className="text-white/60 mb-6">
              This will permanently delete all indexed data, including nodes, edges, flows, groups, and explanations. You will need to re-analyze your pipeline from scratch.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowResetConfirm(false)}
                className="px-4 py-2 rounded-lg bg-white/10 hover:bg-white/20 transition-colors font-medium"
                disabled={isResetting}
              >
                Cancel
              </button>
              <button
                onClick={handleReset}
                disabled={isResetting}
                className="px-4 py-2 rounded-lg bg-red-500 hover:bg-red-400 text-white transition-colors font-medium disabled:opacity-50"
              >
                {isResetting ? "Resetting..." : "Reset All Data"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// Wrap in Suspense for useSearchParams
export default function ExplorerPage() {
  return (
    <Suspense
      fallback={
        <div className="h-screen bg-[#0a0a0f] flex items-center justify-center">
          <div className="text-white/60">Loading...</div>
        </div>
      }
    >
      <ExplorerContent />
    </Suspense>
  );
}
