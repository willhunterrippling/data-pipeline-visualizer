"use client";

import { useEffect, useState, useCallback, useRef, Suspense, useMemo } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import SearchBar from "@/components/SearchBar";
import DepthControl from "@/components/DepthControl";
import OrientationHeader from "@/components/OrientationHeader";
import type { GraphNode, GraphEdge, GraphFlow } from "@/lib/types";
import type { GraphExplorerRef, VisibleNode } from "@/components/GraphExplorer";
import type { VisibilityReason } from "@/lib/graph/visibility";

// Dynamically import GraphExplorer to avoid SSR issues with Cytoscape
const GraphExplorer = dynamic(() => import("@/components/GraphExplorer"), {
  ssr: false,
  loading: () => (
    <div className="w-full h-full flex items-center justify-center">
      <div className="text-white/60">Loading graph...</div>
    </div>
  ),
});

// Markdown content renderer for explanations
interface MarkdownContentProps {
  content: string;
  nodes?: GraphNode[];
  onNodeClick?: (node: GraphNode) => void;
}

function MarkdownContent({ content, nodes, onNodeClick }: MarkdownContentProps) {
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
            const matchingNode = nodes?.find(n => n.name === codeText);
            
            if (matchingNode && onNodeClick) {
              parts.push(
                <button
                  key={key++}
                  onClick={() => onNodeClick(matchingNode)}
                  className="px-1.5 py-0.5 rounded bg-white/10 font-mono text-xs text-emerald-300 hover:bg-emerald-500/20 hover:text-emerald-200 cursor-pointer transition-colors"
                >
                  {codeText}
                </button>
              );
            } else {
              parts.push(
                <code
                  key={key++}
                  className="px-1.5 py-0.5 rounded bg-white/10 font-mono text-xs text-emerald-300"
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
  }, [content, nodes, onNodeClick]);
  
  return <div className="text-sm text-white/80 leading-relaxed">{rendered}</div>;
}

interface SmartLayerName {
  layer: number;
  name: string;
  genericName: string;
  nodeCount: number;
  topPrefixes: string[];
}

interface LineageData {
  anchor: GraphNode;
  nodes: VisibleNode[];
  edges: GraphEdge[];
  ghostNodes: VisibleNode[];
  layers: Record<string, { layer: number; name: string }>;
  smartLayerNames: Record<number, SmartLayerName>;
  visibilityReasons: Record<string, { reason: VisibilityReason; description: string }>;
  stats: {
    totalNodes: number;
    visibleNodes: number;
    ghostNodes: number;
    layerRange: { min: number; max: number };
  };
}

interface SidePanelData {
  node: GraphNode;
  explanation?: string;
  upstream: GraphNode[];
  downstream: GraphNode[];
  isLoadingExplanation: boolean;
  visibilityReason?: string;
}

function ExplorerContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  
  const graphRef = useRef<GraphExplorerRef>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const hasInitializedFlowRef = useRef(false);
  
  // State
  // anchorId = the "origin" source node that should always be visible
  // focusId = the current exploration focus (where we're exploring from)
  const [anchorId, setAnchorId] = useState<string | null>(null);
  const [focusId, setFocusId] = useState<string | null>(null);
  const [flowId, setFlowId] = useState<string | null>(null);
  const [upstreamDepth, setUpstreamDepth] = useState(1);
  const [downstreamDepth, setDownstreamDepth] = useState(1);
  
  const [lineageData, setLineageData] = useState<LineageData | null>(null);
  const [flows, setFlows] = useState<GraphFlow[]>([]);
  const [allNodes, setAllNodes] = useState<GraphNode[]>([]);
  
  const [sidePanel, setSidePanel] = useState<SidePanelData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);

  // Show toast notification
  const showToast = useCallback((message: string, type: "success" | "error" = "success") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }, []);

  // Update URL when state changes
  const updateUrl = useCallback((anchor?: string | null, flow?: string | null) => {
    const params = new URLSearchParams();
    if (anchor) params.set("anchor", anchor);
    if (flow) params.set("flow", flow);
    const newUrl = params.toString() ? `?${params.toString()}` : "/explorer";
    router.push(newUrl, { scroll: false });
  }, [router]);

  // Load initial data (flows and all nodes for search)
  useEffect(() => {
    async function loadInitialData() {
      try {
        const res = await fetch("/api/graph");
        if (!res.ok) throw new Error("Failed to load graph");
        
        const data = await res.json();
        setFlows(data.flows);
        setAllNodes(data.nodes);

        // Restore state from URL
        const anchor = searchParams.get("anchor");
        const flow = searchParams.get("flow");
        
        // Determine which flow to use
        let selectedFlow: GraphFlow | undefined;
        
        if (flow && data.flows.some((f: GraphFlow) => f.id === flow)) {
          selectedFlow = data.flows.find((f: GraphFlow) => f.id === flow);
          setFlowId(flow);
        } else if (!hasInitializedFlowRef.current) {
          // Default to "Mechanized Outreach" flow only on initial load
          selectedFlow = data.flows.find((f: GraphFlow) => 
            f.name.toLowerCase().includes("mechanized outreach")
          );
          if (selectedFlow) {
            setFlowId(selectedFlow.id);
          }
        }
        
        // Set anchor: from URL, or auto-select first anchor from flow
        if (anchor) {
          setAnchorId(anchor);
        } else if (selectedFlow && selectedFlow.anchorNodes.length > 0) {
          // Auto-select first anchor from the flow
          const defaultAnchor = selectedFlow.anchorNodes[0];
          if (data.nodes.some((n: GraphNode) => n.id === defaultAnchor)) {
            setAnchorId(defaultAnchor);
          }
        }
        
        hasInitializedFlowRef.current = true;
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setIsLoading(false);
      }
    }
    loadInitialData();
  }, [searchParams]);

  // Fetch lineage when anchor/focus/flow/depth changes
  useEffect(() => {
    if (!anchorId) {
      setLineageData(null);
      return;
    }

    async function fetchLineage() {
      try {
        const params = new URLSearchParams({
          upstreamDepth: upstreamDepth.toString(),
          downstreamDepth: downstreamDepth.toString(),
        });
        if (flowId) params.set("flowId", flowId);
        // If focus is set and different from anchor, include it for stretching exploration
        if (focusId && focusId !== anchorId) {
          params.set("focusId", focusId);
        }

        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/8b88715b-ceb9-4841-8612-e3ab766e87ab',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'explorer/page.tsx:fetchLineage',message:'Fetching lineage',data:{anchorId:anchorId?.slice(-40),focusId:focusId?.slice(-40),upstreamDepth,downstreamDepth,hasFocus:!!focusId},timestamp:Date.now(),sessionId:'debug-session',hypothesisId:'H4,H5'})}).catch(()=>{});
        // #endregion

        const res = await fetch(`/api/lineage/${encodeURIComponent(anchorId!)}?${params}`);
        if (!res.ok) throw new Error("Failed to load lineage");
        
        const data: LineageData = await res.json();
        setLineageData(data);
      } catch (err) {
        console.error("Failed to fetch lineage:", err);
        showToast("Failed to load lineage", "error");
      }
    }
    fetchLineage();
  }, [anchorId, focusId, flowId, upstreamDepth, downstreamDepth, showToast]);

  // Handle node selection from search - sets a new origin anchor
  const handleSelectAnchor = useCallback((node: GraphNode) => {
    setAnchorId(node.id);
    setFocusId(null); // Reset focus when selecting new anchor
    updateUrl(node.id, flowId);
    
    // Also show in side panel
    setSidePanel({
      node,
      upstream: [],
      downstream: [],
      isLoadingExplanation: true,
    });

    // Focus on the node
    setTimeout(() => {
      graphRef.current?.focusNode(node.id);
    }, 200);

    // Fetch node details
    fetch(`/api/node/${encodeURIComponent(node.id)}`)
      .then((res) => res.json())
      .then((data) => {
        setSidePanel({
          node: data.node,
          explanation: data.explanation,
          upstream: data.upstream,
          downstream: data.downstream,
          isLoadingExplanation: !data.explanation,
        });

        // Generate explanation if not cached
        if (!data.explanation) {
          fetch("/api/explain", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ nodeId: node.id }),
          })
            .then((res) => res.json())
            .then((explainData) => {
              setSidePanel((prev) =>
                prev
                  ? { ...prev, explanation: explainData.explanation, isLoadingExplanation: false }
                  : null
              );
            });
        }
      });
  }, [flowId, updateUrl]);

  // Handle graph node click - implements stretching exploration
  // Clicking a node in the lineage stretches the view in that direction
  const handleNodeClick = useCallback((node: GraphNode | null) => {
    if (!node) {
      setSidePanel(null);
      return;
    }

    // Get visibility reason if available
    const visibilityReason = lineageData?.visibilityReasons[node.id]?.description;
    const nodeLayer = lineageData?.layers[node.id]?.layer;

    // If clicking on a node in the lineage (not the anchor), stretch the view
    if (node.id !== anchorId && lineageData?.nodes.some(n => n.id === node.id)) {
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/8b88715b-ceb9-4841-8612-e3ab766e87ab',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'explorer/page.tsx:handleNodeClick',message:'Setting focus for stretching',data:{clickedNodeId:node.id.slice(-40),anchorId:anchorId?.slice(-40),nodeLayer:nodeLayer},timestamp:Date.now(),sessionId:'debug-session',hypothesisId:'H5'})}).catch(()=>{});
      // #endregion
      // Set this node as the focus to stretch the exploration
      setFocusId(node.id);
    }

    setSidePanel({
      node,
      upstream: [],
      downstream: [],
      isLoadingExplanation: true,
      visibilityReason,
    });

    // Fetch details
    fetch(`/api/node/${encodeURIComponent(node.id)}`)
      .then((res) => res.json())
      .then((data) => {
        setSidePanel((prev) => ({
          ...prev!,
          node: data.node,
          explanation: data.explanation,
          upstream: data.upstream,
          downstream: data.downstream,
          isLoadingExplanation: !data.explanation,
        }));
      });
  }, [lineageData, anchorId]);

  // Handle ghost node click
  const handleGhostNodeClick = useCallback((node: VisibleNode) => {
    // Show options modal or expand to include
    showToast(`${node.name} is outside the current flow`, "success");
  }, [showToast]);

  // Clear anchor and focus
  const handleClearAnchor = useCallback(() => {
    setAnchorId(null);
    setFocusId(null);
    setSidePanel(null);
    updateUrl(null, flowId);
  }, [flowId, updateUrl]);

  // Clear flow
  const handleClearFlow = useCallback(() => {
    setFlowId(null);
    updateUrl(anchorId, null);
  }, [anchorId, updateUrl]);

  // Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        if (e.key === "Escape") {
          (e.target as HTMLElement).blur();
        }
        return;
      }

      switch (e.key) {
        case "Escape":
          setSidePanel(null);
          break;
        case "/":
          e.preventDefault();
          searchInputRef.current?.focus();
          break;
        case "f":
        case "F":
          graphRef.current?.fitToScreen();
          break;
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  // Get current flow object
  const currentFlow = flowId ? flows.find((f) => f.id === flowId) : null;
  const anchorNode = lineageData?.anchor || allNodes.find((n) => n.id === anchorId);

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

            {/* Flow Selector (optional lens) */}
            <select
              value={flowId || ""}
              onChange={(e) => {
                const newFlowId = e.target.value || null;
                setFlowId(newFlowId);
                
                // Auto-select first anchor from the new flow
                const selectedFlow = newFlowId ? flows.find((f) => f.id === newFlowId) : null;
                let newAnchorId = anchorId;
                if (selectedFlow && selectedFlow.anchorNodes.length > 0) {
                  const defaultAnchor = selectedFlow.anchorNodes[0];
                  if (allNodes.some((n) => n.id === defaultAnchor)) {
                    newAnchorId = defaultAnchor;
                    setAnchorId(defaultAnchor);
                  }
                }
                
                updateUrl(newAnchorId, newFlowId);
              }}
              className="px-3 py-2 text-sm bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-cyan-500/50"
            >
              <option value="">All nodes (no flow filter)</option>
              {flows.map((flow) => (
                <option key={flow.id} value={flow.id}>
                  {flow.name} ({flow.memberNodes.length} nodes)
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-4">
            <SearchBar onSelect={handleSelectAnchor} ref={searchInputRef} />
            <DepthControl
              upstreamDepth={upstreamDepth}
              downstreamDepth={downstreamDepth}
              onUpstreamChange={setUpstreamDepth}
              onDownstreamChange={setDownstreamDepth}
            />
            
            <div className="text-sm text-white/40">
              {lineageData ? `${lineageData.stats.visibleNodes} nodes` : `${allNodes.length} total`}
            </div>
          </div>
        </div>
      </header>

      {/* Orientation Header */}
      <OrientationHeader
        anchor={anchorNode || null}
        selectedNode={sidePanel?.node || null}
        flow={currentFlow || null}
        stats={lineageData?.stats}
        onClearAnchor={anchorId ? handleClearAnchor : undefined}
        onClearFlow={flowId ? handleClearFlow : undefined}
        onAnchorClick={() => anchorId && graphRef.current?.centerOnAnchor()}
        onSelectedClick={() => sidePanel?.node && graphRef.current?.focusNode(sidePanel.node.id)}
      />

      {/* Main content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Graph */}
        <div className="flex-1 min-w-0 relative overflow-hidden">
          {lineageData ? (
            <GraphExplorer
              ref={graphRef}
              nodes={lineageData.nodes}
              edges={lineageData.edges}
              ghostNodes={lineageData.ghostNodes}
              anchorId={anchorId}
              layerRange={lineageData.stats.layerRange}
              smartLayerNames={lineageData.smartLayerNames}
              onNodeSelect={handleNodeClick}
              onGhostNodeClick={handleGhostNodeClick}
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-[#0a0a0f]">
              <div className="text-center space-y-4">
                <div className="text-4xl">🔍</div>
                <div className="text-white/60">
                  Search for a table to explore its lineage
                </div>
                <div className="text-white/40 text-sm">
                  Press <kbd className="px-2 py-1 bg-white/10 rounded">/</kbd> to search
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Side Panel */}
        {sidePanel && (
          <div className="w-96 border-l border-white/10 bg-[#0f0f14] overflow-y-auto">
            <div className="p-4 space-y-6">
              {/* Header */}
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span
                      className={`text-xs px-2 py-0.5 rounded flex-shrink-0 ${
                        sidePanel.node.type === "source"
                          ? "bg-blue-500/20 text-blue-300"
                          : sidePanel.node.type === "model"
                          ? "bg-emerald-500/20 text-emerald-300"
                          : "bg-purple-500/20 text-purple-300"
                      }`}
                    >
                      {sidePanel.node.type}
                    </span>
                    {sidePanel.visibilityReason && (
                      <span className="text-xs px-2 py-0.5 rounded bg-white/10 text-white/60">
                        {sidePanel.visibilityReason}
                      </span>
                    )}
                  </div>
                  <h2 className="text-lg font-semibold mt-1 break-words">{sidePanel.node.name}</h2>
                  <p className="text-xs text-white/40 font-mono mt-0.5 break-all">
                    {sidePanel.node.id}
                  </p>
                </div>
                <button
                  onClick={() => setSidePanel(null)}
                  className="p-1 hover:bg-white/10 rounded transition-colors"
                  title="Close (Esc)"
                >
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              {/* Set as Anchor button */}
              {sidePanel.node.id !== anchorId && (
                <button
                  onClick={() => handleSelectAnchor(sidePanel.node)}
                  className="w-full px-3 py-2 text-sm bg-purple-500/20 hover:bg-purple-500/30 border border-purple-500/30 rounded-lg transition-colors flex items-center justify-center gap-2"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122" />
                  </svg>
                  Set as Anchor
                </button>
              )}

              {/* Explanation */}
              <div>
                <h3 className="text-sm font-medium text-white/60 mb-2">Explanation</h3>
                {sidePanel.isLoadingExplanation ? (
                  <div className="flex items-center gap-2 text-sm text-white/40">
                    <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                    Generating explanation...
                  </div>
                ) : sidePanel.explanation ? (
                  <MarkdownContent 
                    content={sidePanel.explanation}
                    nodes={allNodes}
                    onNodeClick={handleSelectAnchor}
                  />
                ) : (
                  <p className="text-sm text-white/40 italic">No explanation available.</p>
                )}
              </div>

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
                        onClick={() => handleSelectAnchor(node)}
                        className="w-full text-left px-2 py-1.5 rounded hover:bg-white/10 text-sm transition-colors flex items-start gap-1.5"
                      >
                        <span className="text-emerald-400 flex-shrink-0">←</span>
                        <span className="text-white/80 break-all">{node.name}</span>
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
                        onClick={() => handleSelectAnchor(node)}
                        className="w-full text-left px-2 py-1.5 rounded hover:bg-white/10 text-sm transition-colors flex items-start gap-1.5"
                      >
                        <span className="text-cyan-400 flex-shrink-0">→</span>
                        <span className="text-white/80 break-all">{node.name}</span>
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
                    {sidePanel.node.metadata.columns.map((col, index) => (
                      <div
                        key={`${col.name}-${index}`}
                        className="flex justify-between py-1 px-2 hover:bg-white/5 rounded"
                        title={col.description || undefined}
                      >
                        <span className="text-white/80">{col.name}</span>
                        <span className="text-white/40">{col.type}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
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
