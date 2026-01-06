"use client";

import { useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";
import FlowSelector from "@/components/FlowSelector";
import SearchBar from "@/components/SearchBar";
import DepthControl from "@/components/DepthControl";
import MiniMap from "@/components/MiniMap";
import CreateFlowModal from "@/components/CreateFlowModal";
import type { GraphNode, GraphEdge, GraphGroup, GraphFlow } from "@/lib/types";

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

export default function ExplorerPage() {
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

  // Load graph data
  useEffect(() => {
    async function loadGraph() {
      try {
        const res = await fetch("/api/graph");
        if (!res.ok) throw new Error("Failed to load graph");
        
        const data = await res.json();
        setNodes(data.nodes);
        setEdges(data.edges);
        setGroups(data.groups);
        setFlows(data.flows);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setIsLoading(false);
      }
    }
    loadGraph();
  }, []);

  // Handle node selection
  const handleNodeSelect = useCallback(async (node: GraphNode | null) => {
    if (!node) {
      setSidePanel(null);
      return;
    }

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
  }, []);

  // Handle node double-click (column lineage)
  const handleNodeDoubleClick = useCallback((node: GraphNode) => {
    // Open column lineage view - for MVP, just log
    console.log("Double-click:", node);
  }, []);

  // Handle search selection
  const handleSearchSelect = useCallback((node: GraphNode) => {
    handleNodeSelect(node);
    // TODO: Focus on node in graph
  }, [handleNodeSelect]);

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
            
            <FlowSelector
              flows={flows}
              selectedFlow={selectedFlow}
              onSelect={setSelectedFlow}
            />
            
            <button
              onClick={() => setShowCreateFlow(true)}
              className="px-3 py-2 text-sm bg-white/10 hover:bg-white/20 rounded-lg transition-colors"
            >
              + New Flow
            </button>
          </div>

          <div className="flex items-center gap-4">
            <SearchBar onSelect={handleSearchSelect} />
            <DepthControl depth={neighborhoodDepth} onChange={setNeighborhoodDepth} />
            
            <div className="text-sm text-white/40">
              {nodes.length} nodes · {edges.length} edges
            </div>
          </div>
        </div>
      </header>

      {/* Main content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Graph */}
        <div className="flex-1 relative">
          <GraphExplorer
            nodes={nodes}
            edges={edges}
            groups={groups}
            flows={flows}
            selectedFlow={selectedFlow}
            onNodeSelect={handleNodeSelect}
            onNodeDoubleClick={handleNodeDoubleClick}
          />
          
          {/* MiniMap */}
          <div className="absolute top-4 right-4">
            <MiniMap
              nodeCount={nodes.length}
              edgeCount={edges.length}
              selectedNode={sidePanel?.node.id}
            />
          </div>
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
                  onClick={() => setSidePanel(null)}
                  className="p-1 hover:bg-white/10 rounded transition-colors"
                >
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              {/* Explanation */}
              <div>
                <h3 className="text-sm font-medium text-white/60 mb-2">Explanation</h3>
                {sidePanel.isLoadingExplanation ? (
                  <div className="flex items-center gap-2 text-sm text-white/40">
                    <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                    Generating explanation...
                  </div>
                ) : (
                  <p className="text-sm text-white/80 leading-relaxed">
                    {sidePanel.explanation || "No explanation available."}
                  </p>
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
                        onClick={() => handleNodeSelect(node)}
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
                        onClick={() => handleNodeSelect(node)}
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
                        className="flex justify-between py-1 px-2 hover:bg-white/5 rounded"
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
            });
        }}
      />
    </div>
  );
}

