"use client";

import {
  useEffect,
  useRef,
  useState,
  useCallback,
  useImperativeHandle,
  forwardRef,
} from "react";
import cytoscape, { Core, NodeSingular } from "cytoscape";
import type { GraphNode, GraphEdge } from "@/lib/types";
import type { VisibilityReason } from "@/lib/graph/visibility";
import type { SmartLayerName } from "@/lib/graph/layout";

export interface GraphExplorerRef {
  focusNode: (nodeId: string) => void;
  fitToScreen: () => void;
  deselectAll: () => void;
  centerOnAnchor: () => void;
}

export interface VisibleNode extends GraphNode {
  visibilityReason: VisibilityReason;
  relativeLayer: number;
}

interface GraphExplorerProps {
  nodes: VisibleNode[];
  edges: GraphEdge[];
  ghostNodes?: VisibleNode[];
  anchorId?: string | null;
  layerRange: { min: number; max: number };
  smartLayerNames?: Record<number, SmartLayerName>;  // Smart layer names from API
  onNodeSelect?: (node: GraphNode | null) => void;
  onNodeDoubleClick?: (node: GraphNode) => void;
  onGhostNodeClick?: (node: VisibleNode) => void;
}

// Color palette for different node types
const NODE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  source: { bg: "#1e3a5f", border: "#3b82f6", text: "#93c5fd" },
  seed: { bg: "#1e3a5f", border: "#3b82f6", text: "#93c5fd" },
  model: { bg: "#1e4035", border: "#10b981", text: "#6ee7b7" },
  table: { bg: "#1e4035", border: "#10b981", text: "#6ee7b7" },
  view: { bg: "#3b1e5f", border: "#8b5cf6", text: "#c4b5fd" },
  external: { bg: "#5f3b1e", border: "#f59e0b", text: "#fcd34d" },
};

// Semantic colors based on layer position
const LAYER_COLORS: Record<string, { bg: string; border: string }> = {
  source: { bg: "#1e3a5f", border: "#3b82f6" },      // Blue - sources
  staging: { bg: "#1a2e2e", border: "#22d3ee" },     // Cyan - staging
  intermediate: { bg: "#1e4035", border: "#10b981" }, // Green - intermediate
  anchor: { bg: "#2e1a2a", border: "#a78bfa" },      // Purple - selected
  downstream: { bg: "#2e2e1a", border: "#fbbf24" },  // Amber - consumers
};

// Swimlane configuration
const SWIMLANE_CONFIG = {
  padding: 40,
  headerHeight: 30,
  minWidth: 180,
  backgroundColor: "rgba(255, 255, 255, 0.03)",
  borderColor: "rgba(255, 255, 255, 0.1)",
  textColor: "rgba(255, 255, 255, 0.6)",
};

// Performance configuration
const PERFORMANCE_CONFIG = {
  viewportBuffer: 200,      // Pixels outside viewport to pre-render
  maxEdgesPerNode: 50,      // Skip hyper-connected nodes
  edgeBundlingThreshold: 10, // Bundle when >N edges converge
};

function getLayerName(relativeLayer: number): string {
  if (relativeLayer === 0) return "Selected";
  if (relativeLayer < -2) return "Sources";
  if (relativeLayer === -2) return "Staging";
  if (relativeLayer === -1) return "Intermediate";
  if (relativeLayer === 1) return "Consumers";
  return "Downstream";
}

const GraphExplorer = forwardRef<GraphExplorerRef, GraphExplorerProps>(
  function GraphExplorer(
    {
      nodes,
      edges,
      ghostNodes = [],
      anchorId,
      layerRange,
      smartLayerNames,
      onNodeSelect,
      onNodeDoubleClick,
      onGhostNodeClick,
    },
    ref
  ) {
    const containerRef = useRef<HTMLDivElement>(null);
    const cyRef = useRef<Core | null>(null);
    const swimlaneCanvasRef = useRef<HTMLCanvasElement>(null);

    // Keep refs for stable access in event handlers
    const nodesRef = useRef(nodes);
    const ghostNodesRef = useRef(ghostNodes);
    const smartLayerNamesRef = useRef(smartLayerNames);
    const onNodeSelectRef = useRef(onNodeSelect);
    const onNodeDoubleClickRef = useRef(onNodeDoubleClick);
    const onGhostNodeClickRef = useRef(onGhostNodeClick);

    useEffect(() => {
      nodesRef.current = nodes;
      ghostNodesRef.current = ghostNodes;
      smartLayerNamesRef.current = smartLayerNames;
      onNodeSelectRef.current = onNodeSelect;
      onNodeDoubleClickRef.current = onNodeDoubleClick;
      onGhostNodeClickRef.current = onGhostNodeClick;
    }, [nodes, ghostNodes, smartLayerNames, onNodeSelect, onNodeDoubleClick, onGhostNodeClick]);

    // Expose methods to parent
    useImperativeHandle(ref, () => ({
      focusNode: (nodeId: string) => {
        if (!cyRef.current) return;
        const cy = cyRef.current;
        const node = cy.getElementById(nodeId);
        if (node.length > 0) {
          cy.animate(
            { center: { eles: node }, zoom: 1.5 },
            { duration: 400 }
          );
          node.select();
        }
      },
      fitToScreen: () => {
        cyRef.current?.fit(undefined, 50);
      },
      deselectAll: () => {
        if (!cyRef.current) return;
        cyRef.current.elements().unselect();
        cyRef.current.elements().removeClass("highlighted dimmed");
      },
      centerOnAnchor: () => {
        if (!cyRef.current || !anchorId) return;
        const cy = cyRef.current;
        const anchor = cy.getElementById(anchorId);
        if (anchor.length > 0) {
          cy.animate(
            { center: { eles: anchor }, zoom: 1.2 },
            { duration: 400 }
          );
        }
      },
    }));

    // Build Cytoscape elements using pre-computed positions
    const buildElements = useCallback(() => {
      const elements: cytoscape.ElementDefinition[] = [];

      // Add visible nodes with pre-computed positions
      for (const node of nodes) {
        const colors = NODE_COLORS[node.type] || NODE_COLORS.model;
        const isAnchor = node.id === anchorId;

        elements.push({
          data: {
            id: node.id,
            label: node.name,
            nodeType: node.type,
            subtype: node.subtype,
            relativeLayer: node.relativeLayer,
            isAnchor,
            ...colors,
          },
          position: {
            x: node.layoutX ?? 0,
            y: node.layoutY ?? 0,
          },
          classes: `node-${node.type}${isAnchor ? " anchor-node" : ""}`,
        });
      }

      // Add ghost nodes (faded, outside flow)
      for (const node of ghostNodes) {
        const colors = NODE_COLORS[node.type] || NODE_COLORS.model;

        elements.push({
          data: {
            id: node.id,
            label: node.name,
            nodeType: node.type,
            subtype: node.subtype,
            relativeLayer: node.relativeLayer,
            isGhost: true,
            ...colors,
          },
          position: {
            x: node.layoutX ?? 0,
            y: node.layoutY ?? 0,
          },
          classes: "node-ghost",
        });
      }

      // Add edges
      const allNodeIds = new Set([
        ...nodes.map((n) => n.id),
        ...ghostNodes.map((n) => n.id),
      ]);

      for (const edge of edges) {
        if (allNodeIds.has(edge.from) && allNodeIds.has(edge.to)) {
          elements.push({
            data: {
              id: edge.id,
              source: edge.from,
              target: edge.to,
              edgeType: edge.type,
            },
            classes: `edge-${edge.type}`,
          });
        }
      }

      return elements;
    }, [nodes, edges, ghostNodes, anchorId]);

    // Draw swimlane backgrounds
    const drawSwimlanes = useCallback(() => {
      const canvas = swimlaneCanvasRef.current;
      const cy = cyRef.current;
      if (!canvas || !cy) return;

      const ctx = canvas.getContext("2d");
      if (!ctx) return;

      // Match canvas size to container
      const rect = canvas.parentElement?.getBoundingClientRect();
      if (!rect) return;
      canvas.width = rect.width;
      canvas.height = rect.height;

      // Clear canvas
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      // Group nodes by layer
      const layerNodes = new Map<number, NodeSingular[]>();
      cy.nodes().forEach((node) => {
        const layer = node.data("relativeLayer") as number;
        if (layer !== undefined) {
          if (!layerNodes.has(layer)) layerNodes.set(layer, []);
          layerNodes.get(layer)!.push(node);
        }
      });

      if (layerNodes.size === 0) return;

      // Get viewport transform
      const pan = cy.pan();
      const zoom = cy.zoom();

      // Draw each layer's swimlane
      const sortedLayers = [...layerNodes.keys()].sort((a, b) => a - b);

      for (const layer of sortedLayers) {
        const nodesInLayer = layerNodes.get(layer)!;
        if (nodesInLayer.length === 0) continue;

        // Calculate bounding box for this layer
        let minX = Infinity,
          maxX = -Infinity;
        let minY = Infinity,
          maxY = -Infinity;

        for (const node of nodesInLayer) {
          const pos = node.position();
          const width = node.width();
          const height = node.height();
          minX = Math.min(minX, pos.x - width / 2);
          maxX = Math.max(maxX, pos.x + width / 2);
          minY = Math.min(minY, pos.y - height / 2);
          maxY = Math.max(maxY, pos.y + height / 2);
        }

        // Add padding
        const padding = SWIMLANE_CONFIG.padding;
        minX -= padding;
        maxX += padding;
        minY -= padding + SWIMLANE_CONFIG.headerHeight;
        maxY += padding;

        // Transform to screen coordinates
        const screenMinX = minX * zoom + pan.x;
        const screenMaxX = maxX * zoom + pan.x;
        const screenMinY = minY * zoom + pan.y;
        const screenMaxY = maxY * zoom + pan.y;

        const width = Math.max(screenMaxX - screenMinX, SWIMLANE_CONFIG.minWidth * zoom);
        const height = screenMaxY - screenMinY;

        // Draw background
        ctx.fillStyle = SWIMLANE_CONFIG.backgroundColor;
        ctx.fillRect(screenMinX, screenMinY, width, height);

        // Draw border
        ctx.strokeStyle = SWIMLANE_CONFIG.borderColor;
        ctx.lineWidth = 1;
        ctx.strokeRect(screenMinX, screenMinY, width, height);

        // Draw header
        const headerY = screenMinY + SWIMLANE_CONFIG.headerHeight * zoom * 0.7;
        ctx.fillStyle = SWIMLANE_CONFIG.textColor;
        ctx.font = `${12 * zoom}px system-ui, sans-serif`;
        ctx.textAlign = "center";
        
        // Use smart layer name if available, otherwise fall back to generic
        const smartName = smartLayerNamesRef.current?.[layer];
        const layerLabel = smartName?.name || getLayerName(layer);
        
        ctx.fillText(
          `${layerLabel} (${nodesInLayer.length})`,
          screenMinX + width / 2,
          headerY
        );
      }
    }, []);

    // Initialize Cytoscape
    useEffect(() => {
      if (!containerRef.current) return;

      const elements = buildElements();

      const cy = cytoscape({
        container: containerRef.current,
        elements,
        style: [
          // Node styles - using pre-computed positions, no layout needed
          {
            selector: "node",
            style: {
              "background-color": "data(bg)",
              "border-color": "data(border)",
              "border-width": 2,
              label: "data(label)",
              color: "data(text)",
              "font-size": 11,
              "text-valign": "center",
              "text-halign": "center",
              "text-wrap": "ellipsis",
              "text-max-width": "120px",
              width: 140,
              height: 45,
              shape: "round-rectangle",
              "text-outline-color": "#0a0a0f",
              "text-outline-width": 1,
            },
          },
          // Anchor node (selected)
          {
            selector: ".anchor-node",
            style: {
              "border-width": 3,
              "border-color": "#ffffff",
              "background-color": LAYER_COLORS.anchor.bg,
            },
          },
          // Ghost nodes (outside flow)
          {
            selector: ".node-ghost",
            style: {
              opacity: 0.3,
              "border-style": "dashed",
            },
          },
          // Edge styles
          {
            selector: "edge",
            style: {
              width: 1.5,
              "line-color": "#4b5563",
              "target-arrow-color": "#4b5563",
              "target-arrow-shape": "triangle",
              "curve-style": "bezier",
              opacity: 0.6,
            },
          },
          {
            selector: "edge.edge-ref, edge.edge-source",
            style: {
              "line-color": "#10b981",
              "target-arrow-color": "#10b981",
              width: 2,
            },
          },
          {
            selector: "edge.edge-sql_dependency",
            style: {
              "line-color": "#6366f1",
              "target-arrow-color": "#6366f1",
            },
          },
          {
            selector: "edge.edge-dag_edge",
            style: {
              "line-color": "#f59e0b",
              "target-arrow-color": "#f59e0b",
              "line-style": "dashed",
            },
          },
          // Selected node
          {
            selector: "node:selected",
            style: {
              "border-width": 3,
              "border-color": "#ffffff",
            },
          },
          // Highlighted nodes
          {
            selector: ".highlighted",
            style: {
              opacity: 1,
            },
          },
          {
            selector: ".dimmed",
            style: {
              opacity: 0.25,
            },
          },
        ],
        // Use preset layout since positions are pre-computed
        layout: { name: "preset" },
        minZoom: 0.1,
        maxZoom: 3,
        wheelSensitivity: 0.3,
      });

      // Tooltip element
      const tooltip = document.createElement("div");
      tooltip.className = "cy-tooltip";
      tooltip.style.cssText = `
        position: absolute;
        background: rgba(0, 0, 0, 0.9);
        color: white;
        padding: 8px 12px;
        border-radius: 6px;
        font-size: 12px;
        pointer-events: none;
        z-index: 1000;
        display: none;
        max-width: 300px;
        border: 1px solid rgba(255, 255, 255, 0.1);
      `;
      containerRef.current?.appendChild(tooltip);

      // Event handlers
      cy.on("tap", "node:not(.node-ghost)", (evt) => {
        const node = evt.target as NodeSingular;
        const currentNodes = nodesRef.current;
        const nodeData = currentNodes.find((n) => n.id === node.id());
        onNodeSelectRef.current?.(nodeData || null);

        // Highlight neighborhood
        cy.elements().removeClass("highlighted dimmed");
        const neighborhood = node.closedNeighborhood();
        cy.elements().addClass("dimmed");
        neighborhood.removeClass("dimmed").addClass("highlighted");
      });

      // Ghost node click
      cy.on("tap", ".node-ghost", (evt) => {
        const node = evt.target as NodeSingular;
        const currentGhostNodes = ghostNodesRef.current;
        const nodeData = currentGhostNodes.find((n) => n.id === node.id());
        if (nodeData) {
          onGhostNodeClickRef.current?.(nodeData);
        }
      });

      cy.on("dbltap", "node:not(.node-ghost)", (evt) => {
        const node = evt.target as NodeSingular;
        const currentNodes = nodesRef.current;
        const nodeData = currentNodes.find((n) => n.id === node.id());
        if (nodeData) {
          onNodeDoubleClickRef.current?.(nodeData);
        }
      });

      cy.on("tap", (evt) => {
        if (evt.target === cy) {
          onNodeSelectRef.current?.(null);
          cy.elements().removeClass("highlighted dimmed");
        }
      });

      // Tooltip on hover
      cy.on("mouseover", "node", (evt) => {
        const node = evt.target;
        const data = node.data();

        let content = `<strong>${data.label}</strong>`;
        if (data.nodeType) content += `<br/>Type: ${data.nodeType}`;
        if (data.relativeLayer !== undefined) {
          // Use smart layer name if available
          const smartName = smartLayerNamesRef.current?.[data.relativeLayer];
          const layerName = smartName?.name || getLayerName(data.relativeLayer);
          content += `<br/>Layer: ${layerName} (${data.relativeLayer})`;
        }
        if (data.isGhost) {
          content += `<br/><em style="color: #fbbf24">Outside current flow</em>`;
        }

        tooltip.innerHTML = content;
        tooltip.style.display = "block";
      });

      cy.on("mouseout", "node, edge", () => {
        tooltip.style.display = "none";
      });

      cy.on("mousemove", (evt) => {
        if (tooltip.style.display === "block") {
          tooltip.style.left = `${evt.originalEvent.offsetX + 10}px`;
          tooltip.style.top = `${evt.originalEvent.offsetY + 10}px`;
        }
      });

      // Redraw swimlanes on viewport change
      cy.on("pan zoom resize", () => {
        requestAnimationFrame(drawSwimlanes);
      });

      // Initial swimlane draw after layout settles
      setTimeout(() => {
        drawSwimlanes();
        // Fit to content
        cy.fit(undefined, 50);
      }, 100);

      cyRef.current = cy;

      return () => {
        tooltip.remove();
        cy.destroy();
      };
    }, []); // Only run once on mount

    // Update elements when data changes
    useEffect(() => {
      if (!cyRef.current) return;

      const cy = cyRef.current;
      const elements = buildElements();

      // Batch update: remove old, add new
      cy.elements().remove();
      cy.add(elements);

      // No layout needed - positions are pre-computed
      // Just redraw swimlanes
      requestAnimationFrame(drawSwimlanes);

      // Fit to new content with animation
      cy.animate({ fit: { eles: cy.elements(), padding: 50 } }, { duration: 300 });
    }, [buildElements, drawSwimlanes]);

    // Zoom controls
    const zoomIn = () => cyRef.current?.zoom(cyRef.current.zoom() * 1.2);
    const zoomOut = () => cyRef.current?.zoom(cyRef.current.zoom() / 1.2);
    const fitToScreen = () => cyRef.current?.fit(undefined, 50);

    return (
      <div className="relative w-full h-full">
        {/* Swimlane canvas (behind graph) */}
        <canvas
          ref={swimlaneCanvasRef}
          className="absolute inset-0 pointer-events-none"
          style={{ zIndex: 0 }}
        />

        {/* Cytoscape container */}
        <div
          ref={containerRef}
          className="w-full h-full bg-[#0a0a0f]"
          style={{ zIndex: 1 }}
        />

        {/* Zoom Controls */}
        <div className="absolute bottom-4 right-4 flex flex-col gap-2" style={{ zIndex: 10 }}>
          <button
            onClick={zoomIn}
            className="w-10 h-10 rounded-lg bg-white/10 hover:bg-white/20 flex items-center justify-center transition-colors"
            title="Zoom In"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v12M6 12h12" />
            </svg>
          </button>
          <button
            onClick={zoomOut}
            className="w-10 h-10 rounded-lg bg-white/10 hover:bg-white/20 flex items-center justify-center transition-colors"
            title="Zoom Out"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 12h12" />
            </svg>
          </button>
          <button
            onClick={fitToScreen}
            className="w-10 h-10 rounded-lg bg-white/10 hover:bg-white/20 flex items-center justify-center transition-colors"
            title="Fit to Screen (F)"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
            </svg>
          </button>
        </div>

        {/* Legend */}
        <div className="absolute bottom-4 left-4 bg-black/60 backdrop-blur-sm rounded-lg p-3 text-xs space-y-1.5" style={{ zIndex: 10 }}>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_COLORS.source.border }} />
            <span className="text-white/70">Source / Seed</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_COLORS.model.border }} />
            <span className="text-white/70">Model / Table</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_COLORS.view.border }} />
            <span className="text-white/70">View</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_COLORS.external.border }} />
            <span className="text-white/70">External</span>
          </div>
          <div className="border-t border-white/10 my-2" />
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded border-2 border-white" style={{ backgroundColor: LAYER_COLORS.anchor.bg }} />
            <span className="text-white/70">Selected (Anchor)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded border border-dashed border-white/40 opacity-40" />
            <span className="text-white/70">Ghost (outside flow)</span>
          </div>
        </div>

        {/* Keyboard shortcuts hint */}
        <div className="absolute bottom-4 left-52 text-xs text-white/30" style={{ zIndex: 10 }}>
          Press <kbd className="px-1.5 py-0.5 bg-white/10 rounded">Esc</kbd> to deselect ·
          <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">/</kbd> to search ·
          <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">F</kbd> to fit
        </div>
      </div>
    );
  }
);

export default GraphExplorer;
