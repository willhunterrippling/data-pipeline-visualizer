"use client";

import {
  useEffect,
  useRef,
  useState,
  useCallback,
  useImperativeHandle,
  forwardRef,
  useMemo,
} from "react";
import cytoscape, { Core, NodeSingular } from "cytoscape";
import type { GraphNode, GraphEdge } from "@/lib/types";
import type { VisibilityReason } from "@/lib/graph/visibility";
import type { SmartLayerName } from "@/lib/graph/layout";

// Layout constants for computing positions client-side
const CLIENT_LAYOUT_CONFIG = {
  nodeSep: 60,      // Vertical spacing between nodes in same layer
  rankSep: 200,     // Horizontal spacing between layers
  marginX: 50,
  marginY: 50,
  nodeHeight: 50,
};

// Cached position for a node
interface CachedPosition {
  x: number;
  y: number;
  relativeLayer: number;
}

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
  anchorId?: string | null;
  layerRange: { min: number; max: number };
  smartLayerNames?: Record<number, SmartLayerName>;  // Smart layer names from API
  onNodeSelect?: (node: GraphNode | null) => void;
  onNodeDoubleClick?: (node: GraphNode) => void;
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
      anchorId,
      layerRange,
      smartLayerNames,
      onNodeSelect,
      onNodeDoubleClick,
    },
    ref
  ) {
    const containerRef = useRef<HTMLDivElement>(null);
    const cyRef = useRef<Core | null>(null);
    const swimlaneCanvasRef = useRef<HTMLCanvasElement>(null);
    
    // Position cache to preserve node positions across updates
    const positionCacheRef = useRef<Map<string, CachedPosition>>(new Map());
    // Track selected node to restore selection after updates
    const selectedNodeIdRef = useRef<string | null>(null);
    // Track anchor to clear cache when it changes
    const prevAnchorIdRef = useRef<string | null | undefined>(anchorId);
    
    // Clear position cache when anchor changes (new exploration starting point)
    useEffect(() => {
      if (anchorId !== prevAnchorIdRef.current) {
        positionCacheRef.current.clear();
        prevAnchorIdRef.current = anchorId;
      }
    }, [anchorId]);

    // Keep refs for stable access in event handlers
    const nodesRef = useRef(nodes);
    const smartLayerNamesRef = useRef(smartLayerNames);
    const onNodeSelectRef = useRef(onNodeSelect);
    const onNodeDoubleClickRef = useRef(onNodeDoubleClick);

    useEffect(() => {
      nodesRef.current = nodes;
      smartLayerNamesRef.current = smartLayerNames;
      onNodeSelectRef.current = onNodeSelect;
      onNodeDoubleClickRef.current = onNodeDoubleClick;
    }, [nodes, smartLayerNames, onNodeSelect, onNodeDoubleClick]);

    // Compute stable positions: preserve existing positions, only compute for truly new nodes
    // Priority: 1) Cached position at same layer, 2) Server-provided position, 3) Compute new position
    const computeStablePositions = useCallback((
      nodeList: VisibleNode[],
      cache: Map<string, CachedPosition>,
      edgeList: GraphEdge[]
    ): Map<string, { x: number; y: number }> => {
      const positions = new Map<string, { x: number; y: number }>();
      
      // Group nodes by relativeLayer for computing truly new positions
      const nodesByLayer = new Map<number, VisibleNode[]>();
      const trulyNewNodesByLayer = new Map<number, VisibleNode[]>();
      
      for (const node of nodeList) {
        const layer = node.relativeLayer;
        if (!nodesByLayer.has(layer)) {
          nodesByLayer.set(layer, []);
          trulyNewNodesByLayer.set(layer, []);
        }
        nodesByLayer.get(layer)!.push(node);
        
        // Priority 1: Check if we have a cached position AT THE SAME LAYER
        // If layer changed, the node needs to be repositioned
        const cached = cache.get(node.id);
        if (cached && cached.relativeLayer === layer) {
          // Use cached position - layer matches, preserve visual position
          positions.set(node.id, { x: cached.x, y: cached.y });
        } 
        // Priority 2: Use server-provided position if available (layer changed or no cache)
        else if (node.layoutX !== undefined && node.layoutY !== undefined) {
          positions.set(node.id, { x: node.layoutX, y: node.layoutY });
          cache.set(node.id, { x: node.layoutX, y: node.layoutY, relativeLayer: layer });
        }
        // Priority 3: New node - needs computed position
        else {
          trulyNewNodesByLayer.get(layer)!.push(node);
        }
      }
      
      // Find the range of X positions from existing cached nodes to position new nodes relative to them
      let minExistingX = Infinity;
      let maxExistingX = -Infinity;
      for (const pos of positions.values()) {
        minExistingX = Math.min(minExistingX, pos.x);
        maxExistingX = Math.max(maxExistingX, pos.x);
      }
      if (minExistingX === Infinity) minExistingX = CLIENT_LAYOUT_CONFIG.marginX;
      if (maxExistingX === -Infinity) maxExistingX = CLIENT_LAYOUT_CONFIG.marginX;
      
      // For each layer, compute positions for truly new nodes
      // Place them to the LEFT (upstream) or RIGHT (downstream) of existing nodes
      for (const [layer, newNodes] of trulyNewNodesByLayer) {
        if (newNodes.length === 0) continue;
        
        // Find existing Y positions in this layer
        const existingYs: number[] = [];
        for (const node of nodesByLayer.get(layer)!) {
          const pos = positions.get(node.id);
          if (pos) {
            existingYs.push(pos.y);
          }
        }
        
        // Sort new nodes alphabetically for consistent ordering
        newNodes.sort((a, b) => a.name.localeCompare(b.name));
        
        // Compute X position: place new nodes to the left of existing for upstream (negative layer)
        // or to the right for downstream (positive layer)
        let layoutX: number;
        
        if (layer < 0) {
          // Upstream: place to the LEFT of minimum existing X
          layoutX = minExistingX - CLIENT_LAYOUT_CONFIG.rankSep;
        } else if (layer > 0) {
          // Downstream: place to the RIGHT of maximum existing X  
          layoutX = maxExistingX + CLIENT_LAYOUT_CONFIG.rankSep;
        } else {
          // Anchor layer: use existing position or compute
          layoutX = CLIENT_LAYOUT_CONFIG.marginX;
        }
        
        // Build edge lookup for finding connected nodes
        const nodeConnections = new Map<string, string[]>();
        for (const edge of edgeList) {
          // For upstream nodes (layer < 0), we want to find what this node flows TO
          // For downstream nodes (layer > 0), we want to find what flows TO this node
          if (!nodeConnections.has(edge.from)) nodeConnections.set(edge.from, []);
          if (!nodeConnections.has(edge.to)) nodeConnections.set(edge.to, []);
          nodeConnections.get(edge.from)!.push(edge.to);
          nodeConnections.get(edge.to)!.push(edge.from);
        }
        
        // Compute Y positions for new nodes based on their connected node's Y position
        // Track used Y positions in this layer to avoid stacking
        const usedYPositions = new Set<number>(existingYs);
        let fallbackY = CLIENT_LAYOUT_CONFIG.marginY;
        if (existingYs.length > 0) {
          fallbackY = Math.max(...existingYs) + CLIENT_LAYOUT_CONFIG.nodeHeight + CLIENT_LAYOUT_CONFIG.nodeSep;
        }
        
        for (const node of newNodes) {
          // Find the connected node that already has a position
          const connectedIds = nodeConnections.get(node.id) || [];
          let nodeY = fallbackY;
          
          for (const connectedId of connectedIds) {
            const connectedPos = positions.get(connectedId);
            if (connectedPos) {
              // Try to use the connected node's Y position if not already taken
              let candidateY = connectedPos.y;
              while (usedYPositions.has(candidateY)) {
                candidateY += CLIENT_LAYOUT_CONFIG.nodeHeight + CLIENT_LAYOUT_CONFIG.nodeSep;
              }
              nodeY = candidateY;
              break;
            }
          }
          
          usedYPositions.add(nodeY);
          positions.set(node.id, { x: layoutX, y: nodeY });
          cache.set(node.id, { x: layoutX, y: nodeY, relativeLayer: layer });
          fallbackY = Math.max(fallbackY, nodeY + CLIENT_LAYOUT_CONFIG.nodeHeight + CLIENT_LAYOUT_CONFIG.nodeSep);
        }
      }
      
      return positions;
    }, []); // No dependencies

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

    // Compute stable positions for all nodes
    const stablePositions = useMemo(() => {
      return computeStablePositions(nodes, positionCacheRef.current, edges);
    }, [nodes, edges, computeStablePositions]);

    // Build Cytoscape elements using stable cached positions
    const buildElements = useCallback(() => {
      const elements: cytoscape.ElementDefinition[] = [];

      // Add visible nodes with stable positions
      for (const node of nodes) {
        const colors = NODE_COLORS[node.type] || NODE_COLORS.model;
        const isAnchor = node.id === anchorId;
        
        // Break label at __ for multi-line display
        const label = node.name.replace(/__/g, '\n');
        
        // Use stable position from cache, fallback to server position
        const pos = stablePositions.get(node.id) || { x: node.layoutX ?? 0, y: node.layoutY ?? 0 };

        elements.push({
          data: {
            id: node.id,
            label,
            nodeType: node.type,
            subtype: node.subtype,
            relativeLayer: node.relativeLayer,
            isAnchor,
            ...colors,
          },
          position: {
            x: pos.x,
            y: pos.y,
          },
          classes: `node-${node.type}${isAnchor ? " anchor-node" : ""}`,
        });
      }

      // Add edges
      const allNodeIds = new Set(nodes.map((n) => n.id));

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
    }, [nodes, edges, anchorId, stablePositions]);

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
              "font-size": 10,
              "text-valign": "center",
              "text-halign": "center",
              "text-wrap": "wrap",
              "text-max-width": "140px",
              width: 160,
              height: 50,
              shape: "round-rectangle",
              "text-outline-color": "#0a0a0f",
              "text-outline-width": 1,
            },
          },
          // Anchor node - purple with moderate glow
          {
            selector: ".anchor-node",
            style: {
              "border-width": 3,
              "border-color": "#c084fc",
              "background-color": "#2e1a3d",
              color: "#e9d5ff",
              // Moderate glow
              "underlay-color": "#a855f7",
              "underlay-padding": 8,
              "underlay-opacity": 0.3,
              "underlay-shape": "round-rectangle",
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
          // Selected/viewed node - red with glow (but not anchor)
          {
            selector: "node:selected:not(.anchor-node)",
            style: {
              "border-width": 3,
              "border-color": "#f87171",
              "underlay-color": "#ef4444",
              "underlay-padding": 8,
              "underlay-opacity": 0.3,
              "underlay-shape": "round-rectangle",
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
      cy.on("tap", "node", (evt) => {
        const node = evt.target as NodeSingular;
        const currentNodes = nodesRef.current;
        const nodeData = currentNodes.find((n) => n.id === node.id());
        
        // Track selected node ID for restoration after updates
        selectedNodeIdRef.current = node.id();
        
        onNodeSelectRef.current?.(nodeData || null);

        // Highlight neighborhood
        cy.elements().removeClass("highlighted dimmed");
        const neighborhood = node.closedNeighborhood();
        cy.elements().addClass("dimmed");
        neighborhood.removeClass("dimmed").addClass("highlighted");
      });

      cy.on("dbltap", "node", (evt) => {
        const node = evt.target as NodeSingular;
        const currentNodes = nodesRef.current;
        const nodeData = currentNodes.find((n) => n.id === node.id());
        if (nodeData) {
          onNodeDoubleClickRef.current?.(nodeData);
        }
      });

      cy.on("tap", (evt) => {
        if (evt.target === cy) {
          // Clear tracked selection
          selectedNodeIdRef.current = null;
          onNodeSelectRef.current?.(null);
          cy.elements().removeClass("highlighted dimmed");
        }
      });

      // Tooltip on hover
      cy.on("mouseover", "node", (evt) => {
        const node = evt.target;
        const data = node.data();

        let content = `<strong>${data.label}</strong>`;
        if (data.isAnchor) {
          content += `<br/><span style="color: #c084fc">⚓ Anchor</span>`;
        }
        if (data.nodeType) content += `<br/>Type: ${data.nodeType}`;
        if (data.relativeLayer !== undefined) {
          // Use smart layer name if available
          const smartName = smartLayerNamesRef.current?.[data.relativeLayer];
          const layerName = smartName?.name || getLayerName(data.relativeLayer);
          content += `<br/>Layer: ${layerName} (${data.relativeLayer})`;
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

      cyRef.current = cy;

      // Initial swimlane draw after layout settles
      const initTimeout = setTimeout(() => {
        if (cyRef.current) {
          drawSwimlanes();
          // Fit to content
          cyRef.current.fit(undefined, 50);
        }
      }, 100);

      return () => {
        clearTimeout(initTimeout);
        tooltip.remove();
        cy.destroy();
        cyRef.current = null;
      };
    }, []); // Only run once on mount

    // Update elements incrementally when data changes
    useEffect(() => {
      if (!cyRef.current) return;

      const cy = cyRef.current;
      const newElements = buildElements();
      
      // Capture current selected node before updating
      const selectedNodes = cy.nodes(":selected");
      const wasSelectedId = selectedNodes.length > 0 ? selectedNodes[0].id() : selectedNodeIdRef.current;
      
      // Build maps of current and new elements
      const currentNodeIds = new Set<string>();
      const currentEdgeIds = new Set<string>();
      cy.nodes().forEach(n => currentNodeIds.add(n.id()));
      cy.edges().forEach(e => currentEdgeIds.add(e.id()));
      
      const newNodeMap = new Map<string, cytoscape.ElementDefinition>();
      const newEdgeMap = new Map<string, cytoscape.ElementDefinition>();
      const newNodeIds = new Set<string>();
      const newEdgeIds = new Set<string>();
      
      for (const el of newElements) {
        if (el.data.source && el.data.target) {
          // It's an edge
          newEdgeMap.set(el.data.id, el);
          newEdgeIds.add(el.data.id);
        } else {
          // It's a node
          newNodeMap.set(el.data.id, el);
          newNodeIds.add(el.data.id);
        }
      }
      
      // Find nodes to remove, add, and update
      const nodesToRemove: string[] = [];
      const nodesToAdd: cytoscape.ElementDefinition[] = [];
      const nodesToUpdate: cytoscape.ElementDefinition[] = [];
      const addedNodeIds: string[] = [];
      
      // Check for nodes to remove
      for (const id of currentNodeIds) {
        if (!newNodeIds.has(id)) {
          nodesToRemove.push(id);
        }
      }
      
      // Check for nodes to add or update
      for (const [id, el] of newNodeMap) {
        if (!currentNodeIds.has(id)) {
          nodesToAdd.push(el);
          addedNodeIds.push(id);
        } else {
          nodesToUpdate.push(el);
        }
      }
      
      // Find edges to remove and add
      const edgesToRemove: string[] = [];
      const edgesToAdd: cytoscape.ElementDefinition[] = [];
      
      for (const id of currentEdgeIds) {
        if (!newEdgeIds.has(id)) {
          edgesToRemove.push(id);
        }
      }
      
      for (const [id, el] of newEdgeMap) {
        if (!currentEdgeIds.has(id)) {
          edgesToAdd.push(el);
        }
      }
      
      // Perform batch update
      cy.batch(() => {
        // Remove old elements
        for (const id of nodesToRemove) {
          cy.getElementById(id).remove();
        }
        for (const id of edgesToRemove) {
          cy.getElementById(id).remove();
        }
        
        // Add new elements
        cy.add([...nodesToAdd, ...edgesToAdd]);
        
        // Update existing node positions (they may have moved)
        for (const el of nodesToUpdate) {
          const node = cy.getElementById(el.data.id);
          if (node.length > 0 && el.position) {
            node.position(el.position);
          }
        }
      });
      
      // Restore selection if the node still exists
      if (wasSelectedId && newNodeIds.has(wasSelectedId)) {
        const nodeToSelect = cy.getElementById(wasSelectedId);
        if (nodeToSelect.length > 0) {
          nodeToSelect.select();
          // Re-apply highlighting
          cy.elements().removeClass("highlighted dimmed");
          const neighborhood = nodeToSelect.closedNeighborhood();
          cy.elements().addClass("dimmed");
          neighborhood.removeClass("dimmed").addClass("highlighted");
        }
      }
      
      // Redraw swimlanes
      requestAnimationFrame(drawSwimlanes);
      
      // Smart pan: if new nodes were added, pan to include them without refitting everything
      if (addedNodeIds.length > 0 && nodesToRemove.length === 0) {
        // Get bounding box of new nodes
        const addedNodes = cy.nodes().filter(n => addedNodeIds.includes(n.id()));
        if (addedNodes.length > 0) {
          const bb = addedNodes.boundingBox({});
          const viewport = {
            x1: -cy.pan().x / cy.zoom(),
            y1: -cy.pan().y / cy.zoom(),
            x2: (-cy.pan().x + cy.width()) / cy.zoom(),
            y2: (-cy.pan().y + cy.height()) / cy.zoom(),
          };
          
          // Check if new nodes are outside viewport
          const outsideLeft = bb.x1 < viewport.x1;
          const outsideRight = bb.x2 > viewport.x2;
          const outsideTop = bb.y1 < viewport.y1;
          const outsideBottom = bb.y2 > viewport.y2;
          
          if (outsideLeft || outsideRight || outsideTop || outsideBottom) {
            // Pan to include new nodes with some padding
            let panX = cy.pan().x;
            let panY = cy.pan().y;
            const padding = 100 * cy.zoom();
            
            if (outsideLeft) {
              panX = -(bb.x1 * cy.zoom()) + padding;
            } else if (outsideRight) {
              panX = cy.width() - (bb.x2 * cy.zoom()) - padding;
            }
            
            if (outsideTop) {
              panY = -(bb.y1 * cy.zoom()) + padding;
            } else if (outsideBottom) {
              panY = cy.height() - (bb.y2 * cy.zoom()) - padding;
            }
            
            cy.animate({
              pan: { x: panX, y: panY }
            }, { duration: 300 });
          }
        }
      } else if (cy.nodes().length > 0 && currentNodeIds.size === 0) {
        // Initial load - fit to content
        cy.fit(undefined, 50);
      }
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
            <div 
              className="w-3 h-3 rounded border-2" 
              style={{ 
                backgroundColor: "#2e1a3d", 
                borderColor: "#c084fc",
                boxShadow: "0 0 6px 2px rgba(168, 85, 247, 0.4)"
              }} 
            />
            <span className="text-purple-300">Anchor</span>
          </div>
          <div className="flex items-center gap-2">
            <div 
              className="w-3 h-3 rounded border-2" 
              style={{ 
                backgroundColor: "#1e4035", 
                borderColor: "#f87171",
                boxShadow: "0 0 6px 2px rgba(239, 68, 68, 0.4)"
              }} 
            />
            <span className="text-red-300">Selected</span>
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

