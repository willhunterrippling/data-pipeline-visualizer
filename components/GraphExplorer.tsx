"use client";

import { useEffect, useRef, useState, useCallback, useImperativeHandle, forwardRef } from "react";
import cytoscape, { Core, NodeSingular } from "cytoscape";
import type { GraphNode, GraphEdge, GraphGroup, GraphFlow } from "@/lib/types";

export interface GraphExplorerRef {
  focusNode: (nodeId: string) => void;
  fitToScreen: () => void;
}

interface GraphExplorerProps {
  nodes: GraphNode[];
  edges: GraphEdge[];
  groups: GraphGroup[];
  flows: GraphFlow[];
  selectedFlow?: string;
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

const GROUP_COLORS = [
  { bg: "#1a1a2e", border: "#4a4a6a" },
  { bg: "#1e2a1e", border: "#4a6a4a" },
  { bg: "#2e1a1a", border: "#6a4a4a" },
  { bg: "#1a2e2e", border: "#4a6a6a" },
  { bg: "#2e2e1a", border: "#6a6a4a" },
];

const GraphExplorer = forwardRef<GraphExplorerRef, GraphExplorerProps>(function GraphExplorer(
  {
    nodes,
    edges,
    groups,
    flows,
    selectedFlow,
    onNodeSelect,
    onNodeDoubleClick,
  },
  ref
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  
  // Keep refs for stable access in event handlers (avoid stale closures)
  const nodesRef = useRef(nodes);
  const onNodeSelectRef = useRef(onNodeSelect);
  const onNodeDoubleClickRef = useRef(onNodeDoubleClick);
  
  // Update refs when props change
  useEffect(() => {
    nodesRef.current = nodes;
    onNodeSelectRef.current = onNodeSelect;
    onNodeDoubleClickRef.current = onNodeDoubleClick;
  }, [nodes, onNodeSelect, onNodeDoubleClick]);

  // Expose methods to parent
  useImperativeHandle(ref, () => ({
    focusNode: (nodeId: string) => {
      if (!cyRef.current) return;
      const node = cyRef.current.getElementById(nodeId);
      if (node.length > 0) {
        cyRef.current.animate({
          center: { eles: node },
          zoom: 1.5,
        }, {
          duration: 500,
        });
        node.select();
      }
    },
    fitToScreen: () => {
      cyRef.current?.fit(undefined, 50);
    },
  }));

  // Initialize collapsed groups from defaults
  useEffect(() => {
    const defaultCollapsed = new Set(
      groups.filter((g) => g.collapsedDefault).map((g) => g.id)
    );
    setCollapsedGroups(defaultCollapsed);
  }, [groups]);

  // Build Cytoscape elements
  const buildElements = useCallback(() => {
    const elements: cytoscape.ElementDefinition[] = [];
    const groupColorMap = new Map<string, typeof GROUP_COLORS[0]>();

    // Filter nodes/edges by flow if selected
    let filteredNodes = nodes;
    let filteredEdges = edges;
    
    if (selectedFlow) {
      const flow = flows.find((f) => f.id === selectedFlow);
      if (flow) {
        const memberSet = new Set(flow.memberNodes);
        filteredNodes = nodes.filter((n) => memberSet.has(n.id));
        filteredEdges = edges.filter(
          (e) => memberSet.has(e.from) && memberSet.has(e.to)
        );
      }
    }

    // Create group nodes (compound parents)
    groups.forEach((group, index) => {
      const color = GROUP_COLORS[index % GROUP_COLORS.length];
      groupColorMap.set(group.id, color);

      const groupNodes = filteredNodes.filter((n) => n.groupId === group.id);
      if (groupNodes.length === 0) return;

      elements.push({
        data: {
          id: `group_${group.id}`,
          label: `${group.name} (${groupNodes.length})`,
          isGroup: true,
          groupId: group.id,
          nodeCount: groupNodes.length,
          collapsed: collapsedGroups.has(group.id),
          description: group.description,
        },
        classes: "group-node",
      });
    });

    // Create node elements
    for (const node of filteredNodes) {
      const colors = NODE_COLORS[node.type] || NODE_COLORS.model;
      const isInCollapsedGroup = node.groupId && collapsedGroups.has(node.groupId);

      if (isInCollapsedGroup) continue; // Don't show nodes in collapsed groups

      elements.push({
        data: {
          id: node.id,
          label: node.name,
          parent: node.groupId ? `group_${node.groupId}` : undefined,
          nodeType: node.type,
          subtype: node.subtype,
          repo: node.repo,
          description: node.metadata?.description,
          schema: node.metadata?.schema,
          ...colors,
        },
        classes: `node-${node.type}`,
      });
    }

    // Create edge elements
    for (const edge of filteredEdges) {
      // Skip edges to/from nodes in collapsed groups
      const fromNode = filteredNodes.find((n) => n.id === edge.from);
      const toNode = filteredNodes.find((n) => n.id === edge.to);
      
      if (!fromNode || !toNode) continue;
      
      const fromCollapsed = fromNode.groupId && collapsedGroups.has(fromNode.groupId);
      const toCollapsed = toNode.groupId && collapsedGroups.has(toNode.groupId);
      
      if (fromCollapsed || toCollapsed) continue;

      elements.push({
        data: {
          id: edge.id,
          source: edge.from,
          target: edge.to,
          edgeType: edge.type,
          label: edge.type.replace("_", " "),
        },
        classes: `edge-${edge.type}`,
      });
    }

    return elements;
  }, [nodes, edges, groups, flows, selectedFlow, collapsedGroups]);

  // Determine appropriate layout based on graph size
  const getLayoutConfig = useCallback((nodeCount: number, isUpdate: boolean) => {
    // For very large graphs, use a simple grid layout (fast)
    if (nodeCount > 1000) {
      return {
        name: "grid",
        fit: true,
        padding: 50,
        avoidOverlap: true,
        condense: true,
        rows: Math.ceil(Math.sqrt(nodeCount)),
      };
    }
    // For medium graphs, use cose with reduced iterations
    return {
      name: "cose",
      idealEdgeLength: 100,
      nodeOverlap: 20,
      refresh: 20,
      fit: true,
      padding: 50,
      randomize: false,
      componentSpacing: 100,
      nodeRepulsion: 400000,
      edgeElasticity: 100,
      nestingFactor: 5,
      gravity: 80,
      numIter: isUpdate ? 100 : 300, // Reduced iterations
      initialTemp: 200,
      coolingFactor: 0.95,
      minTemp: 1.0,
    };
  }, []);

  // Initialize Cytoscape
  useEffect(() => {
    if (!containerRef.current) return;

    const elements = buildElements();
    const layoutConfig = getLayoutConfig(nodes.length, false);

    const cy = cytoscape({
      container: containerRef.current,
      elements: elements,
      style: [
        // Node styles
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
            "text-max-width": "100px",
            width: 120,
            height: 40,
            shape: "round-rectangle",
            "text-outline-color": "#0a0a0f",
            "text-outline-width": 1,
          },
        },
        // Group (compound) node styles
        {
          selector: ".group-node",
          style: {
            "background-color": "#12121a",
            "background-opacity": 0.8,
            "border-color": "#3f3f5f",
            "border-width": 2,
            "border-style": "dashed",
            label: "data(label)",
            color: "#9ca3af",
            "font-size": 14,
            "font-weight": "bold",
            "text-valign": "top",
            "text-halign": "center",
            "text-margin-y": 10,
            padding: "30px",
          },
        },
        // Collapsed group styles
        {
          selector: ".group-node[collapsed]",
          style: {
            width: 150,
            height: 60,
            "background-color": "#1a1a2e",
            "background-opacity": 1,
            "border-style": "solid",
            "text-valign": "center",
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
        // Highlighted nodes (neighbors)
        {
          selector: ".highlighted",
          style: {
            opacity: 1,
          },
        },
        {
          selector: ".dimmed",
          style: {
            opacity: 0.2,
          },
        },
      ],
      layout: layoutConfig,
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

    // Event handlers - use refs to avoid stale closures
    cy.on("tap", "node:not(.group-node)", (evt) => {
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

    cy.on("tap", ".group-node", (evt) => {
      const groupNode = evt.target as NodeSingular;
      const groupId = groupNode.data("groupId");
      
      setCollapsedGroups((prev) => {
        const next = new Set(prev);
        if (next.has(groupId)) {
          next.delete(groupId);
        } else {
          next.add(groupId);
        }
        return next;
      });
    });

    cy.on("dbltap", "node:not(.group-node)", (evt) => {
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
    cy.on("mouseover", "node:not(.group-node)", (evt) => {
      const node = evt.target;
      const data = node.data();
      
      let content = `<strong>${data.label}</strong>`;
      if (data.nodeType) content += `<br/>Type: ${data.nodeType}`;
      if (data.schema) content += `<br/>Schema: ${data.schema}`;
      if (data.description) content += `<br/>${data.description.substring(0, 100)}...`;
      
      tooltip.innerHTML = content;
      tooltip.style.display = "block";
    });

    cy.on("mouseover", "edge", (evt) => {
      const edge = evt.target;
      const data = edge.data();
      
      tooltip.innerHTML = `<strong>${data.edgeType?.replace("_", " ") || "dependency"}</strong>`;
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

    cyRef.current = cy;

    return () => {
      tooltip.remove();
      cy.destroy();
    };
  }, []); // Only run once on mount

  // Track if initial mount is complete to avoid duplicate layouts
  const isInitialMount = useRef(true);

  // Update elements when data changes (but skip initial mount since init already handles it)
  useEffect(() => {
    if (!cyRef.current) return;

    // Skip the first run - the init effect already set up elements and layout
    if (isInitialMount.current) {
      isInitialMount.current = false;
      return;
    }

    const cy = cyRef.current;
    cy.elements().remove();
    cy.add(buildElements());
    
    const layoutConfig = getLayoutConfig(cy.nodes().length, true);
    cy.layout(layoutConfig).run();
  }, [buildElements, getLayoutConfig]);

  // Zoom controls
  const zoomIn = () => cyRef.current?.zoom(cyRef.current.zoom() * 1.2);
  const zoomOut = () => cyRef.current?.zoom(cyRef.current.zoom() / 1.2);
  const fitToScreen = () => cyRef.current?.fit(undefined, 50);

  return (
    <div className="relative w-full h-full">
      <div ref={containerRef} className="w-full h-full bg-[#0a0a0f]" />
      
      {/* Zoom Controls */}
      <div className="absolute bottom-4 right-4 flex flex-col gap-2">
        <button
          onClick={zoomIn}
          className="w-10 h-10 rounded-lg bg-white/10 hover:bg-white/20 flex items-center justify-center transition-colors"
          title="Zoom In (or scroll)"
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
      <div className="absolute bottom-4 left-4 bg-black/60 backdrop-blur-sm rounded-lg p-3 text-xs space-y-1.5">
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
        <div className="text-white/40">Click group to expand/collapse</div>
        <div className="text-white/40">Double-click node for details</div>
      </div>

      {/* Keyboard shortcuts hint */}
      <div className="absolute top-4 left-4 text-xs text-white/30">
        Press <kbd className="px-1.5 py-0.5 bg-white/10 rounded">Esc</kbd> to deselect · 
        <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">/</kbd> to search · 
        <kbd className="px-1.5 py-0.5 bg-white/10 rounded ml-1">F</kbd> to fit
      </div>
    </div>
  );
});

export default GraphExplorer;
