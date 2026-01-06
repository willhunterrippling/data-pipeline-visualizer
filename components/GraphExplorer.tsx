"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import cytoscape, { Core, NodeSingular } from "cytoscape";
import type { GraphNode, GraphEdge, GraphGroup, GraphFlow } from "@/lib/types";

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

export default function GraphExplorer({
  nodes,
  edges,
  groups,
  flows,
  selectedFlow,
  onNodeSelect,
  onNodeDoubleClick,
}: GraphExplorerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());

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
        },
        classes: `edge-${edge.type}`,
      });
    }

    return elements;
  }, [nodes, edges, groups, flows, selectedFlow, collapsedGroups]);

  // Initialize Cytoscape
  useEffect(() => {
    if (!containerRef.current) return;

    const cy = cytoscape({
      container: containerRef.current,
      elements: buildElements(),
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
      layout: {
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
        numIter: 1000,
        initialTemp: 200,
        coolingFactor: 0.95,
        minTemp: 1.0,
      },
      minZoom: 0.1,
      maxZoom: 3,
      wheelSensitivity: 0.3,
    });

    // Event handlers
    cy.on("tap", "node:not(.group-node)", (evt) => {
      const node = evt.target as NodeSingular;
      const nodeData = nodes.find((n) => n.id === node.id());
      onNodeSelect?.(nodeData || null);

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
      const nodeData = nodes.find((n) => n.id === node.id());
      if (nodeData) {
        onNodeDoubleClick?.(nodeData);
      }
    });

    cy.on("tap", (evt) => {
      if (evt.target === cy) {
        onNodeSelect?.(null);
        cy.elements().removeClass("highlighted dimmed");
      }
    });

    cyRef.current = cy;

    return () => {
      cy.destroy();
    };
  }, []); // Only run once on mount

  // Update elements when data changes
  useEffect(() => {
    if (!cyRef.current) return;

    const cy = cyRef.current;
    cy.elements().remove();
    cy.add(buildElements());
    
    cy.layout({
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
      numIter: 500,
      initialTemp: 200,
      coolingFactor: 0.95,
      minTemp: 1.0,
    }).run();
  }, [buildElements]);

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
          title="Fit to Screen"
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
      </div>
    </div>
  );
}

