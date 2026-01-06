"use client";

import { useEffect, useRef } from "react";

interface MiniMapProps {
  nodeCount: number;
  edgeCount: number;
  selectedNode?: string;
  visibleArea?: { x: number; y: number; width: number; height: number };
}

export default function MiniMap({
  nodeCount,
  edgeCount,
  selectedNode,
}: MiniMapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Clear
    ctx.fillStyle = "#0a0a0f";
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    // Draw border
    ctx.strokeStyle = "#3f3f5f";
    ctx.lineWidth = 1;
    ctx.strokeRect(0, 0, canvas.width, canvas.height);

    // Draw placeholder nodes (simplified representation)
    const nodeRadius = 2;
    const rows = Math.ceil(Math.sqrt(nodeCount));
    const cols = Math.ceil(nodeCount / rows);
    const cellWidth = canvas.width / cols;
    const cellHeight = canvas.height / rows;

    ctx.fillStyle = "#10b981";
    for (let i = 0; i < Math.min(nodeCount, 200); i++) {
      const row = Math.floor(i / cols);
      const col = i % cols;
      const x = col * cellWidth + cellWidth / 2;
      const y = row * cellHeight + cellHeight / 2;
      
      ctx.beginPath();
      ctx.arc(x, y, nodeRadius, 0, Math.PI * 2);
      ctx.fill();
    }

    // Draw viewport indicator
    ctx.strokeStyle = "#ffffff";
    ctx.lineWidth = 2;
    ctx.strokeRect(canvas.width * 0.2, canvas.height * 0.2, canvas.width * 0.6, canvas.height * 0.6);
  }, [nodeCount, selectedNode]);

  return (
    <div className="w-32 h-24 bg-black/60 backdrop-blur-sm rounded-lg overflow-hidden">
      <canvas ref={canvasRef} width={128} height={96} className="w-full h-full" />
    </div>
  );
}

