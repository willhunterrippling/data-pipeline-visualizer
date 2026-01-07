"use client";

import { useState } from "react";

interface DepthControlProps {
  upstreamDepth: number;
  downstreamDepth: number;
  onUpstreamChange: (depth: number) => void;
  onDownstreamChange: (depth: number) => void;
  maxDepth?: number;
  linked?: boolean;
  onLinkedChange?: (linked: boolean) => void;
}

export default function DepthControl({
  upstreamDepth,
  downstreamDepth,
  onUpstreamChange,
  onDownstreamChange,
  maxDepth = 10,
  linked = true,
  onLinkedChange,
}: DepthControlProps) {
  const [isLinked, setIsLinked] = useState(linked);

  const handleUpstreamChange = (value: number) => {
    onUpstreamChange(value);
    if (isLinked) {
      onDownstreamChange(value);
    }
  };

  const handleDownstreamChange = (value: number) => {
    onDownstreamChange(value);
    if (isLinked) {
      onUpstreamChange(value);
    }
  };

  const toggleLinked = () => {
    const newLinked = !isLinked;
    setIsLinked(newLinked);
    onLinkedChange?.(newLinked);
  };

  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-white/5 rounded-lg">
      {/* Upstream control */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-white/60 flex items-center gap-1">
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Up
        </span>
        <input
          type="range"
          min={0}
          max={maxDepth}
          value={upstreamDepth}
          onChange={(e) => handleUpstreamChange(parseInt(e.target.value, 10))}
          className="w-16 h-1 bg-white/20 rounded-lg appearance-none cursor-pointer accent-emerald-500"
        />
        <span className="text-xs text-white/80 w-4 text-center">{upstreamDepth}</span>
      </div>

      {/* Link toggle */}
      <button
        onClick={toggleLinked}
        className={`p-1 rounded transition-colors ${
          isLinked ? "text-cyan-400 bg-cyan-500/20" : "text-white/40 hover:text-white/60"
        }`}
        title={isLinked ? "Depths are linked" : "Depths are independent"}
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          {isLinked ? (
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
            />
          ) : (
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M17 16l4-4m0 0l-4-4m4 4H7m6 4l-4-4m0 0l4-4m-4 4h14"
            />
          )}
        </svg>
      </button>

      {/* Downstream control */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-white/60 flex items-center gap-1">
          Down
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
          </svg>
        </span>
        <input
          type="range"
          min={0}
          max={maxDepth}
          value={downstreamDepth}
          onChange={(e) => handleDownstreamChange(parseInt(e.target.value, 10))}
          className="w-16 h-1 bg-white/20 rounded-lg appearance-none cursor-pointer accent-cyan-500"
        />
        <span className="text-xs text-white/80 w-4 text-center">{downstreamDepth}</span>
      </div>
    </div>
  );
}
