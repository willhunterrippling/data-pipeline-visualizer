"use client";

import type { GraphNode, GraphFlow } from "@/lib/types";

interface OrientationHeaderProps {
  anchor: GraphNode | null;
  flow: GraphFlow | null;
  stats?: {
    visibleNodes: number;
    totalNodes: number;
    layerRange: { min: number; max: number };
  };
  onClearAnchor?: () => void;
  onClearFlow?: () => void;
  onAnchorClick?: () => void;
  onFlowClick?: () => void;
}

export default function OrientationHeader({
  anchor,
  flow,
  stats,
  onClearAnchor,
  onClearFlow,
  onAnchorClick,
  onFlowClick,
}: OrientationHeaderProps) {
  // Empty state
  if (!anchor && !flow) {
    return (
      <div className="px-4 py-2 bg-[#12121a] border-b border-white/10 flex items-center justify-center">
        <span className="text-white/40 text-sm">
          Select a table to explore its lineage
        </span>
      </div>
    );
  }

  return (
    <div className="px-4 py-2 bg-[#12121a] border-b border-white/10 flex items-center justify-between">
      <div className="flex items-center gap-2 text-sm">
        <span className="text-white/50">You are viewing:</span>

        {/* Anchor badge */}
        {anchor && (
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-purple-500/20 border border-purple-500/30">
            <button
              onClick={onAnchorClick}
              className="text-purple-300 hover:text-purple-200 font-medium transition-colors"
            >
              {anchor.name}
            </button>
            {onClearAnchor && (
              <button
                onClick={onClearAnchor}
                className="text-purple-300/60 hover:text-purple-200 transition-colors"
                title="Clear anchor"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            )}
          </span>
        )}

        {/* Flow context */}
        {flow && (
          <>
            <span className="text-white/40">within</span>
            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-cyan-500/20 border border-cyan-500/30">
              <button
                onClick={onFlowClick}
                className="text-cyan-300 hover:text-cyan-200 font-medium transition-colors"
              >
                {flow.name}
              </button>
              {onClearFlow && (
                <button
                  onClick={onClearFlow}
                  className="text-cyan-300/60 hover:text-cyan-200 transition-colors"
                  title="Clear flow filter"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              )}
            </span>
          </>
        )}

        {/* Global indicator when no flow */}
        {anchor && !flow && (
          <>
            <span className="text-white/40">—</span>
            <span className="text-white/40 text-xs">(Global lineage)</span>
          </>
        )}
      </div>

      {/* Stats */}
      {stats && (
        <div className="flex items-center gap-4 text-xs text-white/40">
          <span>
            {stats.visibleNodes} of {stats.totalNodes} nodes
          </span>
          <span>
            Layers: {stats.layerRange.min} to {stats.layerRange.max}
          </span>
        </div>
      )}
    </div>
  );
}

