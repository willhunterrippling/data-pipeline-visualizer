"use client";

import { useState, useRef, useEffect } from "react";
import type { GraphFlow } from "@/lib/types";

interface FlowSelectorProps {
  flows: GraphFlow[];
  selectedFlow?: string;
  onSelect: (flowId: string | undefined) => void;
}

export default function FlowSelector({
  flows,
  selectedFlow,
  onSelect,
}: FlowSelectorProps) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const selectedFlowData = flows.find((f) => f.id === selectedFlow);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 px-4 py-2 rounded-lg bg-white/10 hover:bg-white/20 transition-colors"
      >
        <svg className="w-4 h-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
        </svg>
        <span className="text-sm font-medium">
          {selectedFlowData ? selectedFlowData.name : "All Flows"}
        </span>
        <svg
          className={`w-4 h-4 transition-transform ${isOpen ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 mt-2 w-80 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-xl z-50 overflow-hidden animate-slide-up">
          <div className="p-2">
            <button
              onClick={() => {
                onSelect(undefined);
                setIsOpen(false);
              }}
              className={`w-full text-left px-3 py-2 rounded-lg transition-colors ${
                !selectedFlow
                  ? "bg-white/10 text-white"
                  : "text-white/70 hover:bg-white/5 hover:text-white"
              }`}
            >
              <div className="font-medium">All Flows</div>
              <div className="text-xs text-white/50">Show entire graph</div>
            </button>
          </div>

          <div className="border-t border-white/10" />

          <div className="p-2 max-h-80 overflow-y-auto">
            {flows.map((flow) => (
              <button
                key={flow.id}
                onClick={() => {
                  onSelect(flow.id);
                  setIsOpen(false);
                }}
                className={`w-full text-left px-3 py-2 rounded-lg transition-colors ${
                  selectedFlow === flow.id
                    ? "bg-cyan-500/20 text-cyan-300"
                    : "text-white/70 hover:bg-white/5 hover:text-white"
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="font-medium">{flow.name}</span>
                  <span className="text-xs bg-white/10 px-2 py-0.5 rounded-full">
                    {flow.memberNodes.length} nodes
                  </span>
                </div>
                {flow.description && (
                  <div className="text-xs text-white/50 mt-1 line-clamp-2">
                    {flow.description}
                  </div>
                )}
              </button>
            ))}
          </div>

          {flows.length === 0 && (
            <div className="p-4 text-center text-white/50 text-sm">
              No flows detected yet
            </div>
          )}
        </div>
      )}
    </div>
  );
}

