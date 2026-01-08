"use client";

import { useState, useEffect, useRef } from "react";
import type { GraphNode, GraphFlow } from "@/lib/types";

interface EditFlowModalProps {
  isOpen: boolean;
  flow: GraphFlow | null;
  allNodes: GraphNode[];
  onClose: () => void;
  onUpdated: (flow: GraphFlow) => void;
}

export default function EditFlowModal({
  isOpen,
  flow,
  allNodes,
  onClose,
  onUpdated,
}: EditFlowModalProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Anchor search state
  const [anchorQuery, setAnchorQuery] = useState("");
  const [anchorResults, setAnchorResults] = useState<GraphNode[]>([]);
  const [selectedAnchor, setSelectedAnchor] = useState<GraphNode | null>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Initialize with current anchor when modal opens
  useEffect(() => {
    if (isOpen && flow && flow.anchorNodes.length > 0) {
      const currentAnchorId = flow.anchorNodes[0];
      const currentAnchorNode = allNodes.find((n) => n.id === currentAnchorId);
      if (currentAnchorNode) {
        setSelectedAnchor(currentAnchorNode);
      }
    }
  }, [isOpen, flow, allNodes]);

  // Reset state when modal closes
  useEffect(() => {
    if (!isOpen) {
      setAnchorQuery("");
      setAnchorResults([]);
      setSelectedAnchor(null);
      setError(null);
      setIsDropdownOpen(false);
    }
  }, [isOpen]);

  // Debounced search for anchor nodes
  useEffect(() => {
    if (!anchorQuery || anchorQuery.length < 2) {
      setAnchorResults([]);
      return;
    }

    const handler = setTimeout(async () => {
      setIsSearching(true);
      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(anchorQuery)}&limit=20`);
        const data = await res.json();
        setAnchorResults(data.nodes || []);
        setIsDropdownOpen(true);
        setSelectedIndex(0);
      } catch (err) {
        console.error("Search failed:", err);
      } finally {
        setIsSearching(false);
      }
    }, 300);

    return () => clearTimeout(handler);
  }, [anchorQuery]);

  // Close dropdown on outside click
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setIsDropdownOpen(false);
      inputRef.current?.blur();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.min(prev + 1, anchorResults.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.max(prev - 1, 0));
    } else if (e.key === "Enter" && anchorResults[selectedIndex]) {
      e.preventDefault();
      handleSelectAnchor(anchorResults[selectedIndex]);
    }
  };

  const handleSelectAnchor = (node: GraphNode) => {
    setSelectedAnchor(node);
    setAnchorQuery("");
    setIsDropdownOpen(false);
    setAnchorResults([]);
  };

  const handleClearAnchor = () => {
    setSelectedAnchor(null);
    setAnchorQuery("");
    inputRef.current?.focus();
  };

  if (!isOpen || !flow) return null;

  // Check if anchor has changed
  const currentAnchorId = flow.anchorNodes[0];
  const hasChanged = selectedAnchor && selectedAnchor.id !== currentAnchorId;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();

    if (!selectedAnchor) {
      setError("Please select an anchor node");
      return;
    }

    if (!hasChanged) {
      onClose();
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/flows/${encodeURIComponent(flow.id)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          anchorNodeId: selectedAnchor.id,
        }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to update flow");
      }

      const data = await res.json();
      onUpdated(data);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-[#1a1a2e] border border-white/10 rounded-xl w-full max-w-lg shadow-xl">
        <div className="px-6 py-4 border-b border-white/10 flex items-center justify-between">
          <h2 className="text-lg font-semibold">Edit Flow</h2>
          <button
            onClick={onClose}
            className="p-1 hover:bg-white/10 rounded transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Flow name (read-only) */}
          <div>
            <label className="block text-sm font-medium text-white/60 mb-1">
              Flow Name
            </label>
            <div className="px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white/80">
              {flow.name}
            </div>
          </div>

          {/* Description (read-only if exists) */}
          {flow.description && (
            <div>
              <label className="block text-sm font-medium text-white/60 mb-1">
                Description
              </label>
              <div className="px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white/60">
                {flow.description}
              </div>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-white/60 mb-1">
              Anchor Node
            </label>
            
            {/* Selected anchor badge */}
            {selectedAnchor ? (
              <div className="flex items-center gap-2 p-2 bg-white/5 border border-white/10 rounded-lg">
                <span
                  className={`text-xs px-1.5 py-0.5 rounded ${
                    selectedAnchor.type === "source"
                      ? "bg-blue-500/20 text-blue-300"
                      : selectedAnchor.type === "model"
                      ? "bg-emerald-500/20 text-emerald-300"
                      : "bg-purple-500/20 text-purple-300"
                  }`}
                >
                  {selectedAnchor.type}
                </span>
                <span className="text-sm font-medium text-white flex-1 truncate">
                  {selectedAnchor.name}
                </span>
                <button
                  type="button"
                  onClick={handleClearAnchor}
                  className="p-1 hover:bg-white/10 rounded transition-colors"
                >
                  <svg className="w-4 h-4 text-white/60" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            ) : (
              /* Search input */
              <div className="relative" ref={dropdownRef}>
                <div className="relative">
                  <svg
                    className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/40"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                    />
                  </svg>
                  <input
                    ref={inputRef}
                    type="text"
                    value={anchorQuery}
                    onChange={(e) => setAnchorQuery(e.target.value)}
                    onFocus={() => anchorQuery.length >= 2 && setIsDropdownOpen(true)}
                    onKeyDown={handleKeyDown}
                    placeholder="Search for new anchor table..."
                    className="w-full pl-10 pr-4 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30"
                  />
                  {isSearching && (
                    <div className="absolute right-3 top-1/2 -translate-y-1/2">
                      <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                    </div>
                  )}
                </div>

                {/* Dropdown results */}
                {isDropdownOpen && anchorResults.length > 0 && (
                  <div className="absolute top-full left-0 right-0 mt-2 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-xl z-50 overflow-hidden">
                    <div className="p-2 max-h-60 overflow-y-auto">
                      {anchorResults.map((node, index) => (
                        <button
                          key={node.id}
                          type="button"
                          onClick={() => handleSelectAnchor(node)}
                          className={`w-full text-left px-3 py-2 rounded-lg transition-colors ${
                            index === selectedIndex
                              ? "bg-white/10"
                              : "hover:bg-white/5"
                          }`}
                        >
                          <div className="flex items-center gap-2">
                            <span
                              className={`text-xs px-1.5 py-0.5 rounded ${
                                node.type === "source"
                                  ? "bg-blue-500/20 text-blue-300"
                                  : node.type === "model"
                                  ? "bg-emerald-500/20 text-emerald-300"
                                  : "bg-purple-500/20 text-purple-300"
                              }`}
                            >
                              {node.type}
                            </span>
                            <span className="font-medium text-white truncate">{node.name}</span>
                          </div>
                          <div className="text-xs text-white/50 truncate mt-0.5">{node.id}</div>
                        </button>
                      ))}
                    </div>
                    <div className="px-3 py-2 border-t border-white/10 text-xs text-white/40">
                      ↑↓ to navigate · Enter to select
                    </div>
                  </div>
                )}

                {isDropdownOpen && anchorQuery.length >= 2 && anchorResults.length === 0 && !isSearching && (
                  <div className="absolute top-full left-0 right-0 mt-2 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-xl z-50 p-4 text-center text-white/50 text-sm">
                    No results found for "{anchorQuery}"
                  </div>
                )}
              </div>
            )}
            <p className="text-xs text-white/40 mt-1">
              Changing the anchor will recalculate the flow's member nodes
            </p>
          </div>

          {/* Member count info */}
          <div className="p-3 bg-white/5 border border-white/10 rounded-lg">
            <div className="flex items-center justify-between text-sm">
              <span className="text-white/60">Current members</span>
              <span className="font-medium">{flow.memberNodes.length} nodes</span>
            </div>
            {hasChanged && (
              <div className="text-xs text-amber-400 mt-2">
                Member nodes will be recalculated when you save
              </div>
            )}
          </div>

          {error && (
            <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-sm text-red-300">
              {error}
            </div>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-white/60 hover:text-white transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading || !selectedAnchor}
              className="px-4 py-2 bg-gradient-to-r from-emerald-500 to-cyan-500 text-black text-sm font-medium rounded-lg hover:from-emerald-400 hover:to-cyan-400 transition-all disabled:opacity-50"
            >
              {isLoading ? "Saving..." : hasChanged ? "Save Changes" : "Close"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
