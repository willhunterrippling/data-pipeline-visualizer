"use client";

import { useState, useEffect, useRef, useCallback } from "react";

interface AnchorCandidate {
  nodeId: string;
  nodeName: string;
  nodeType: string;
  nodeSubtype?: string;
  importanceScore: number;
  upstreamCount: number;
  downstreamCount: number;
  totalConnections: number;
  reason: string | null;
}

interface SearchResult {
  id: string;
  name: string;
  type: string;
  subtype?: string;
}

interface CreateFlowModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreated: (flow: { id: string; name: string; memberCount: number; anchorNodes?: string[]; memberNodes?: string[] }) => void;
}

// Node type colors for badges
const typeColors: Record<string, string> = {
  external: "bg-purple-500/20 text-purple-300 border-purple-500/30",
  mart: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  model: "bg-cyan-500/20 text-cyan-300 border-cyan-500/30",
  table: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  view: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  source: "bg-orange-500/20 text-orange-300 border-orange-500/30",
  seed: "bg-pink-500/20 text-pink-300 border-pink-500/30",
};

function getTypeColor(type: string, name: string): string {
  // Check for mart/report by name prefix
  if (name.startsWith("mart_") || name.startsWith("rpt_")) {
    return typeColors.mart;
  }
  return typeColors[type] || "bg-white/10 text-white/70 border-white/20";
}

function getTypeLabel(type: string, subtype?: string, name?: string): string {
  if (type === "external") return "External";
  if (name?.startsWith("mart_")) return "Mart";
  if (name?.startsWith("rpt_")) return "Report";
  if (subtype === "dbt_model") return "Model";
  if (subtype === "dbt_source") return "Source";
  return type.charAt(0).toUpperCase() + type.slice(1);
}

export default function CreateFlowModal({
  isOpen,
  onClose,
  onCreated,
}: CreateFlowModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedAnchors, setSelectedAnchors] = useState<SearchResult[]>([]);
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [suggestedAnchors, setSuggestedAnchors] = useState<AnchorCandidate[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const searchInputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Fetch suggested anchor candidates on mount
  useEffect(() => {
    if (isOpen) {
      fetchSuggestedAnchors();
    }
  }, [isOpen]);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const fetchSuggestedAnchors = async () => {
    try {
      const res = await fetch("/api/anchor-candidates?limit=10&includeExternal=true");
      if (res.ok) {
        const data = await res.json();
        setSuggestedAnchors(data.candidates || []);
      }
    } catch {
      // Silently fail - suggestions are not critical
    }
  };

  // Debounced search
  const searchNodesApi = useCallback(async (query: string) => {
    if (query.length < 2) {
      setSearchResults([]);
      return;
    }

    setIsSearching(true);
    try {
      const res = await fetch(`/api/search?q=${encodeURIComponent(query)}&limit=15`);
      if (res.ok) {
        const data = await res.json();
        // Filter out already selected nodes and map to SearchResult format
        const selectedIds = new Set(selectedAnchors.map((a) => a.id));
        const filtered = (data.nodes || [])
          .filter((n: { id: string }) => !selectedIds.has(n.id))
          .map((n: { id: string; name: string; type: string; subtype?: string }) => ({
            id: n.id,
            name: n.name,
            type: n.type,
            subtype: n.subtype,
          }));
        setSearchResults(filtered);
      }
    } catch {
      setSearchResults([]);
    } finally {
      setIsSearching(false);
    }
  }, [selectedAnchors]);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      if (searchQuery) {
        searchNodesApi(searchQuery);
      } else {
        setSearchResults([]);
      }
    }, 200);
    return () => clearTimeout(timer);
  }, [searchQuery, searchNodesApi]);

  const addAnchor = (node: SearchResult) => {
    if (!selectedAnchors.some((a) => a.id === node.id)) {
      setSelectedAnchors([...selectedAnchors, node]);
    }
    setSearchQuery("");
    setSearchResults([]);
    setShowDropdown(false);
  };

  const removeAnchor = (nodeId: string) => {
    setSelectedAnchors(selectedAnchors.filter((a) => a.id !== nodeId));
  };

  const addSuggestedAnchor = (candidate: AnchorCandidate) => {
    const node: SearchResult = {
      id: candidate.nodeId,
      name: candidate.nodeName,
      type: candidate.nodeType,
      subtype: candidate.nodeSubtype,
    };
    if (!selectedAnchors.some((a) => a.id === node.id)) {
      setSelectedAnchors([...selectedAnchors, node]);
    }
  };

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    
    if (selectedAnchors.length === 0) {
      setError("Please select at least one anchor node");
      return;
    }
    
    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch("/api/flows", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          description: description || undefined,
          anchorNodeIds: selectedAnchors.map((a) => a.id),
        }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to create flow");
      }

      const data = await res.json();
      onCreated(data);
      handleClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setIsLoading(false);
    }
  }

  const handleClose = () => {
    setName("");
    setDescription("");
    setSearchQuery("");
    setSelectedAnchors([]);
    setSearchResults([]);
    setError(null);
    onClose();
  };

  if (!isOpen) return null;

  // Filter suggested anchors to exclude already selected
  const filteredSuggestions = suggestedAnchors.filter(
    (s) => !selectedAnchors.some((a) => a.id === s.nodeId)
  );

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-[#1a1a2e] border border-white/10 rounded-xl w-full max-w-lg shadow-xl max-h-[90vh] overflow-hidden flex flex-col">
        <div className="px-6 py-4 border-b border-white/10 flex items-center justify-between flex-shrink-0">
          <h2 className="text-lg font-semibold">Create Flow</h2>
          <button
            onClick={handleClose}
            className="p-1 hover:bg-white/10 rounded transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4 overflow-y-auto flex-1">
          <div>
            <label className="block text-sm font-medium text-white/60 mb-1">
              Flow Name
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g., Customer Churn Analysis"
              className="w-full px-3 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-white/60 mb-1">
              Description (optional)
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Brief description of what this flow represents..."
              rows={2}
              className="w-full px-3 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30 resize-none"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-white/60 mb-1">
              Anchor Nodes
            </label>
            
            {/* Selected anchors as chips */}
            {selectedAnchors.length > 0 && (
              <div className="flex flex-wrap gap-2 mb-2">
                {selectedAnchors.map((anchor) => (
                  <span
                    key={anchor.id}
                    className="inline-flex items-center gap-1.5 px-2 py-1 rounded-lg bg-white/10 border border-white/20 text-sm"
                  >
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium border ${getTypeColor(anchor.type, anchor.name)}`}>
                      {getTypeLabel(anchor.type, anchor.subtype, anchor.name)}
                    </span>
                    <span className="text-white/90 truncate max-w-[150px]">{anchor.name}</span>
                    <button
                      type="button"
                      onClick={() => removeAnchor(anchor.id)}
                      className="text-white/40 hover:text-white/80 ml-1"
                    >
                      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </span>
                ))}
              </div>
            )}

            {/* Search input with dropdown */}
            <div className="relative" ref={dropdownRef}>
              <div className="relative">
                <svg
                  className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/40"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <input
                  ref={searchInputRef}
                  type="text"
                  value={searchQuery}
                  onChange={(e) => {
                    setSearchQuery(e.target.value);
                    setShowDropdown(true);
                  }}
                  onFocus={() => setShowDropdown(true)}
                  placeholder="Search for tables, external systems..."
                  className="w-full pl-9 pr-3 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30"
                />
                {isSearching && (
                  <div className="absolute right-3 top-1/2 -translate-y-1/2">
                    <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                  </div>
                )}
              </div>

              {/* Search results dropdown */}
              {showDropdown && (searchResults.length > 0 || searchQuery.length >= 2) && (
                <div className="absolute z-10 w-full mt-1 bg-[#1a1a2e] border border-white/10 rounded-lg shadow-xl max-h-48 overflow-y-auto">
                  {searchResults.length > 0 ? (
                    searchResults.map((result) => (
                      <button
                        key={result.id}
                        type="button"
                        onClick={() => addAnchor(result)}
                        className="w-full px-3 py-2 text-left hover:bg-white/10 flex items-center gap-2 text-sm"
                      >
                        <span className={`px-1.5 py-0.5 rounded text-xs font-medium border ${getTypeColor(result.type, result.name)}`}>
                          {getTypeLabel(result.type, result.subtype, result.name)}
                        </span>
                        <span className="text-white/90 truncate">{result.name}</span>
                      </button>
                    ))
                  ) : searchQuery.length >= 2 && !isSearching ? (
                    <div className="px-3 py-2 text-sm text-white/40">
                      No results found
                    </div>
                  ) : null}
                </div>
              )}
            </div>
            
            <p className="text-xs text-white/40 mt-1">
              Search for tables, models, or external systems to use as anchor points
            </p>
          </div>

          {/* Suggested anchors */}
          {filteredSuggestions.length > 0 && selectedAnchors.length < 3 && (
            <div>
              <label className="block text-sm font-medium text-white/40 mb-2">
                Suggested Anchors
              </label>
              <div className="flex flex-wrap gap-2">
                {filteredSuggestions.slice(0, 6).map((suggestion) => (
                  <button
                    key={suggestion.nodeId}
                    type="button"
                    onClick={() => addSuggestedAnchor(suggestion)}
                    className="inline-flex items-center gap-1.5 px-2 py-1 rounded-lg bg-white/5 border border-white/10 hover:bg-white/10 hover:border-white/20 transition-colors text-sm"
                  >
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium border ${getTypeColor(suggestion.nodeType, suggestion.nodeName)}`}>
                      {getTypeLabel(suggestion.nodeType, suggestion.nodeSubtype, suggestion.nodeName)}
                    </span>
                    <span className="text-white/70 truncate max-w-[120px]">{suggestion.nodeName}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {error && (
            <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-sm text-red-300">
              {error}
            </div>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={handleClose}
              className="px-4 py-2 text-sm font-medium text-white/60 hover:text-white transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading || selectedAnchors.length === 0}
              className="px-4 py-2 bg-gradient-to-r from-emerald-500 to-cyan-500 text-black text-sm font-medium rounded-lg hover:from-emerald-400 hover:to-cyan-400 transition-all disabled:opacity-50"
            >
              {isLoading ? "Creating..." : "Create Flow"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
