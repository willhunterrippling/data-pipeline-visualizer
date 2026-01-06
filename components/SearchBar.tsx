"use client";

import { useState, useEffect, useRef, forwardRef, useImperativeHandle } from "react";
import type { GraphNode } from "@/lib/types";

interface SearchBarProps {
  onSelect: (node: GraphNode) => void;
}

const SearchBar = forwardRef<HTMLInputElement, SearchBarProps>(function SearchBar(
  { onSelect },
  ref
) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<GraphNode[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Expose the input ref
  useImperativeHandle(ref, () => inputRef.current!);

  useEffect(() => {
    const handler = setTimeout(async () => {
      if (query.length < 2) {
        setResults([]);
        return;
      }

      setIsLoading(true);
      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(query)}&limit=20`);
        const data = await res.json();
        setResults(data.nodes || []);
        setIsOpen(true);
        setSelectedIndex(0);
      } catch (error) {
        console.error("Search failed:", error);
      } finally {
        setIsLoading(false);
      }
    }, 300);

    return () => clearTimeout(handler);
  }, [query]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setIsOpen(false);
      inputRef.current?.blur();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.min(prev + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.max(prev - 1, 0));
    } else if (e.key === "Enter" && results[selectedIndex]) {
      e.preventDefault();
      onSelect(results[selectedIndex]);
      setIsOpen(false);
      setQuery("");
    }
  };

  return (
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
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => query.length >= 2 && setIsOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder="Search tables... (/)"
          className="w-64 pl-10 pr-4 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30 focus:bg-white/15 transition-colors"
        />
        {isLoading && (
          <div className="absolute right-3 top-1/2 -translate-y-1/2">
            <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
          </div>
        )}
      </div>

      {isOpen && results.length > 0 && (
        <div className="absolute top-full left-0 mt-2 w-80 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-xl z-50 overflow-hidden animate-slide-up">
          <div className="p-2 max-h-80 overflow-y-auto">
            {results.map((node, index) => (
              <button
                key={node.id}
                onClick={() => {
                  onSelect(node);
                  setIsOpen(false);
                  setQuery("");
                }}
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
            ↑↓ to navigate · Enter to select · Esc to close
          </div>
        </div>
      )}

      {isOpen && query.length >= 2 && results.length === 0 && !isLoading && (
        <div className="absolute top-full left-0 mt-2 w-80 bg-[#1a1a2e] border border-white/10 rounded-xl shadow-xl z-50 p-4 text-center text-white/50 text-sm">
          No results found for "{query}"
        </div>
      )}
    </div>
  );
});

export default SearchBar;
