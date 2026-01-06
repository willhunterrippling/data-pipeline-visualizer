"use client";

import { useState, useEffect, useRef } from "react";
import type { GraphNode } from "@/lib/types";

interface SearchBarProps {
  onSelect: (node: GraphNode) => void;
}

export default function SearchBar({ onSelect }: SearchBarProps) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<GraphNode[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

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
          placeholder="Search tables, models..."
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
            {results.map((node) => (
              <button
                key={node.id}
                onClick={() => {
                  onSelect(node);
                  setIsOpen(false);
                  setQuery("");
                }}
                className="w-full text-left px-3 py-2 rounded-lg hover:bg-white/10 transition-colors"
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
        </div>
      )}
    </div>
  );
}

