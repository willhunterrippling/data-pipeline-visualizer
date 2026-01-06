"use client";

import { useState } from "react";

interface CreateFlowModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreated: (flow: { id: string; name: string; memberCount: number }) => void;
}

export default function CreateFlowModal({
  isOpen,
  onClose,
  onCreated,
}: CreateFlowModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [anchorSearch, setAnchorSearch] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!isOpen) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch("/api/flows", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          description: description || undefined,
          naturalLanguage: anchorSearch,
        }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to create flow");
      }

      const data = await res.json();
      onCreated(data);
      onClose();
      setName("");
      setDescription("");
      setAnchorSearch("");
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
          <h2 className="text-lg font-semibold">Create Flow</h2>
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
              Anchor Tables
            </label>
            <input
              type="text"
              value={anchorSearch}
              onChange={(e) => setAnchorSearch(e.target.value)}
              placeholder="e.g., mechanized outreach, or mart_growth__lsw_lead_data"
              className="w-full px-3 py-2 bg-white/10 border border-white/10 rounded-lg text-sm placeholder-white/40 focus:outline-none focus:border-white/30"
              required
            />
            <p className="text-xs text-white/40 mt-1">
              Describe the flow in natural language or provide table names
            </p>
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
              disabled={isLoading}
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

