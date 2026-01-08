"use client";

import { useState, useEffect, useCallback } from "react";
import { INDEXING_STAGES } from "@/lib/types";

interface ActivityLogEntry {
  timestamp: string;
  message: string;
}

interface UsageStats {
  totalInputTokens: number;
  totalOutputTokens: number;
  totalCalls: number;
  estimatedCostUsd: number;
}

interface JobStatus {
  id: string;
  status: "pending" | "running" | "completed" | "failed";
  stage?: string;
  stageName?: string;
  stageProgress: number;
  overallProgress: number;
  message?: string;
  error?: string;
  activityLog?: ActivityLogEntry[];
  usageStats?: UsageStats | null;
}

interface GraphStats {
  nodes: number;
  edges: number;
  groups: number;
  flows: number;
}

// Check if we're in static/production mode (Vercel)
// In static mode, indexing operations are not available
const IS_STATIC_MODE = process.env.NEXT_PUBLIC_STATIC_MODE === "true" || 
  (typeof window !== "undefined" && window.location.hostname !== "localhost");

export default function Home() {
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [graphStats, setGraphStats] = useState<GraphStats | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const [existingData, setExistingData] = useState<{ nodes: number; edges: number } | null>(null);
  const [isCheckingData, setIsCheckingData] = useState(true);
  const [isStaticMode, setIsStaticMode] = useState(IS_STATIC_MODE);

  // Detect static mode on mount (check if ingest endpoint is available)
  useEffect(() => {
    // If hostname is not localhost, we're likely in production
    if (typeof window !== "undefined" && window.location.hostname !== "localhost") {
      setIsStaticMode(true);
    }
  }, []);

  // Check for existing indexed data on mount
  useEffect(() => {
    async function checkExistingData() {
      try {
        const res = await fetch("/api/graph");
        if (res.ok) {
          const graph = await res.json();
          if (graph.nodes.length > 0) {
            setExistingData({
              nodes: graph.nodes.length,
              edges: graph.edges.length,
            });
          }
        }
      } catch (error) {
        console.error("Failed to check existing data:", error);
      } finally {
        setIsCheckingData(false);
      }
    }
    checkExistingData();
  }, []);

  const pollStatus = useCallback(async (id: string) => {
    try {
      const res = await fetch(`/api/status/${id}`);
      if (!res.ok) return;
      
      const status: JobStatus = await res.json();
      setJobStatus(status);

      if (status.status === "completed") {
        // Fetch graph stats
        const graphRes = await fetch("/api/graph");
        if (graphRes.ok) {
          const graph = await graphRes.json();
          setGraphStats({
            nodes: graph.nodes.length,
            edges: graph.edges.length,
            groups: graph.groups.length,
            flows: graph.flows.length,
          });
        }
      } else if (status.status === "running" || status.status === "pending") {
        // Continue polling
        setTimeout(() => pollStatus(id), 1000);
      }
    } catch (error) {
      console.error("Failed to poll status:", error);
    }
  }, []);

  async function startIndexing() {
    setIsLoading(true);
    setJobStatus(null);
    setGraphStats(null);

    try {
      const res = await fetch("/api/ingest", { method: "POST" });
      const json = await res.json();
      setJobId(json.jobId);
      pollStatus(json.jobId);
    } catch (error) {
      console.error("Failed to start indexing:", error);
    } finally {
      setIsLoading(false);
    }
  }

  const currentStageIndex = jobStatus?.stage
    ? INDEXING_STAGES.findIndex((s) => s.id === jobStatus.stage)
    : -1;

  // Format relative time for activity log
  function formatRelativeTime(timestamp: string): string {
    const now = Date.now();
    const then = new Date(timestamp).getTime();
    const diffMs = now - then;
    const diffSec = Math.floor(diffMs / 1000);
    
    if (diffSec < 5) return "now";
    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
    return `${Math.floor(diffSec / 3600)}h ago`;
  }

  async function handleReset() {
    setIsResetting(true);
    try {
      const res = await fetch("/api/reset", { method: "POST" });
      if (res.ok) {
        // Clear all state to return to initial view
        setJobId(null);
        setJobStatus(null);
        setGraphStats(null);
        setExistingData(null);
        setShowResetConfirm(false);
      } else {
        console.error("Failed to reset database");
      }
    } catch (error) {
      console.error("Failed to reset:", error);
    } finally {
      setIsResetting(false);
    }
  }

  return (
    <main className="min-h-screen bg-[#0a0a0f] text-white">
      {/* Header */}
      <header className="border-b border-white/10 bg-[#0a0a0f]/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-cyan-500 flex items-center justify-center">
              <svg className="w-5 h-5 text-black" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            </div>
            <h1 className="text-xl font-semibold tracking-tight">Pipeline Explorer</h1>
          </div>
          
          {graphStats && (
            <div className="flex items-center gap-6 text-sm text-white/60">
              <span><strong className="text-white">{graphStats.nodes}</strong> nodes</span>
              <span><strong className="text-white">{graphStats.edges}</strong> edges</span>
              <span><strong className="text-white">{graphStats.flows}</strong> flows</span>
            </div>
          )}
        </div>
      </header>

      <div className="max-w-4xl mx-auto px-6 py-16">
        {/* Hero Section */}
        {!jobId && !isCheckingData && (
          <div className="text-center space-y-8">
            <div className="space-y-4">
              <h2 className="text-4xl font-bold tracking-tight bg-gradient-to-r from-white via-white to-white/60 bg-clip-text text-transparent">
                Explore Your Data Pipelines
              </h2>
              <p className="text-lg text-white/60 max-w-2xl mx-auto">
                Index your dbt models, Airflow DAGs, and Snowflake metadata to build an interactive lineage graph with AI-powered insights.
              </p>
            </div>

            {/* Existing Data Banner */}
            {existingData && (
              <div className={`p-4 rounded-xl max-w-lg mx-auto ${
                isStaticMode 
                  ? "bg-emerald-500/10 border border-emerald-500/30" 
                  : "bg-amber-500/10 border border-amber-500/30"
              }`}>
                <div className="flex items-center gap-3">
                  <div className={`w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 ${
                    isStaticMode ? "bg-emerald-500/20" : "bg-amber-500/20"
                  }`}>
                    <svg className={`w-5 h-5 ${isStaticMode ? "text-emerald-400" : "text-amber-400"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      {isStaticMode ? (
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                      ) : (
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                      )}
                    </svg>
                  </div>
                  <div className="text-left">
                    <p className={`text-sm font-medium ${isStaticMode ? "text-emerald-200" : "text-amber-200"}`}>
                      {isStaticMode ? "Graph data loaded" : "Existing data found"}
                    </p>
                    <p className="text-xs text-white/50">
                      {existingData.nodes} nodes, {existingData.edges} edges
                    </p>
                  </div>
                </div>
              </div>
            )}

            {/* Action Buttons */}
            {existingData ? (
              <div className="flex justify-center gap-4 flex-wrap">
                <a
                  href="/explorer"
                  className="px-8 py-4 rounded-xl bg-gradient-to-r from-emerald-500 to-cyan-500 text-black font-semibold text-lg hover:from-emerald-400 hover:to-cyan-400 transition-all shadow-lg shadow-emerald-500/20"
                >
                  Open Explorer
                </a>
                {!isStaticMode && (
                  <>
                    <button
                      onClick={startIndexing}
                      disabled={isLoading}
                      className="px-8 py-4 rounded-xl bg-white/10 hover:bg-white/20 transition-colors font-semibold text-lg disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                    >
                      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                      </svg>
                      {isLoading ? "Starting..." : "Refresh"}
                    </button>
                    <button
                      onClick={() => setShowResetConfirm(true)}
                      className="px-8 py-4 rounded-xl bg-red-500/10 hover:bg-red-500/20 text-red-400 transition-colors font-semibold text-lg border border-red-500/30"
                    >
                      Reset
                    </button>
                  </>
                )}
              </div>
            ) : isStaticMode ? (
              <div className="text-center space-y-4">
                <p className="text-white/60">No graph data available.</p>
                <p className="text-sm text-white/40">
                  Run <code className="px-2 py-1 bg-white/10 rounded">npm run dev</code> locally to index your pipelines.
                </p>
              </div>
            ) : (
              <button
                onClick={startIndexing}
                disabled={isLoading}
                className="px-8 py-4 rounded-xl bg-gradient-to-r from-emerald-500 to-cyan-500 text-black font-semibold text-lg hover:from-emerald-400 hover:to-cyan-400 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-emerald-500/20"
              >
                {isLoading ? "Starting..." : "Build Graph"}
              </button>
            )}

            <div className="grid grid-cols-3 gap-6 mt-16 text-left">
              <div className="p-6 rounded-xl bg-white/5 border border-white/10">
                <div className="w-10 h-10 rounded-lg bg-emerald-500/20 flex items-center justify-center mb-4">
                  <svg className="w-5 h-5 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
                  </svg>
                </div>
                <h3 className="font-semibold mb-2">Multi-Repo Indexing</h3>
                <p className="text-sm text-white/60">Parse dbt manifests, Airflow DAGs, and SQL files across repositories.</p>
              </div>

              <div className="p-6 rounded-xl bg-white/5 border border-white/10">
                <div className="w-10 h-10 rounded-lg bg-cyan-500/20 flex items-center justify-center mb-4">
                  <svg className="w-5 h-5 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                  </svg>
                </div>
                <h3 className="font-semibold mb-2">Smart Grouping</h3>
                <p className="text-sm text-white/60">AI-inferred groups based on domains, layers, and naming conventions.</p>
              </div>

              <div className="p-6 rounded-xl bg-white/5 border border-white/10">
                <div className="w-10 h-10 rounded-lg bg-purple-500/20 flex items-center justify-center mb-4">
                  <svg className="w-5 h-5 text-purple-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                </div>
                <h3 className="font-semibold mb-2">Flow Discovery</h3>
                <p className="text-sm text-white/60">Automatically detect data flows like Mechanized Outreach and Bookings.</p>
              </div>
            </div>
          </div>
        )}

        {/* Loading state while checking for existing data */}
        {!jobId && isCheckingData && (
          <div className="text-center py-16">
            <div className="w-8 h-8 border-2 border-white/20 border-t-white/60 rounded-full animate-spin mx-auto" />
            <p className="text-white/40 mt-4">Checking for existing data...</p>
          </div>
        )}

        {/* Waiting for status (gap state - jobId set but jobStatus not yet received) */}
        {jobId && !jobStatus && (
          <div className="text-center space-y-8">
            <div className="space-y-2">
              <h2 className="text-2xl font-semibold">Starting Build...</h2>
              <p className="text-white/60">Initializing indexer</p>
            </div>
            <div className="flex justify-center">
              <div className="w-8 h-8 border-2 border-white/20 border-t-emerald-500 rounded-full animate-spin" />
            </div>
          </div>
        )}

        {/* Progress Section */}
        {jobStatus && jobStatus.status !== "completed" && (
          <div className="space-y-8">
            <div className="text-center space-y-2">
              <h2 className="text-2xl font-semibold">
                {jobStatus.status === "failed" ? "Indexing Failed" : "Building Graph..."}
              </h2>
              {jobStatus.message && (
                <p className="text-white/60">{jobStatus.message}</p>
              )}
            </div>

            {jobStatus.status === "failed" ? (
              <div className="p-6 rounded-xl bg-red-500/10 border border-red-500/30">
                <p className="text-red-400 font-mono text-sm">{jobStatus.error}</p>
                <button
                  onClick={startIndexing}
                  className="mt-4 px-4 py-2 rounded-lg bg-white/10 hover:bg-white/20 transition-colors"
                >
                  Retry
                </button>
              </div>
            ) : (
              <div className="space-y-6">
                {/* Overall Progress Bar */}
                <div className="space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="text-white/60">Overall Progress</span>
                    <span className="font-mono">{jobStatus.overallProgress}%</span>
                  </div>
                  <div className="h-2 bg-white/10 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-gradient-to-r from-emerald-500 to-cyan-500 transition-all duration-500 ease-out"
                      style={{ width: `${jobStatus.overallProgress}%` }}
                    />
                  </div>
                </div>

                {/* Stage List */}
                <div className="space-y-2">
                  {INDEXING_STAGES.map((stage, index) => {
                    const isActive = stage.id === jobStatus.stage;
                    const isComplete = currentStageIndex > index;
                    const isPending = currentStageIndex < index;

                    return (
                      <div
                        key={stage.id}
                        className={`flex items-center gap-4 p-3 rounded-lg transition-colors ${
                          isActive
                            ? "bg-white/10"
                            : isComplete
                            ? "bg-emerald-500/10"
                            : "bg-white/5"
                        }`}
                      >
                        <div
                          className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-mono ${
                            isComplete
                              ? "bg-emerald-500 text-black"
                              : isActive
                              ? "bg-cyan-500 text-black"
                              : "bg-white/20 text-white/40"
                          }`}
                        >
                          {isComplete ? (
                            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                            </svg>
                          ) : (
                            index + 1
                          )}
                        </div>
                        <div className="flex-1">
                          <span className={isPending ? "text-white/40" : "text-white"}>
                            {stage.name}
                          </span>
                        </div>
                        {isActive && (
                          <div className="flex items-center gap-2">
                            <div className="w-16 h-1.5 bg-white/20 rounded-full overflow-hidden">
                              <div
                                className="h-full bg-cyan-500 transition-all duration-300"
                                style={{ width: `${jobStatus.stageProgress}%` }}
                              />
                            </div>
                            <span className="text-xs font-mono text-white/60">
                              {jobStatus.stageProgress}%
                            </span>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

                {/* Activity Log */}
                {jobStatus.activityLog && jobStatus.activityLog.length > 0 && (
                  <div className="mt-6 p-4 rounded-xl bg-[#12121a] border border-white/5">
                    <div className="flex items-center gap-2 mb-3">
                      <div className="w-2 h-2 rounded-full bg-cyan-500 animate-pulse" />
                      <span className="text-xs font-medium text-white/50 uppercase tracking-wide">Activity</span>
                    </div>
                    <div className="space-y-1.5 max-h-48 overflow-y-auto font-mono text-xs">
                      {jobStatus.activityLog.slice(-10).reverse().map((entry, i) => (
                        <div
                          key={`${entry.timestamp}-${i}`}
                          className={`flex gap-3 ${i === 0 ? "text-white" : "text-white/50"}`}
                        >
                          <span className="text-white/30 w-14 flex-shrink-0 text-right">
                            {formatRelativeTime(entry.timestamp)}
                          </span>
                          <span className="truncate">{entry.message}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Success Section */}
        {jobStatus?.status === "completed" && graphStats && (
          <div className="space-y-8">
            <div className="text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-emerald-500/20 flex items-center justify-center mx-auto">
                <svg className="w-8 h-8 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h2 className="text-2xl font-semibold">Graph Built Successfully</h2>
              <p className="text-white/60">
                Indexed {graphStats.nodes} nodes across {graphStats.flows} flows
              </p>
            </div>

            <div className="grid grid-cols-4 gap-4">
              <div className="p-6 rounded-xl bg-white/5 border border-white/10 text-center">
                <div className="text-3xl font-bold text-emerald-400">{graphStats.nodes}</div>
                <div className="text-sm text-white/60 mt-1">Nodes</div>
              </div>
              <div className="p-6 rounded-xl bg-white/5 border border-white/10 text-center">
                <div className="text-3xl font-bold text-cyan-400">{graphStats.edges}</div>
                <div className="text-sm text-white/60 mt-1">Edges</div>
              </div>
              <div className="p-6 rounded-xl bg-white/5 border border-white/10 text-center">
                <div className="text-3xl font-bold text-purple-400">{graphStats.groups}</div>
                <div className="text-sm text-white/60 mt-1">Groups</div>
              </div>
              <div className="p-6 rounded-xl bg-white/5 border border-white/10 text-center">
                <div className="text-3xl font-bold text-orange-400">{graphStats.flows}</div>
                <div className="text-sm text-white/60 mt-1">Flows</div>
              </div>
            </div>

            {/* AI Usage Cost Summary */}
            {jobStatus.usageStats && jobStatus.usageStats.totalCalls > 0 && (
              <div className="flex items-center justify-center gap-6 py-3 px-5 rounded-lg bg-white/5 border border-white/10 text-sm">
                <div className="flex items-center gap-2">
                  <svg className="w-4 h-4 text-white/40" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                  </svg>
                  <span className="text-white/50">AI Usage:</span>
                </div>
                <div className="flex items-center gap-4 text-white/70">
                  <span>
                    <strong className="text-white">{jobStatus.usageStats.totalCalls}</strong> API calls
                  </span>
                  <span className="text-white/30">•</span>
                  <span>
                    <strong className="text-white">{(jobStatus.usageStats.totalInputTokens / 1000).toFixed(1)}K</strong> input tokens
                  </span>
                  <span className="text-white/30">•</span>
                  <span>
                    <strong className="text-white">{(jobStatus.usageStats.totalOutputTokens / 1000).toFixed(1)}K</strong> output tokens
                  </span>
                  <span className="text-white/30">•</span>
                  <span className="text-emerald-400 font-medium">
                    ~${jobStatus.usageStats.estimatedCostUsd < 0.01 
                      ? jobStatus.usageStats.estimatedCostUsd.toFixed(4) 
                      : jobStatus.usageStats.estimatedCostUsd.toFixed(2)} estimated
                  </span>
                </div>
              </div>
            )}

            <div className="flex justify-center gap-4">
              <a
                href="/explorer"
                className="px-8 py-4 rounded-xl bg-gradient-to-r from-emerald-500 to-cyan-500 text-black font-semibold hover:from-emerald-400 hover:to-cyan-400 transition-all"
              >
                Open Graph Explorer
              </a>
              {!isStaticMode && (
                <>
                  <button
                    onClick={startIndexing}
                    className="px-8 py-4 rounded-xl bg-white/10 hover:bg-white/20 transition-colors font-semibold"
                  >
                    Re-index
                  </button>
                  <button
                    onClick={() => setShowResetConfirm(true)}
                    className="px-8 py-4 rounded-xl bg-red-500/10 hover:bg-red-500/20 text-red-400 transition-colors font-semibold border border-red-500/30"
                  >
                    Reset
                  </button>
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Reset Confirmation Modal */}
      {showResetConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-[#1a1a24] rounded-xl border border-white/10 p-6 max-w-md w-full mx-4 shadow-2xl">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-full bg-red-500/20 flex items-center justify-center">
                <svg className="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold">Reset All Data?</h3>
            </div>
            <p className="text-white/60 mb-6">
              This will permanently delete all indexed data, including nodes, edges, flows, groups, and explanations. You will need to re-analyze your pipeline from scratch.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowResetConfirm(false)}
                className="px-4 py-2 rounded-lg bg-white/10 hover:bg-white/20 transition-colors font-medium"
                disabled={isResetting}
              >
                Cancel
              </button>
              <button
                onClick={handleReset}
                disabled={isResetting}
                className="px-4 py-2 rounded-lg bg-red-500 hover:bg-red-400 text-white transition-colors font-medium disabled:opacity-50"
              >
                {isResetting ? "Resetting..." : "Reset All Data"}
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
