"use client";

import { useState } from "react";

export default function Home() {
  const [jobId, setJobId] = useState<string | null>(null);

  async function startIngest() {
    const res = await fetch("/api/ingest", { method: "POST" });
    const json = await res.json();
    setJobId(json.jobId);
  }

  return (
    <main className="p-6 space-y-4">
      <h1 className="text-2xl font-semibold">
        Data Pipeline Explorer
      </h1>

      <button
        onClick={startIngest}
        className="px-4 py-2 rounded bg-black text-white"
      >
        Build Graph
      </button>

      {jobId && (
        <p className="text-sm text-gray-600">
          Job started: {jobId}
        </p>
      )}
    </main>
  );
}
