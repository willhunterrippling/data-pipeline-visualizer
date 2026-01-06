"use client";

interface DepthControlProps {
  depth: number;
  onChange: (depth: number) => void;
  maxDepth?: number;
}

export default function DepthControl({
  depth,
  onChange,
  maxDepth = 5,
}: DepthControlProps) {
  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-white/5 rounded-lg">
      <span className="text-xs text-white/60">Depth</span>
      <div className="flex items-center gap-1">
        {Array.from({ length: maxDepth }, (_, i) => i + 1).map((d) => (
          <button
            key={d}
            onClick={() => onChange(d)}
            className={`w-6 h-6 rounded text-xs font-medium transition-colors ${
              depth === d
                ? "bg-cyan-500 text-black"
                : "bg-white/10 text-white/60 hover:bg-white/20 hover:text-white"
            }`}
          >
            {d}
          </button>
        ))}
      </div>
    </div>
  );
}

