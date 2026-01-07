"use client";

import { useState } from "react";

interface DepthControlProps {
  upstreamDepth: number;
  downstreamDepth: number;
  onUpstreamChange: (depth: number) => void;
  onDownstreamChange: (depth: number) => void;
  maxDepth?: number;
  linked?: boolean;
  onLinkedChange?: (linked: boolean) => void;
}

interface StepperProps {
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
  color: "emerald" | "cyan";
  label: string;
  icon: React.ReactNode;
  iconPosition: "left" | "right";
}

function DepthStepper({
  value,
  min,
  max,
  onChange,
  color,
  label,
  icon,
  iconPosition,
}: StepperProps) {
  const colorClasses = {
    emerald: {
      button: "hover:bg-emerald-500/30 active:bg-emerald-500/50 text-emerald-400",
      badge: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
    },
    cyan: {
      button: "hover:bg-cyan-500/30 active:bg-cyan-500/50 text-cyan-400",
      badge: "bg-cyan-500/20 text-cyan-300 border-cyan-500/30",
    },
  };

  const decrement = () => onChange(Math.max(min, value - 1));
  const increment = () => onChange(Math.min(max, value + 1));

  return (
    <div className="flex items-center gap-1.5">
      {iconPosition === "left" && (
        <span className="text-xs text-white/50 flex items-center gap-1 mr-1">
          {icon}
          <span className="hidden sm:inline">{label}</span>
        </span>
      )}
      
      <button
        onClick={decrement}
        disabled={value <= min}
        className={`w-6 h-6 rounded flex items-center justify-center transition-all
          ${value <= min ? "text-white/20 cursor-not-allowed" : colorClasses[color].button}
        `}
      >
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M20 12H4" />
        </svg>
      </button>

      <span
        className={`min-w-[2rem] h-6 px-2 flex items-center justify-center text-sm font-mono font-medium 
          rounded border ${colorClasses[color].badge}`}
      >
        {value}
      </span>

      <button
        onClick={increment}
        disabled={value >= max}
        className={`w-6 h-6 rounded flex items-center justify-center transition-all
          ${value >= max ? "text-white/20 cursor-not-allowed" : colorClasses[color].button}
        `}
      >
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
        </svg>
      </button>

      {iconPosition === "right" && (
        <span className="text-xs text-white/50 flex items-center gap-1 ml-1">
          <span className="hidden sm:inline">{label}</span>
          {icon}
        </span>
      )}
    </div>
  );
}

export default function DepthControl({
  upstreamDepth,
  downstreamDepth,
  onUpstreamChange,
  onDownstreamChange,
  maxDepth = 10,
  linked = true,
  onLinkedChange,
}: DepthControlProps) {
  const [isLinked, setIsLinked] = useState(linked);

  const handleUpstreamChange = (value: number) => {
    onUpstreamChange(value);
    if (isLinked) {
      onDownstreamChange(value);
    }
  };

  const handleDownstreamChange = (value: number) => {
    onDownstreamChange(value);
    if (isLinked) {
      onUpstreamChange(value);
    }
  };

  const toggleLinked = () => {
    const newLinked = !isLinked;
    setIsLinked(newLinked);
    onLinkedChange?.(newLinked);
  };

  const upstreamIcon = (
    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
    </svg>
  );

  const downstreamIcon = (
    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
    </svg>
  );

  return (
    <div className="flex items-center gap-2 px-2 py-1.5 bg-white/5 rounded-lg backdrop-blur-sm">
      {/* Upstream control */}
      <DepthStepper
        value={upstreamDepth}
        min={0}
        max={maxDepth}
        onChange={handleUpstreamChange}
        color="emerald"
        label="Up"
        icon={upstreamIcon}
        iconPosition="left"
      />

      {/* Link toggle */}
      <button
        onClick={toggleLinked}
        className={`p-1.5 rounded-md transition-all ${
          isLinked
            ? "text-amber-400 bg-amber-500/20 shadow-inner shadow-amber-500/10"
            : "text-white/30 hover:text-white/50 hover:bg-white/5"
        }`}
        title={isLinked ? "Depths are linked (click to unlink)" : "Depths are independent (click to link)"}
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          {isLinked ? (
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
            />
          ) : (
            <>
              <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 10.5L21 3" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M16 3h5v5" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M10.5 13.5L3 21" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M8 21H3v-5" />
            </>
          )}
        </svg>
      </button>

      {/* Downstream control */}
      <DepthStepper
        value={downstreamDepth}
        min={0}
        max={maxDepth}
        onChange={handleDownstreamChange}
        color="cyan"
        label="Down"
        icon={downstreamIcon}
        iconPosition="right"
      />
    </div>
  );
}
