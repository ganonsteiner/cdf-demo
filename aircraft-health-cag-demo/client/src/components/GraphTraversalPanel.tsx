import { useEffect, useRef, useState } from "react";
import { Network, Box, Zap, FileText, GitBranch, Activity, RotateCcw } from "lucide-react";
import { cn } from "../lib/utils";
import type { AgentEvent } from "../lib/types";

interface Props {
  events: AgentEvent[];
  isStreaming: boolean;
  canReplay?: boolean;
  onReplay?: () => void;
  isReplaying?: boolean;
}

function nodeIcon(node: string) {
  if (
    node.startsWith("Asset:") ||
    node.startsWith("AssetSubtree:") ||
    node.startsWith("AssetChildren:") ||
    node.startsWith("AssetSubgraph:")
  )
    return { icon: <Box className="w-3 h-3" />, color: "text-sky-400", bg: "bg-sky-950/60 border-sky-800/50" };

  if (node.startsWith("Sensor:") || node.startsWith("Datapoint:") || node.startsWith("TimeSeries:"))
    return { icon: <Activity className="w-3 h-3" />, color: "text-emerald-400", bg: "bg-emerald-950/60 border-emerald-800/50" };

  if (
    node.startsWith("Events:") ||
    node.startsWith("ComponentEvents:") ||
    node.startsWith("Symptoms:") ||
    node.startsWith("FleetSearch:") ||
    node.startsWith("PolicyCompliance:")
  )
    return { icon: <Zap className="w-3 h-3" />, color: "text-orange-400", bg: "bg-orange-950/60 border-orange-800/50" };

  if (
    node.startsWith("Relationships:") ||
    node.startsWith("FleetPolicies:") ||
    node.startsWith("FleetOverview:") ||
    node.startsWith("EngineTypeHistory:") ||
    node.startsWith("IS_TYPE:") ||
    node.startsWith("HAS_COMPONENT:")
  )
    return { icon: <GitBranch className="w-3 h-3" />, color: "text-violet-400", bg: "bg-violet-950/60 border-violet-800/50" };

  if (node.startsWith("File:") || node.startsWith("Documents:"))
    return { icon: <FileText className="w-3 h-3" />, color: "text-purple-400", bg: "bg-purple-950/60 border-purple-800/50" };

  if (node.startsWith("Context:") || node.startsWith("FleetContext:"))
    return { icon: <Network className="w-3 h-3" />, color: "text-yellow-400", bg: "bg-yellow-950/60 border-yellow-800/50" };

  return { icon: <Box className="w-3 h-3" />, color: "text-zinc-400", bg: "bg-zinc-800/60 border-zinc-700/50" };
}

function toolColor(name: string): string {
  const map: Record<string, string> = {
    assemble_aircraft_context: "text-yellow-400",
    assemble_fleet_context: "text-yellow-300",
    get_fleet_overview: "text-sky-300",
    get_fleet_policies: "text-violet-300",
    get_aircraft_symptoms: "text-orange-300",
    get_engine_type_history: "text-cyan-400",
    search_fleet_for_similar_events: "text-pink-400",
    check_fleet_policy_compliance: "text-violet-400",
    get_asset: "text-sky-400",
    get_asset_children: "text-sky-300",
    get_asset_subgraph: "text-sky-500",
    get_time_series: "text-emerald-400",
    get_datapoints: "text-emerald-300",
    get_events: "text-orange-400",
    get_relationships: "text-violet-400",
    get_linked_documents: "text-purple-400",
  };
  return map[name] || "text-zinc-400";
}

const STAGGER_MS = 150;

export default function GraphTraversalPanel({
  events,
  isStreaming,
  canReplay = false,
  onReplay,
  isReplaying = false,
}: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);

  // 150ms stagger: buffer incoming events, drain one every 150ms into visible list
  const [visibleEvents, setVisibleEvents] = useState<AgentEvent[]>([]);
  const bufferRef = useRef<AgentEvent[]>([]);
  const drainTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lastEventsLenRef = useRef(0);

  useEffect(() => {
    // When not streaming or replaying, show all events immediately
    if (!isStreaming && !isReplaying) {
      setVisibleEvents(events);
      bufferRef.current = [];
      if (drainTimerRef.current) {
        clearInterval(drainTimerRef.current);
        drainTimerRef.current = null;
      }
      lastEventsLenRef.current = events.length;
      return;
    }

    // New events arrived — add to buffer
    const newEvents = events.slice(lastEventsLenRef.current);
    if (newEvents.length > 0) {
      bufferRef.current.push(...newEvents);
      lastEventsLenRef.current = events.length;
    }

    // Start drain timer if not already running
    if (!drainTimerRef.current) {
      drainTimerRef.current = setInterval(() => {
        if (bufferRef.current.length > 0) {
          const next = bufferRef.current.shift()!;
          setVisibleEvents((prev) => [...prev, next]);
        } else if (!isStreaming && !isReplaying) {
          clearInterval(drainTimerRef.current!);
          drainTimerRef.current = null;
        }
      }, STAGGER_MS);
    }
  }, [events, isStreaming, isReplaying]);

  // Clear visible events when events array is reset (new query)
  useEffect(() => {
    if (events.length === 0) {
      setVisibleEvents([]);
      bufferRef.current = [];
      lastEventsLenRef.current = 0;
      if (drainTimerRef.current) {
        clearInterval(drainTimerRef.current);
        drainTimerRef.current = null;
      }
    }
  }, [events.length]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (drainTimerRef.current) clearInterval(drainTimerRef.current);
    };
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [visibleEvents]);

  const traversalCount = visibleEvents.filter((e) => e.type === "traversal").length;
  const toolCallCount = visibleEvents.filter((e) => e.type === "tool_call").length;

  return (
    <div className="flex flex-col h-full bg-zinc-900 border-l border-zinc-800 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800 bg-zinc-900/80 shrink-0">
        <div className="flex items-center gap-2 min-w-0">
          <Network className="w-4 h-4 text-sky-400 shrink-0" />
          <span className="text-sm font-semibold text-zinc-300">Graph Traversal</span>
          {isStreaming && (
            <span className="flex items-center gap-1 text-xs text-sky-400 animate-pulse shrink-0">
              <span className="w-1.5 h-1.5 bg-sky-400 rounded-full" />
              scanning
            </span>
          )}
          {isReplaying && (
            <span className="flex items-center gap-1 text-xs text-violet-400 shrink-0">
              <span className="w-1.5 h-1.5 bg-violet-400 rounded-full animate-pulse" />
              replaying
            </span>
          )}
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <span className="text-xs text-zinc-600">
            {traversalCount} nodes · {toolCallCount} calls
          </span>
          {canReplay && onReplay && (
            <button
              onClick={onReplay}
              title="Replay traversal at 150ms"
              className="p-1 rounded text-zinc-500 hover:text-violet-400 hover:bg-violet-950/30 transition-colors"
            >
              <RotateCcw className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Legend */}
      <div className="px-3 py-2 border-b border-zinc-800/50 flex flex-wrap gap-x-3 gap-y-1 shrink-0">
        {[
          { label: "Asset", color: "text-sky-400" },
          { label: "Sensor/TS", color: "text-emerald-400" },
          { label: "Event", color: "text-orange-400" },
          { label: "Relation", color: "text-violet-400" },
          { label: "Document", color: "text-purple-400" },
          { label: "Context", color: "text-yellow-400" },
        ].map((l) => (
          <span key={l.label} className={cn("text-xs", l.color)}>
            ● {l.label}
          </span>
        ))}
      </div>

      {/* Events feed */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-3 space-y-1.5 font-mono" style={{ minHeight: 0 }}>
        {visibleEvents.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full text-zinc-700 py-8 text-center">
            <Network className="w-8 h-8 mb-3" />
            <p className="text-sm">Graph traversal events</p>
            <p className="text-xs mt-1">appear here at 150ms intervals</p>
          </div>
        )}

        {visibleEvents.map((event, idx) => {
          if (event.type === "tool_call") {
            return (
              <div key={idx} className="animate-fade-slide-in flex items-center gap-2 py-0.5">
                <span className="text-zinc-700 text-xs w-5 text-right shrink-0">{event.iteration}</span>
                <span className="text-zinc-600 text-xs">→</span>
                <span className={cn("text-xs font-semibold", toolColor(event.tool_name || ""))}>
                  {event.tool_name}
                </span>
                {event.args && Object.keys(event.args).length > 0 && (
                  <span className="text-zinc-600 text-xs truncate">
                    ({Object.values(event.args).slice(0, 2).join(", ")})
                  </span>
                )}
              </div>
            );
          }

          if (event.type === "tool_result") {
            return (
              <div key={idx} className="animate-fade-slide-in flex items-center gap-2 py-0.5 pl-8">
                <span className="text-zinc-700 text-xs">↳</span>
                <span className="text-zinc-500 text-xs truncate">{event.summary}</span>
              </div>
            );
          }

          if (event.type === "traversal") {
            const { icon, color, bg } = nodeIcon(event.node || "");
            const colonIdx = (event.node || "").indexOf(":");
            const nodeType = colonIdx >= 0 ? (event.node || "").slice(0, colonIdx) : (event.node || "");
            const nodeId = colonIdx >= 0 ? (event.node || "").slice(colonIdx + 1) : "";
            return (
              <div key={idx} className="animate-fade-slide-in flex items-center gap-1.5 py-0.5 pl-5">
                <span className={cn("inline-flex items-center gap-1 px-1.5 py-0.5 rounded border text-xs", bg, color)}>
                  {icon}
                  <span className="text-xs opacity-70">{nodeType}</span>
                  {nodeId && <span className="text-xs font-medium">{nodeId.slice(0, 40)}</span>}
                </span>
              </div>
            );
          }

          return null;
        })}

        {isStreaming && (
          <div className="animate-pulse text-xs text-sky-600 flex items-center gap-1 pl-5 py-0.5">
            <span className="w-1 h-1 bg-sky-600 rounded-full" />
            traversing...
          </div>
        )}
      </div>
    </div>
  );
}
