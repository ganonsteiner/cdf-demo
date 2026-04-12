import { useEffect, useState, useMemo, useRef } from "react";
import {
  Box,
  Crosshair,
  FileText,
  LineChart,
  RotateCcw,
  Share2,
  AlertTriangle,
  Waypoints,
} from "lucide-react";
import {
  cn,
  CARD_SURFACE_A,
  CARD_SURFACE_B,
  MAIN_TAB_CONTENT_FRAME,
  TAB_PAGE_TOP_INSET,
} from "../lib/utils";
import { api } from "../lib/api";
import { highlightedGraphIdsFromTraversal } from "../lib/traversalGraphIds";
import { useStore } from "../lib/store";
import type { GraphData, GraphLink, GraphNode } from "../lib/types";
import TraversalGraph, { type TraversalGraphHandle } from "./TraversalGraph";

interface Props {
  active: boolean;
}

export default function KnowledgeGraph({ active }: Props) {
  const { traversalEvents, setGraphDataSnapshot } = useStore();
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [_selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<TraversalGraphHandle>(null);
  const [viewport, setViewport] = useState<{ width: number; height: number }>({ width: 0, height: 0 });

  /** Load once per page session; revisiting the tab keeps data and TraversalGraph state (no re-fetch, no re-animation). */
  useEffect(() => {
    if (!active) return;
    if (graphData !== null) return;
    setLoading(true);
    setError(null);
    api
      .graph()
      .then(setGraphData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [active, graphData]);

  useEffect(() => {
    if (graphData) setGraphDataSnapshot(graphData);
  }, [graphData, setGraphDataSnapshot]);

  // Measure while mounted — graph tab stays in layout (absolute when inactive) so size stays valid.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const update = () => {
      const box = containerRef.current;
      if (box) {
        const w = box.clientWidth;
        const h = box.clientHeight;
        if (w > 0 && h > 0) {
          setViewport({ width: w, height: h });
        }
      }
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    const id = requestAnimationFrame(() => update());
    return () => {
      cancelAnimationFrame(id);
      ro.disconnect();
    };
  }, []);

  /** Distinct graph node ids from the traversal log (yellow ring; legend “N nodes traversed”). */
  const traversedIds = useMemo(
    () => highlightedGraphIdsFromTraversal(traversalEvents, graphData),
    [traversalEvents, graphData]
  );

  const allEdgeTypes = useMemo(() => {
    if (!graphData?.links.length) return [] as string[];
    const s = new Set<string>();
    for (const l of graphData.links) {
      if (l.type) s.add(l.type);
    }
    return Array.from(s).sort();
  }, [graphData?.links]);

  const [visibleEdgeTypes, setVisibleEdgeTypes] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (graphData && allEdgeTypes.length) {
      setVisibleEdgeTypes(new Set(allEdgeTypes));
    }
  }, [graphData, allEdgeTypes]);

  const filteredGraphData = useMemo((): GraphData | null => {
    if (!graphData) return null;
    if (visibleEdgeTypes.size === 0) {
      return { ...graphData, links: [] };
    }
    const links = graphData.links.filter((l: GraphLink) => visibleEdgeTypes.has(l.type));
    return { ...graphData, links };
  }, [graphData, visibleEdgeTypes]);

  const toggleEdgeType = (t: string) => {
    setVisibleEdgeTypes((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return next;
    });
  };

  const stats = graphData?.stats;

  return (
    <div
      className={cn(
        "flex flex-1 min-h-0 flex-col overflow-hidden pb-6 relative",
        MAIN_TAB_CONTENT_FRAME,
        TAB_PAGE_TOP_INSET
      )}
    >
      {/* Tab→graph card ≈ AI chat: TAB_PAGE_TOP_INSET + chrome row + mb-1. Min-height offsets smaller mb-1 vs AI mb-3; items-end keeps stats flush above the card. */}
      <div className="shrink-0 mb-1 flex min-h-[34px] flex-wrap items-end justify-end gap-3 sm:min-h-[30px]">
        {stats && (
          <div className="flex items-center gap-3">
            {[
              { label: "Assets", count: stats.assets, icon: <Box className="w-3 h-3" />, color: "text-sky-400" },
              { label: "TimeSeries", count: stats.timeseries, icon: <LineChart className="w-3 h-3" aria-hidden />, color: "text-emerald-400" },
              { label: "Files", count: stats.files, icon: <FileText className="w-3 h-3" />, color: "text-purple-400" },
              { label: "Relations", count: stats.relationships, icon: <Share2 className="w-3 h-3" aria-hidden />, color: "text-violet-400" },
            ].map((s) => (
              <div key={s.label} className={`flex items-center gap-1.5 text-xs ${s.color}`}>
                {s.icon}
                <span className="font-semibold">{s.count}</span>
                <span className="text-zinc-600 hidden sm:inline">{s.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Graph canvas */}
      <div
        ref={containerRef}
        className={cn("flex-1 min-h-[16rem] sm:min-h-[24rem] rounded-xl overflow-hidden relative", CARD_SURFACE_A)}
      >
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center text-zinc-600">
            <div className="text-center">
              <Waypoints className="w-10 h-10 mb-3 animate-pulse mx-auto" />
              <p className="text-sm">Loading knowledge graph…</p>
            </div>
          </div>
        )}

        {error && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="flex flex-col items-center gap-3 text-center px-8">
              <AlertTriangle className="w-10 h-10 text-yellow-400" />
              <p className="text-sm text-zinc-300">Could not load graph data</p>
              <p className="text-xs text-zinc-600 font-mono max-w-sm">{error}</p>
            </div>
          </div>
        )}

        {!loading &&
          !error &&
          filteredGraphData &&
          viewport.width > 0 &&
          viewport.height > 0 && (
            <TraversalGraph
              ref={graphRef}
              active={active}
              data={filteredGraphData}
              traversedIds={traversedIds}
              onNodeClick={setSelectedNode}
              width={viewport.width}
              height={viewport.height}
            />
          )}

        {graphData &&
          !loading &&
          !error &&
          filteredGraphData &&
          viewport.width > 0 &&
          viewport.height > 0 && (
            <div className="absolute bottom-3 right-3 z-10 flex gap-2">
              <button
                type="button"
                className="inline-flex items-center gap-1.5 rounded-lg border border-zinc-700 bg-zinc-900/90 px-2.5 py-1.5 text-xs text-zinc-300 backdrop-blur-sm hover:border-zinc-600 hover:bg-zinc-800/90 focus:outline-none focus:border-sky-600"
                title="Fit graph in view"
                aria-label="Recenter graph"
                onClick={() => graphRef.current?.recenter()}
              >
                <Crosshair className="h-3.5 w-3.5 shrink-0" aria-hidden />
                <span className="hidden sm:inline">Recenter</span>
              </button>
              <button
                type="button"
                className="inline-flex items-center gap-1.5 rounded-lg border border-zinc-700 bg-zinc-900/90 px-2.5 py-1.5 text-xs text-zinc-300 backdrop-blur-sm hover:border-zinc-600 hover:bg-zinc-800/90 focus:outline-none focus:border-sky-600"
                title="Animate back to the default settled layout"
                aria-label="Reset graph layout"
                onClick={() => graphRef.current?.resetLayout()}
              >
                <RotateCcw className="h-3.5 w-3.5 shrink-0" aria-hidden />
                <span className="hidden sm:inline">Reset</span>
              </button>
            </div>
          )}

        {/* Legend overlay */}
        {graphData && !loading && (
          <div className={cn("absolute top-3 left-3 rounded-lg px-3 py-2 backdrop-blur-sm max-w-[220px] max-h-[min(70vh,520px)] overflow-y-auto bg-zinc-900/90 border-zinc-800", CARD_SURFACE_B)}>
            <p className="text-xs font-semibold text-zinc-500 mb-1.5 uppercase tracking-widest">
              Node types
            </p>
            <div className="space-y-1">
              {[
                { type: "Asset", color: "#38bdf8" },
                { type: "Engine model", color: "#f1f5f9" },
                { type: "TimeSeries", color: "#4ade80" },
                { type: "File", color: "#c084fc" },
              ].map((l) => (
                <div key={l.type} className="flex items-center gap-2 text-xs text-zinc-400">
                  <span
                    className="w-2.5 h-2.5 rounded-full shrink-0 border border-zinc-600"
                    style={{ backgroundColor: l.color }}
                  />
                  {l.type}
                </div>
              ))}
            </div>
            {allEdgeTypes.length > 0 && (
              <>
                <p className="text-xs font-semibold text-zinc-500 mb-1 mt-2 pt-2 border-t border-zinc-800/60 uppercase tracking-widest">
                  Edge types
                </p>
                <div className="space-y-1">
                  {allEdgeTypes.map((t) => {
                    const on = visibleEdgeTypes.has(t);
                    const sample = graphData.links.find((l) => l.type === t);
                    const swatch = sample?.color || "#666";
                    const isTypeStyle = t === "IS_TYPE";
                    return (
                      <label
                        key={t}
                        className="flex items-center gap-2 text-xs text-zinc-400 cursor-pointer select-none"
                      >
                        <input
                          type="checkbox"
                          checked={on}
                          onChange={() => toggleEdgeType(t)}
                          className="rounded border-zinc-600"
                        />
                        <span
                          className="w-6 h-0.5 shrink-0"
                          style={{
                            backgroundColor: isTypeStyle ? "transparent" : swatch,
                            borderTop: isTypeStyle ? `2px dashed ${swatch}` : undefined,
                          }}
                        />
                        <span className="truncate font-mono">{t}</span>
                      </label>
                    );
                  })}
                </div>
              </>
            )}
            {traversedIds.size > 0 && (
              <div className="flex items-center gap-2 text-xs text-yellow-400 mt-1.5 border-t border-zinc-800/60 pt-1.5">
                <span className="w-2.5 h-2.5 rounded-full bg-yellow-400 shrink-0" />
                {traversedIds.size} node{traversedIds.size === 1 ? "" : "s"} traversed
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
