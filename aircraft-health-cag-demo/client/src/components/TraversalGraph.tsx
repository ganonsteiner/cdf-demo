/**
 * TraversalGraph — shared force-directed knowledge graph visualization.
 *
 * Node types map to CDF resource types (aligned with GraphTraversalPanel):
 *   asset       → #38bdf8
 *   EngineModel → #f1f5f9 (larger radius — hub for IS_TYPE)
 *   timeseries  → #4ade80
 *   event       → #fb923c
 *   file        → #c084fc
 *
 * IS_TYPE edges use color from API (#e0f2fe) and a dashed stroke.
 */

import {
  useRef,
  useEffect,
  useCallback,
  useState,
  useMemo,
  forwardRef,
  useImperativeHandle,
} from "react";
import ForceGraph2D from "react-force-graph-2d";
import type { GraphData, GraphLink, GraphNode } from "../lib/types";

/** Imperative controls for the knowledge graph canvas (reset layout, zoom-to-fit). */
export interface TraversalGraphHandle {
  /** Ease nodes from their current positions back to the default settled layout (first equilibrium after load for this graph), then zoom-to-fit. */
  resetLayout: () => void;
  /** Fit current node positions in view without changing the simulation. */
  recenter: () => void;
}

const NODE_COLOR: Record<string, string> = {
  asset: "#38bdf8",
  EngineModel: "#f1f5f9",
  SymptomNode: "#f97316",
  OperationalPolicy: "#a855f7",
  FleetOwner: "#c084fc",
  timeseries: "#4ade80",
  event: "#fb923c",
  file: "#c084fc",
};

const DEFAULT_LINK = "rgba(113,113,122,0.3)";
const HIGHLIGHT_COLOR = "#facc15";

const ZOOM_PADDING_PX = 40;
/** Single smooth zoom when layout settles (no mid-sim snap — that read as jerky with a second zoom). */
const FINAL_ZOOM_MS = 320;
const RESIZE_ZOOM_MS = 220;
const RESIZE_DEBOUNCE_MS = 120;
/** Duration for reset: interpolate from current positions to the stored default layout. */
const RESET_TWEEN_MS = 780;
/** If every node is within this distance (graph coords) of the stored default, Reset is a no-op. */
const RESET_LAYOUT_EPS = 3;

function easeOutCubic(t: number): number {
  return 1 - Math.pow(1 - t, 3);
}

/** Nudge the camera in place so the force-graph marks the canvas dirty (no layout reheat). */
function forceGraphRedraw(fg: { centerAt: (...args: unknown[]) => unknown } | null | undefined) {
  if (!fg?.centerAt) return;
  const c = fg.centerAt() as { x: number; y: number } | null | undefined;
  if (c && Number.isFinite(c.x) && Number.isFinite(c.y)) {
    fg.centerAt(c.x, c.y, 0);
  }
}

type SimNode = GraphNode & {
  x: number;
  y: number;
  vx?: number;
  vy?: number;
  fx?: number;
  fy?: number;
};

function nodeRadius(node: GraphNode): number {
  const base = 3 + Math.min(10, node.linkCount ?? 0) * 0.8;
  let r = Math.max(4, Math.min(14, base));
  if (node.type === "EngineModel") {
    r = Math.min(22, Math.max(12, r + 6));
  }
  return r;
}

/** Deterministic tight ring around origin so the first frame is centered before forces spread the graph. */
function graphDataWithCenteredSeed(data: GraphData): GraphData {
  const n = data.nodes.length;
  const R = Math.min(28, 8 + n * 0.35);
  const nodes = data.nodes.map((node, i) => {
    const angle = (2 * Math.PI * i) / Math.max(n, 1);
    return {
      ...node,
      x: R * Math.cos(angle),
      y: R * Math.sin(angle),
    };
  });
  return {
    ...data,
    nodes,
    links: data.links.map((l) => ({ ...l })),
  };
}

function graphRevision(data: GraphData): string {
  return `${data.nodes.length}:${data.links.map((l) => `${l.source}>${l.target}:${l.type}`).join("|")}`;
}

interface SelectedNode extends GraphNode {
  x?: number;
  y?: number;
}

interface Props {
  /** False when the KG tab is in the background — pauses the canvas loop without resetting layout. */
  active?: boolean;
  data: GraphData;
  traversedIds?: Set<string>;
  onNodeClick?: (node: GraphNode) => void;
  /** Viewport width in px; must be > 0 before mount. */
  width: number;
  /** Viewport height in px; must be > 0 before mount. */
  height: number;
}

const TraversalGraph = forwardRef<TraversalGraphHandle, Props>(function TraversalGraph(
  { active = true, data, traversedIds = new Set(), onNodeClick, width, height },
  ref
) {
  const fgRef = useRef<any>(null);
  const [selectedNode, setSelectedNode] = useState<SelectedNode | null>(null);
  const hasInitialFitRef = useRef(false);
  const revisionRef = useRef<string>("");
  const resizeDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const prevViewportRef = useRef<{ w: number; h: number }>({ w: 0, h: 0 });
  /** Node positions after the first layout settle for this graph revision (the “default” layout). */
  const defaultPositionsRef = useRef<Map<string, { x: number; y: number }>>(new Map());
  const resetTweenRafRef = useRef<number | null>(null);
  const snapshotRafRef = useRef<number | null>(null);
  /** Bumped on graph revision so deferred layout snapshots never overwrite a newer graph. */
  const layoutEpochRef = useRef(0);

  const seededData = useMemo(() => graphDataWithCenteredSeed(data), [data]);

  const revision = useMemo(() => graphRevision(data), [data]);

  useEffect(() => {
    if (revision === revisionRef.current) return;
    revisionRef.current = revision;
    layoutEpochRef.current += 1;
    hasInitialFitRef.current = false;
    defaultPositionsRef.current = new Map();
    if (snapshotRafRef.current != null) {
      cancelAnimationFrame(snapshotRafRef.current);
      snapshotRafRef.current = null;
    }
  }, [revision]);

  const runZoomToFit = useCallback((durationMs: number) => {
    const fg = fgRef.current;
    if (!fg || data.nodes.length === 0) return;
    fg.zoomToFit(durationMs, ZOOM_PADDING_PX);
  }, [data.nodes.length]);

  const captureDefaultLayout = useCallback(() => {
    const m = new Map<string, { x: number; y: number }>();
    for (const n of seededData.nodes) {
      const nn = n as SimNode;
      if (Number.isFinite(nn.x) && Number.isFinite(nn.y)) {
        m.set(n.id, { x: nn.x, y: nn.y });
      }
    }
    return m;
  }, [seededData]);

  const onEngineStop = useCallback(() => {
    if (data.nodes.length === 0) return;
    if (!hasInitialFitRef.current) {
      hasInitialFitRef.current = true;
      defaultPositionsRef.current = captureDefaultLayout();
      runZoomToFit(FINAL_ZOOM_MS);
      // Re-sample after zoom + paint so “default” matches what the user sees once fully still.
      if (snapshotRafRef.current != null) cancelAnimationFrame(snapshotRafRef.current);
      const epoch = layoutEpochRef.current;
      snapshotRafRef.current = requestAnimationFrame(() => {
        snapshotRafRef.current = requestAnimationFrame(() => {
          snapshotRafRef.current = null;
          if (epoch !== layoutEpochRef.current) return;
          defaultPositionsRef.current = captureDefaultLayout();
        });
      });
    }
  }, [data.nodes.length, runZoomToFit, captureDefaultLayout]);

  const resetLayout = useCallback(() => {
    if (data.nodes.length === 0) return;
    if (resetTweenRafRef.current != null) {
      cancelAnimationFrame(resetTweenRafRef.current);
      resetTweenRafRef.current = null;
    }
    setSelectedNode(null);

    const targets = defaultPositionsRef.current;
    const fg = fgRef.current;

    const releasePinsZeroVelocity = () => {
      for (const n of seededData.nodes) {
        const node = n as SimNode;
        delete node.fx;
        delete node.fy;
        node.vx = 0;
        node.vy = 0;
      }
    };

    // No snapshot yet (e.g. reset during the very first intro): gently reheat from wherever nodes are.
    if (targets.size === 0) {
      releasePinsZeroVelocity();
      hasInitialFitRef.current = false;
      fg?.d3ReheatSimulation?.();
      return;
    }

    let maxDelta = 0;
    for (const n of seededData.nodes) {
      const end = targets.get(n.id);
      if (!end) continue;
      const nn = n as SimNode;
      if (!Number.isFinite(nn.x) || !Number.isFinite(nn.y)) continue;
      const d = Math.hypot(nn.x - end.x, nn.y - end.y);
      if (d > maxDelta) maxDelta = d;
    }

    if (maxDelta <= RESET_LAYOUT_EPS) {
      releasePinsZeroVelocity();
      return;
    }

    const starts = new Map<string, { x: number; y: number }>();
    for (const n of seededData.nodes) {
      const end = targets.get(n.id);
      if (!end) continue;
      const nn = n as SimNode;
      if (Number.isFinite(nn.x) && Number.isFinite(nn.y)) {
        starts.set(n.id, { x: nn.x, y: nn.y });
      }
    }

    // Pin every node at its current coordinates so nothing moves until the tween updates fx/fy.
    for (const n of seededData.nodes) {
      const nn = n as SimNode;
      if (!Number.isFinite(nn.x) || !Number.isFinite(nn.y)) continue;
      const s0 = starts.get(n.id);
      nn.fx = s0 ? s0.x : nn.x;
      nn.fy = s0 ? s0.y : nn.y;
      nn.vx = 0;
      nn.vy = 0;
    }

    // Do not call d3ReheatSimulation here: a running layout would still be cooling when we unpin at
    // the end and would pull nodes off the snapshot. Redraw-only each frame via centerAt.
    fg?.resumeAnimation?.();

    const t0 = performance.now();
    const step = () => {
      const elapsed = performance.now() - t0;
      const t = Math.min(1, elapsed / RESET_TWEEN_MS);
      const e = easeOutCubic(t);

      for (const n of seededData.nodes) {
        const end = targets.get(n.id);
        const s0 = starts.get(n.id);
        if (!end || !s0) continue;
        const node = n as SimNode;
        node.fx = s0.x + (end.x - s0.x) * e;
        node.fy = s0.y + (end.y - s0.y) * e;
      }

      forceGraphRedraw(fgRef.current);

      if (t < 1) {
        resetTweenRafRef.current = requestAnimationFrame(step);
      } else {
        resetTweenRafRef.current = null;
        for (const n of seededData.nodes) {
          const end = targets.get(n.id);
          if (!end) continue;
          const node = n as SimNode;
          node.x = end.x;
          node.y = end.y;
          delete node.fx;
          delete node.fy;
          node.vx = 0;
          node.vy = 0;
        }
        runZoomToFit(FINAL_ZOOM_MS);
      }
    };
    resetTweenRafRef.current = requestAnimationFrame(step);
  }, [data.nodes.length, seededData, runZoomToFit]);

  useEffect(() => {
    return () => {
      if (resetTweenRafRef.current != null) {
        cancelAnimationFrame(resetTweenRafRef.current);
        resetTweenRafRef.current = null;
      }
      if (snapshotRafRef.current != null) {
        cancelAnimationFrame(snapshotRafRef.current);
        snapshotRafRef.current = null;
      }
    };
  }, []);

  useImperativeHandle(
    ref,
    () => ({
      resetLayout,
      recenter: () => runZoomToFit(FINAL_ZOOM_MS),
    }),
    [resetLayout, runZoomToFit]
  );

  useEffect(() => {
    if (width <= 0 || height <= 0) return;
    const prev = prevViewportRef.current;
    const isFirstSize = prev.w === 0 && prev.h === 0;
    prevViewportRef.current = { w: width, h: height };
    if (isFirstSize) return;

    if (resizeDebounceRef.current) clearTimeout(resizeDebounceRef.current);
    resizeDebounceRef.current = setTimeout(() => {
      resizeDebounceRef.current = null;
      if (!fgRef.current || !hasInitialFitRef.current) return;
      runZoomToFit(RESIZE_ZOOM_MS);
    }, RESIZE_DEBOUNCE_MS);
    return () => {
      if (resizeDebounceRef.current) clearTimeout(resizeDebounceRef.current);
    };
  }, [width, height, runZoomToFit]);

  /** Pause RAF while tab hidden (saves work); resume when visible so zoom/pan still redraw. */
  useEffect(() => {
    const fg = fgRef.current;
    if (!fg) return;
    if (active) fg.resumeAnimation?.();
    else fg.pauseAnimation?.();
  }, [active]);

  const drawNode = useCallback(
    (node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const gNode = node as GraphNode & { x: number; y: number };
      const r = nodeRadius(gNode);
      const isHighlighted = traversedIds.has(gNode.id);
      const isSelected = selectedNode?.id === gNode.id;

      if (isHighlighted || isSelected) {
        ctx.beginPath();
        ctx.arc(gNode.x, gNode.y, r + 3, 0, 2 * Math.PI);
        ctx.fillStyle = isSelected ? "rgba(250,204,21,0.25)" : "rgba(250,204,21,0.15)";
        ctx.fill();
        ctx.strokeStyle = HIGHLIGHT_COLOR;
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      ctx.beginPath();
      ctx.arc(gNode.x, gNode.y, r, 0, 2 * Math.PI);
      ctx.fillStyle = NODE_COLOR[gNode.type] || "#71717a";
      ctx.globalAlpha = isHighlighted ? 1 : 0.85;
      ctx.fill();
      ctx.globalAlpha = 1;
      if (gNode.type === "EngineModel") {
        ctx.strokeStyle = "rgba(148,163,184,0.5)";
        ctx.lineWidth = 1;
        ctx.stroke();
      }

      const fontSize = 10 / globalScale;
      if (globalScale > 1.5 || r >= 10) {
        const label = gNode.label.length > 16 ? gNode.label.slice(0, 14) + "…" : gNode.label;
        ctx.font = `${fontSize}px monospace`;
        ctx.fillStyle = gNode.type === "EngineModel" ? "rgba(15,23,42,0.9)" : "rgba(255,255,255,0.8)";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(label, gNode.x, gNode.y + r + fontSize * 0.8);
      }
    },
    [traversedIds, selectedNode]
  );

  const handleNodeClick = useCallback(
    (node: any) => {
      setSelectedNode((prev) => (prev?.id === node.id ? null : node));
      onNodeClick?.(node as GraphNode);
    },
    [onNodeClick]
  );

  const linkColor = useCallback((link: GraphLink) => (link as GraphLink).color || DEFAULT_LINK, []);

  const linkLineDash = useCallback((link: GraphLink) => {
    return link.type === "IS_TYPE" ? [4, 4] : undefined;
  }, []);

  const linkWidth = useCallback((link: GraphLink) => (link.type === "IS_TYPE" ? 1 : 0.5), []);

  return (
    <div className="relative h-full w-full" style={{ width, height }}>
      <ForceGraph2D
        ref={fgRef}
        graphData={seededData as any}
        width={width}
        height={height}
        backgroundColor="#09090b"
        nodeCanvasObject={drawNode}
        nodeCanvasObjectMode={() => "replace"}
        linkColor={linkColor as any}
        linkWidth={linkWidth as any}
        linkLineDash={linkLineDash as any}
        linkDirectionalParticles={2}
        linkDirectionalParticleWidth={1}
        linkDirectionalParticleColor={linkColor as any}
        onNodeClick={handleNodeClick}
        onEngineStop={onEngineStop}
        cooldownTicks={85}
        d3AlphaDecay={0.042}
        d3VelocityDecay={0.32}
      />

      {selectedNode && (
        <div className="absolute top-3 right-3 bg-zinc-900/95 border border-zinc-700 rounded-xl p-4 max-w-xs text-xs shadow-xl backdrop-blur-sm">
          <div className="flex items-center gap-2 mb-2">
            <span
              className="w-2.5 h-2.5 rounded-full shrink-0"
              style={{ backgroundColor: NODE_COLOR[selectedNode.type] || "#71717a" }}
            />
            <span className="font-semibold text-zinc-100 truncate">{selectedNode.label}</span>
            <button
              className="ml-auto text-zinc-600 hover:text-zinc-300 shrink-0"
              onClick={() => setSelectedNode(null)}
            >
              ×
            </button>
          </div>
          <div className="space-y-1 text-zinc-400">
            <div className="flex gap-2">
              <span className="text-zinc-600 w-14 shrink-0">Type</span>
              <span className="capitalize">{selectedNode.type}</span>
            </div>
            <div className="flex gap-2">
              <span className="text-zinc-600 w-14 shrink-0">ID</span>
              <span className="font-mono truncate text-zinc-300">{selectedNode.id}</span>
            </div>
            <div className="flex gap-2">
              <span className="text-zinc-600 w-14 shrink-0">Links</span>
              <span>{selectedNode.linkCount ?? 0}</span>
            </div>
            {selectedNode.unit && (
              <div className="flex gap-2">
                <span className="text-zinc-600 w-14 shrink-0">Unit</span>
                <span>{selectedNode.unit}</span>
              </div>
            )}
            {selectedNode.metadata &&
              Object.entries(selectedNode.metadata).slice(0, 4).map(([k, v]) => (
                <div key={k} className="flex gap-2">
                  <span className="text-zinc-600 w-14 shrink-0 truncate">{k}</span>
                  <span className="truncate">{String(v).slice(0, 40)}</span>
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  );
});

TraversalGraph.displayName = "TraversalGraph";

export default TraversalGraph;
