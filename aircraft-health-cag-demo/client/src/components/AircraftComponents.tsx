import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { Puzzle, ChevronRight, AlertTriangle, Wrench } from "lucide-react";
import {
  cn,
  CARD_SURFACE_A,
  CARD_SURFACE_B,
  MAIN_TAB_CONTENT_FRAME,
  TAB_PAGE_TOP_INSET,
  calendarDaysUntil,
  formatDate,
  toneClasses,
} from "../lib/utils";
import { api } from "../lib/api";
import { useStore, TAILS } from "../lib/store";
import type { ComponentNode, MaintenanceRecord } from "../lib/types";
interface Props {
  active: boolean;
}

/** Strip leading `N4798E — ` style prefix from CDF asset names; tail is already shown in the page strip. */
function componentDisplayName(name: string, tail: string): string {
  const escaped = tail.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const stripped = name.replace(new RegExp(`^${escaped}\\s*[\\u2014\\-–]\\s*`), "").trim();
  return stripped.length > 0 ? stripped : name;
}

function buildTree(nodes: ComponentNode[]): Map<string | null, ComponentNode[]> {
  const tree = new Map<string | null, ComponentNode[]>();
  for (const node of nodes) {
    const parent = node.parentExternalId ?? null;
    if (!tree.has(parent)) tree.set(parent, []);
    tree.get(parent)!.push(node);
  }
  return tree;
}

function statusDot(status: ComponentNode["status"]) {
  if (status === "overdue") return "bg-red-500";
  if (status === "due_soon") return "bg-yellow-400";
  return "bg-emerald-500";
}

/** Same relative component on another aircraft (N4798E-ENGINE → N2251K-ENGINE). Unknown ids → root. */
function mapComponentExternalIdForTailChange(
  id: string,
  fromTail: string,
  toTail: string
): string {
  if (fromTail === toTail) return id;
  if (id === fromTail) return toTail;
  const prefix = `${fromTail}-`;
  if (id.startsWith(prefix)) {
    return `${toTail}-${id.slice(prefix.length)}`;
  }
  return toTail;
}

/** True when this payload is for the given aircraft (avoids reconciling selection against a stale tail's graph). */
function componentsBelongToTail(nodes: ComponentNode[], aircraftTail: string): boolean {
  return nodes.some((c) => c.externalId === aircraftTail);
}

export default function AircraftComponents({ active }: Props) {
  const { selectedAircraft, setSelectedAircraft } = useStore();
  const tail = selectedAircraft ?? "N4798E";
  const [components, setComponents] = useState<ComponentNode[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [compHistory, setCompHistory] = useState<MaintenanceRecord[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const prevTailRef = useRef<string | null>(null);
  /** False while another app tab is focused — used to detect return to Components. */
  const prevAppTabActiveRef = useRef(false);

  useLayoutEffect(() => {
    if (!active) {
      prevAppTabActiveRef.current = false;
      return;
    }

    const reenteredComponentsTab = !prevAppTabActiveRef.current;
    prevAppTabActiveRef.current = true;

    setLoading(true);
    setError(null);
    setComponents([]);

    const prev = prevTailRef.current;
    if (reenteredComponentsTab) {
      setSelectedId(tail);
    } else if (prev !== null && prev !== tail) {
      setSelectedId((sid) =>
        sid === null ? null : mapComponentExternalIdForTailChange(sid, prev, tail)
      );
    }
    prevTailRef.current = tail;
  }, [active, tail]);

  useEffect(() => {
    if (!active) return;
    const onVisibility = () => {
      if (document.visibilityState !== "visible") return;
      setSelectedId(tail);
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, [active, tail]);

  useEffect(() => {
    if (!active) return;
    const ac = new AbortController();
    api
      .components(tail, { signal: ac.signal })
      .then((data) => {
        if (ac.signal.aborted) return;
        setComponents(data);
      })
      .catch((e: unknown) => {
        if (ac.signal.aborted) return;
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (ac.signal.aborted) return;
        setLoading(false);
      });
    return () => ac.abort();
  }, [active, tail]);

  /**
   * Default to aircraft root when there is no valid selection; keep remapped id once the graph matches this tail.
   * Must ignore stale component lists (out-of-order fetches) so we do not snap back to root.
   */
  useEffect(() => {
    if (!active || loading) return;
    if (components.length === 0) return;
    if (!componentsBelongToTail(components, tail)) return;
    setSelectedId((sid) => {
      const ok = sid !== null && components.some((c) => c.externalId === sid);
      if (ok) return sid;
      return tail;
    });
  }, [active, loading, components, tail]);

  useEffect(() => {
    if (!selectedId) {
      setCompHistory([]);
      setHistoryLoading(false);
      return;
    }
    const ac = new AbortController();
    setHistoryLoading(true);
    api
      .maintenanceHistory(tail, { component: selectedId, per_page: 50, signal: ac.signal })
      .then((res) => {
        if (ac.signal.aborted) return;
        setCompHistory(res.records);
      })
      .catch(() => {
        if (ac.signal.aborted) return;
        setCompHistory([]);
      })
      .finally(() => {
        if (ac.signal.aborted) return;
        setHistoryLoading(false);
      });
    return () => ac.abort();
  }, [selectedId, tail]);

  const tree = buildTree(components);

  function renderTree(parentId: string | null, depth: number): React.ReactNode {
    const children = tree.get(parentId);
    if (!children) return null;
    return children.map((node) => {
      const hasChildren = tree.has(node.externalId);
      return (
        <div key={node.externalId}>
          <button
            onClick={() =>
              setSelectedId((prev) => (prev === node.externalId ? null : node.externalId))
            }
            className={cn(
              "w-full flex items-center gap-2 px-3 py-2.5 rounded-lg text-left text-sm transition-colors group",
              selectedId === node.externalId
                ? "bg-sky-950/50 border border-sky-800/50 text-sky-100"
                : "hover:bg-zinc-900/80 border border-transparent"
            )}
          >
            <span className={cn("w-2 h-2 rounded-full shrink-0", statusDot(node.status))} />
            <span className="flex-1 min-w-0">
              <span className="text-zinc-200 font-medium truncate block">
                {componentDisplayName(node.name, tail)}
              </span>
              <span className="text-xs text-zinc-500 font-mono truncate block">{node.externalId}</span>
            </span>
            {(node.maintenanceCount > 0 || hasChildren) && (
              <div className="flex items-center gap-2 shrink-0">
                {node.maintenanceCount > 0 && (
                  <span className="text-xs text-zinc-600">{node.maintenanceCount}</span>
                )}
                {hasChildren && (
                  <ChevronRight className="w-3.5 h-3.5 text-zinc-600 group-hover:text-zinc-400" />
                )}
              </div>
            )}
          </button>
          {/* Children indented with a left border connector line */}
          {hasChildren && (
            <div className="ml-5 pl-3 border-l border-zinc-700/60">
              {renderTree(node.externalId, depth + 1)}
            </div>
          )}
        </div>
      );
    });
  }

  const selectedComp = components.find((c) => c.externalId === selectedId);

  const treeSkeletonMargins = [0, 0, 20, 20, 40, 20, 20, 0];

  return (
    <div
      className={cn(
        "flex flex-1 min-h-0 flex-col overflow-hidden pb-6",
        MAIN_TAB_CONTENT_FRAME,
        TAB_PAGE_TOP_INSET
      )}
    >
      <div className="shrink-0 mb-3">
        <div className="flex items-center gap-2">
          <div className="flex gap-1 flex-wrap">
            {TAILS.map((t) => (
              <button
                key={t}
                type="button"
                onClick={() => setSelectedAircraft(t)}
                className={cn(
                  "px-2.5 py-0.5 rounded-full text-xs font-medium border transition-colors",
                  tail === t
                    ? "bg-sky-600 text-white border-sky-500"
                    : "bg-zinc-800 text-zinc-400 border-zinc-700 hover:border-zinc-500"
                )}
              >
                {t}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex-1 flex gap-4 overflow-hidden min-h-0">
        <div className={cn("flex-1 min-w-0 rounded-xl overflow-y-auto", CARD_SURFACE_B)}>
          <div className="px-4 py-3 border-b border-zinc-800 flex items-center justify-between gap-3 flex-wrap">
            <div className="flex items-center gap-2 min-w-0">
              <Puzzle className="w-3.5 h-3.5 text-zinc-500 shrink-0" aria-hidden />
              <span className="text-xs font-semibold text-zinc-500 uppercase tracking-widest">
                Aircraft Components
              </span>
            </div>
            <div className="flex items-center gap-4 shrink-0">
              {[
                { label: "OK", color: "bg-emerald-500" },
                { label: "Due soon", color: "bg-yellow-400" },
                { label: "Overdue", color: "bg-red-500" },
              ].map((l) => (
                <span key={l.label} className="flex items-center gap-1.5 text-xs text-zinc-500">
                  <span className={cn("w-2 h-2 rounded-full shrink-0", l.color)} />
                  {l.label}
                </span>
              ))}
            </div>
          </div>

          <div className="p-2 space-y-0.5">
            {loading ? (
              <div className="space-y-0.5" aria-busy="true">
                {treeSkeletonMargins.map((ml, i) => (
                  <div
                    key={i}
                    className="flex items-center gap-2 px-3 py-2.5 rounded-lg animate-pulse"
                    style={{ marginLeft: ml }}
                  >
                    <div className="w-2 h-2 rounded-full bg-zinc-800 shrink-0" />
                    <div className="flex-1 space-y-1.5 min-w-0">
                      <div className="h-3.5 bg-zinc-800 rounded w-32 max-w-[70%]" />
                      <div className="h-3 bg-zinc-800/80 rounded w-28 max-w-[55%]" />
                    </div>
                    <div className="w-3.5 h-3.5 rounded bg-zinc-800 shrink-0" />
                  </div>
                ))}
              </div>
            ) : error ? (
              <div
                className={cn(
                  "flex items-center gap-3 p-4 m-2 rounded-xl",
                  toneClasses("bad").bannerPanel
                )}
              >
                <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
                <p className="text-sm text-red-300">{error}</p>
              </div>
            ) : (
              renderTree(null, 0)
            )}
          </div>
        </div>

        <div className="w-80 shrink-0 flex flex-col gap-4 min-h-0">
        {selectedComp ? (
          <>
            {/* Component card */}
            <div className={cn("rounded-xl p-4", CARD_SURFACE_B)}>
              <div className="flex items-start gap-2 mb-3">
                <span className={cn("w-2.5 h-2.5 rounded-full mt-1 shrink-0", statusDot(selectedComp.status))} />
                <div className="min-w-0">
                  <p className="font-semibold text-zinc-100 text-sm leading-tight">
                    {componentDisplayName(selectedComp.name, tail)}
                  </p>
                  <p className="text-xs font-mono text-zinc-500 mt-0.5">{selectedComp.externalId}</p>
                </div>
              </div>
              {selectedComp.description && (
                <p className="text-xs text-zinc-500 mb-3">{selectedComp.description}</p>
              )}
              <div className="space-y-2 text-xs">
                <DetailRow
                  label="Current hobbs"
                  value={`${selectedComp.currentHobbs.toFixed(1)} hr`}
                />
                <DetailRow
                  label="Current tach"
                  value={`${(selectedComp.currentTach ?? 0).toFixed(1)} hr`}
                />
                <DetailRow
                  label="Last maintenance"
                  value={
                    <span className="whitespace-nowrap">
                      {formatDate(selectedComp.lastMaintenanceDate) ?? "No records"}
                    </span>
                  }
                />
                {selectedComp.nextDueTach != null && (
                  <DetailRow
                    label="Next due (tach)"
                    value={
                      <>
                        {`${selectedComp.nextDueTach.toFixed(1)} hr`}
                        {selectedComp.hoursUntilDue !== null && (
                          <span className="whitespace-nowrap">
                            {` (${selectedComp.hoursUntilDue.toFixed(1)} hr)`}
                          </span>
                        )}
                      </>
                    }
                    valueTone={
                      selectedComp.hoursUntilDue !== null && selectedComp.hoursUntilDue < 0
                        ? "bad"
                        : selectedComp.status === "due_soon"
                          ? "warn"
                          : "default"
                    }
                  />
                )}
                {selectedComp.nextDueDate && (
                  <DetailRow
                    label="Due date"
                    value={(() => {
                      const fd = formatDate(selectedComp.nextDueDate) ?? "";
                      const days = calendarDaysUntil(selectedComp.nextDueDate);
                      const text =
                        days === null ? fd : `${fd} (${days} d)`;
                      return <span className="whitespace-nowrap">{text}</span>;
                    })()}
                  />
                )}
              </div>
            </div>

            {/* Maintenance history for this component — Tier A shell, Tier B rows */}
            <div className={cn("flex-1 rounded-xl overflow-hidden flex flex-col", CARD_SURFACE_A)}>
              <div className="px-4 py-3 border-b border-zinc-800 flex items-center gap-2">
                <Wrench className="w-3.5 h-3.5 text-zinc-500 shrink-0" aria-hidden />
                <span className="text-xs font-semibold text-zinc-500 uppercase tracking-widest">
                  Maintenance History
                </span>
              </div>
              <div className="flex-1 overflow-y-auto" style={{ minHeight: 0 }}>
                {historyLoading ? (
                  <div className="p-4 space-y-2 animate-pulse">
                    {Array.from({ length: 4 }).map((_, i) => (
                      <div key={i} className={cn("h-12 rounded-lg", CARD_SURFACE_B)} />
                    ))}
                  </div>
                ) : compHistory.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-12 text-zinc-600 text-center px-4">
                    <Wrench className="w-8 h-8 mb-2" />
                    <p className="text-xs">No maintenance records for this component</p>
                  </div>
                ) : (
                  <div className="p-2 space-y-1">
                    {compHistory.map((rec, i) => (
                      <div
                        key={rec.externalId || i}
                        className={cn("p-3 rounded-lg", CARD_SURFACE_B)}
                      >
                        <p className="text-xs text-zinc-200 leading-snug">
                          {rec.description || rec.subtype || rec.type}
                        </p>
                        <div className="flex flex-wrap gap-x-2 gap-y-0.5 mt-1 text-xs text-zinc-600">
                          <span className="whitespace-nowrap">
                            {(() => {
                              const d =
                                formatDate(rec.metadata?.date) ??
                                rec.metadata?.date ??
                                "—";
                              const nd = rec.metadata?.next_due_date?.trim();
                              if (!nd) return d;
                              const days = calendarDaysUntil(nd);
                              if (days === null) return d;
                              return `${d} (${days} d)`;
                            })()}
                          </span>
                          {rec.metadata?.hobbs_at_service && (
                            <span className="font-mono">{rec.metadata.hobbs_at_service} hr</span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </>
        ) : (
          <div className={cn("flex-1 flex flex-col items-center justify-center rounded-xl text-zinc-600 text-center p-8", CARD_SURFACE_A)}>
            <Puzzle className="w-10 h-10 mb-3" />
            <p className="text-sm">Select a component</p>
            <p className="text-xs mt-1">to view maintenance history</p>
          </div>
        )}
        </div>
      </div>
    </div>
  );
}

function DetailRow({
  label,
  value,
  valueTone = "default",
}: {
  label: string;
  value: ReactNode;
  valueTone?: "default" | "warn" | "bad";
}) {
  const valueClass =
    valueTone === "bad"
      ? "text-red-400"
      : valueTone === "warn"
        ? "text-yellow-400"
        : "text-zinc-300";
  return (
    <div className="flex items-start gap-2">
      <span className="text-zinc-600 w-28 shrink-0">{label}</span>
      <span className={cn("font-mono min-w-0 flex-1", valueClass)}>{value}</span>
    </div>
  );
}
