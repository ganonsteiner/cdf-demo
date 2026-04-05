import { useEffect, useState } from "react";
import { Plane, ChevronRight, CheckCircle, AlertTriangle, XCircle, Wrench } from "lucide-react";
import { cn, formatDate } from "../lib/utils";
import { api } from "../lib/api";
import { useStore, TAILS } from "../lib/store";
import type { ComponentNode, MaintenanceRecord } from "../lib/types";

interface Props {
  active: boolean;
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

function statusIcon(status: ComponentNode["status"]) {
  if (status === "overdue")
    return <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0" />;
  if (status === "due_soon")
    return <AlertTriangle className="w-3.5 h-3.5 text-yellow-400 shrink-0" />;
  return <CheckCircle className="w-3.5 h-3.5 text-emerald-400 shrink-0" />;
}

function statusDot(status: ComponentNode["status"]) {
  if (status === "overdue") return "bg-red-500";
  if (status === "due_soon") return "bg-yellow-400";
  return "bg-emerald-500";
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

  useEffect(() => {
    if (!active) return;
    setLoading(true);
    setError(null);
    setSelectedId(null);
    api
      .components(tail)
      .then(setComponents)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [active, tail]);

  useEffect(() => {
    if (!selectedId) {
      setCompHistory([]);
      return;
    }
    setHistoryLoading(true);
    api
      .maintenanceHistory(tail, { component: selectedId, per_page: 50 })
      .then((res) => setCompHistory(res.records))
      .catch(() => setCompHistory([]))
      .finally(() => setHistoryLoading(false));
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
                ? "bg-sky-950/50 border border-sky-800/50"
                : "hover:bg-zinc-800/60 border border-transparent"
            )}
          >
            <span className={cn("w-2 h-2 rounded-full shrink-0", statusDot(node.status))} />
            <span className="flex-1 min-w-0">
              <span className="text-zinc-200 font-medium truncate block">{node.name}</span>
              <span className="text-xs text-zinc-600 font-mono truncate block">{node.externalId}</span>
            </span>
            <div className="flex items-center gap-2 shrink-0">
              {node.maintenanceCount > 0 && (
                <span className="text-xs text-zinc-600">{node.maintenanceCount}</span>
              )}
              {statusIcon(node.status)}
              {hasChildren && (
                <ChevronRight className="w-3.5 h-3.5 text-zinc-600 group-hover:text-zinc-400" />
              )}
            </div>
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

  if (loading)
    return (
      <div className="space-y-2 animate-pulse">
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="h-12 bg-zinc-800 rounded-xl" style={{ marginLeft: `${(i % 4) * 16}px` }} />
        ))}
      </div>
    );

  if (error)
    return (
      <div className="flex items-center gap-3 p-4 rounded-xl bg-red-950/20 border border-red-800/30">
        <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
        <p className="text-sm text-red-300">{error}</p>
      </div>
    );

  const selectedComp = components.find((c) => c.externalId === selectedId);

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Aircraft selector */}
      <div className="flex items-center gap-2 px-4 py-3 border-b border-zinc-800 shrink-0">
        <span className="text-xs text-zinc-500">Aircraft:</span>
        <div className="flex gap-1">
          {TAILS.map((t) => (
            <button key={t} onClick={() => setSelectedAircraft(t)}
              className={`px-2.5 py-0.5 rounded-full text-xs font-medium border transition-colors ${
                t === tail ? "bg-sky-600 text-white border-sky-500" : "bg-zinc-800 text-zinc-400 border-zinc-700 hover:border-zinc-500"
              }`}>{t}</button>
          ))}
        </div>
      </div>

    <div className="flex-1 flex gap-4 overflow-hidden p-4">
      {/* Component tree */}
      <div className="flex-1 min-w-0 bg-zinc-900 rounded-xl border border-zinc-800 overflow-y-auto">
        <div className="px-4 py-3 border-b border-zinc-800 flex items-center gap-2">
          <Plane className="w-4 h-4 text-sky-400" />
          <span className="text-sm font-semibold text-zinc-300">{tail} — Component Hierarchy</span>
          <span className="text-xs text-zinc-600 ml-auto">{components.length} nodes</span>
        </div>

        {/* Legend */}
        <div className="px-4 py-2 border-b border-zinc-800/50 flex gap-4">
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

        <div className="p-2 space-y-0.5">{renderTree(null, 0)}</div>
      </div>

      {/* Side panel — component detail */}
      <div className="w-80 shrink-0 flex flex-col gap-4">
        {selectedComp ? (
          <>
            {/* Component card */}
            <div className="bg-zinc-900 rounded-xl border border-zinc-800 p-4">
              <div className="flex items-start gap-2 mb-3">
                <span className={cn("w-2.5 h-2.5 rounded-full mt-1 shrink-0", statusDot(selectedComp.status))} />
                <div className="min-w-0">
                  <p className="font-semibold text-zinc-100 text-sm leading-tight">
                    {selectedComp.name}
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
                  label="Last maintenance"
                  value={formatDate(selectedComp.lastMaintenanceDate) ?? "No records"}
                />
                {selectedComp.nextDueHobbs !== null && (
                  <DetailRow
                    label="Next due"
                    value={`${selectedComp.nextDueHobbs.toFixed(1)} hr${selectedComp.hoursUntilDue !== null ? ` (${selectedComp.hoursUntilDue > 0 ? "+" : ""}${selectedComp.hoursUntilDue?.toFixed(1)} hr)` : ""}`}
                    highlight={selectedComp.status !== "ok"}
                  />
                )}
                {selectedComp.nextDueDate && (
                  <DetailRow
                    label="Due date"
                    value={formatDate(selectedComp.nextDueDate) ?? ""}
                  />
                )}
              </div>
            </div>

            {/* Maintenance history for this component */}
            <div className="flex-1 bg-zinc-900 rounded-xl border border-zinc-800 overflow-hidden flex flex-col">
              <div className="px-4 py-3 border-b border-zinc-800 flex items-center gap-2">
                <Wrench className="w-3.5 h-3.5 text-zinc-500" />
                <span className="text-sm font-medium text-zinc-400">Maintenance History</span>
              </div>
              <div className="flex-1 overflow-y-auto" style={{ minHeight: 0 }}>
                {historyLoading ? (
                  <div className="p-4 space-y-2 animate-pulse">
                    {Array.from({ length: 5 }).map((_, i) => (
                      <div key={i} className="h-12 bg-zinc-800 rounded-lg" />
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
                        className="p-3 rounded-lg bg-zinc-800/40 border border-zinc-800/60"
                      >
                        <p className="text-xs text-zinc-200 leading-snug">
                          {rec.description || rec.subtype || rec.type}
                        </p>
                        <div className="flex gap-2 mt-1 text-xs text-zinc-600">
                          <span>{rec.metadata?.date || "—"}</span>
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
          <div className="flex-1 flex flex-col items-center justify-center bg-zinc-900 rounded-xl border border-zinc-800 text-zinc-600 text-center p-8">
            <Plane className="w-10 h-10 mb-3" />
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
  highlight = false,
}: {
  label: string;
  value: string;
  highlight?: boolean;
}) {
  return (
    <div className="flex items-start gap-2">
      <span className="text-zinc-600 w-28 shrink-0">{label}</span>
      <span className={cn("font-mono", highlight ? "text-yellow-400" : "text-zinc-300")}>
        {value}
      </span>
    </div>
  );
}
