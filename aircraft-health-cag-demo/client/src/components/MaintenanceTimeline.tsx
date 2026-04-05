import { useEffect, useState } from "react";
import {
  Clock,
  Wrench,
  CheckCircle,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { cn, formatDate, formatTimestamp, urgencyColor } from "../lib/utils";
import { api } from "../lib/api";
import { useStore, TAILS } from "../lib/store";
import type { MaintenanceItem, MaintenanceRecord } from "../lib/types";

const COMPONENT_OPTIONS = [
  { value: "", label: "All components" },
  { value: "ENGINE", label: "Engine" },
  { value: "PROPELLER", label: "Propeller" },
  { value: "AIRFRAME", label: "Airframe" },
  { value: "AVIONICS", label: "Avionics" },
];

const YEARS = [
  { value: "", label: "All years" },
  ...Array.from({ length: 5 }, (_, i) => {
    const y = 2026 - i;
    return { value: String(y), label: String(y) };
  }),
];

interface Props {
  active: boolean;
}

export default function MaintenanceTimeline({ active }: Props) {
  const { selectedAircraft, setSelectedAircraft } = useStore();
  const tail = selectedAircraft ?? "N4798E";
  const [upcoming, setUpcoming] = useState<MaintenanceItem[]>([]);
  const [history, setHistory] = useState<MaintenanceRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [page, setPage] = useState(1);
  const [componentFilter, setComponentFilter] = useState("");
  const [yearFilter, setYearFilter] = useState("");
  const [loading, setLoading] = useState(false);
  const [upcomingLoading, setUpcomingLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Upcoming maintenance — refetch on tail change
  useEffect(() => {
    setUpcomingLoading(true);
    api
      .upcomingMaintenance(tail)
      .then(setUpcoming)
      .catch(() => setUpcoming([]))
      .finally(() => setUpcomingLoading(false));
  }, [tail]);

  // History — refetch on tail, page, or filter change
  useEffect(() => {
    if (!active) return;
    setLoading(true);
    setError(null);
    api
      .maintenanceHistory(tail, {
        page,
        per_page: 25,
        component: componentFilter || undefined,
        year: yearFilter ? Number(yearFilter) : undefined,
      })
      .then((res) => {
        setHistory(res.records);
        setTotal(res.total);
        setTotalPages(res.total_pages);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [active, tail, page, componentFilter, yearFilter]);

  const handleFilterChange = () => {
    setPage(1);
  };

  return (
    <div className="h-full overflow-y-auto">
    <div className="max-w-4xl mx-auto py-6 px-2 space-y-6">
      {/* Aircraft selector */}
      <div className="flex items-center gap-2">
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
      {/* Upcoming maintenance */}
      <section>
        <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wide mb-3 flex items-center gap-2">
          <Clock className="w-4 h-4" />
          Upcoming &amp; Overdue Maintenance
        </h2>

        {upcomingLoading ? (
          <div className="space-y-2 animate-pulse">
            {Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="h-16 bg-zinc-800 rounded-xl" />
            ))}
          </div>
        ) : upcoming.length === 0 ? (
          <div className="flex items-center gap-3 p-4 rounded-xl bg-emerald-950/20 border border-emerald-800/30 text-emerald-400">
            <CheckCircle className="w-5 h-5 shrink-0" />
            <p className="text-sm">No maintenance due in the next 250 hobbs hours</p>
          </div>
        ) : (
          <div className="space-y-2">
            {upcoming.map((item, idx) => {
              const hoursColor = item.isOverdue
                ? "text-red-400"
                : urgencyColor(item.hoursUntilDue, "hours");
              const urgencyBg = item.isOverdue
                ? "bg-red-950/20 border-red-700/40"
                : item.hoursUntilDue <= 50
                ? "bg-yellow-950/20 border-yellow-800/30"
                : "bg-zinc-900 border-zinc-800";

              return (
                <div
                  key={idx}
                  className={cn("flex items-center gap-4 p-4 rounded-xl border", urgencyBg)}
                >
                  <div className="shrink-0 text-center w-20">
                    {item.isOverdue ? (
                      <>
                        <p className={cn("text-lg font-bold tabular-nums", hoursColor)}>
                          {Math.abs(item.hoursUntilDue).toFixed(0)}hr
                        </p>
                        <p className="text-xs text-red-600 font-semibold">OVERDUE</p>
                      </>
                    ) : (
                      <>
                        <p className={cn("text-xl font-bold tabular-nums", hoursColor)}>
                          {item.hoursUntilDue.toFixed(1)}
                        </p>
                        <p className="text-xs text-zinc-600">hrs</p>
                      </>
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-zinc-200 leading-snug">
                      {item.description}
                    </p>
                    <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1">
                      <span className="text-xs text-zinc-600">{item.component}</span>
                      <span className="text-xs text-zinc-600">
                        Due @ {item.nextDueHobbs.toFixed(1)} hr
                      </span>
                      {item.nextDueDate && (
                        <span className="text-xs text-zinc-600">
                          or {formatDate(item.nextDueDate)}
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="shrink-0">
                    <span
                      className={cn(
                        "text-xs px-2 py-1 rounded-full border font-medium",
                        item.isOverdue
                          ? "text-red-400 bg-red-950/40 border-red-800/50"
                          : item.hoursUntilDue <= 50
                          ? "text-yellow-400 bg-yellow-950/40 border-yellow-800/50"
                          : "text-zinc-400 bg-zinc-800 border-zinc-700"
                      )}
                    >
                      {item.maintenanceType}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* Full maintenance history */}
      <section>
        <div className="flex items-center justify-between gap-4 mb-3 flex-wrap">
          <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wide flex items-center gap-2">
            <Wrench className="w-4 h-4" />
            Maintenance History
            {total > 0 && (
              <span className="text-zinc-600 normal-case font-normal">— {total} records</span>
            )}
          </h2>

          {/* Filters */}
          <div className="flex items-center gap-2 flex-wrap">
            <select
              value={componentFilter}
              onChange={(e) => {
                setComponentFilter(e.target.value);
                handleFilterChange();
              }}
              className="bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-sm text-zinc-300
                focus:outline-none focus:border-sky-600"
            >
              {COMPONENT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
            <select
              value={yearFilter}
              onChange={(e) => {
                setYearFilter(e.target.value);
                handleFilterChange();
              }}
              className="bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-sm text-zinc-300
                focus:outline-none focus:border-sky-600"
            >
              {YEARS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {loading ? (
          <div className="space-y-2 animate-pulse">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="h-16 bg-zinc-800 rounded-xl" />
            ))}
          </div>
        ) : error ? (
          <div className="flex items-center gap-3 p-4 rounded-xl bg-red-950/20 border border-red-800/30">
            <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
            <p className="text-sm text-red-300">{error}</p>
          </div>
        ) : history.length === 0 ? (
          <div className="p-8 rounded-xl bg-zinc-900 border border-zinc-800 text-zinc-500 text-sm text-center">
            <Wrench className="w-8 h-8 mx-auto mb-2 opacity-30" />
            No maintenance records found. Run ingestion first.
          </div>
        ) : (
          <>
            <div className="relative">
              <div className="absolute left-5 top-2 bottom-2 w-px bg-zinc-800" />
              <div className="space-y-1">
                {history.map((record, idx) => {
                  const isInspection = record.type === "Inspection";
                  const isSquawk = record.type === "Squawk";

                  return (
                    <div key={record.externalId || idx} className="flex gap-4 relative pl-2">
                      <div
                        className={cn(
                          "shrink-0 w-6 h-6 rounded-full border-2 flex items-center justify-center z-10 mt-2",
                          isInspection
                            ? "bg-sky-950 border-sky-700"
                            : isSquawk
                            ? "bg-yellow-950 border-yellow-700"
                            : "bg-zinc-900 border-zinc-700"
                        )}
                      >
                        <span
                          className={cn(
                            "w-2 h-2 rounded-full",
                            isInspection
                              ? "bg-sky-500"
                              : isSquawk
                              ? "bg-yellow-500"
                              : "bg-zinc-600"
                          )}
                        />
                      </div>

                      <div className="flex-1 bg-zinc-900 rounded-xl border border-zinc-800 p-3 mb-2">
                        <div className="flex items-start justify-between gap-2">
                          <p className="text-sm text-zinc-200 leading-snug flex-1">
                            {record.description || record.subtype || record.type}
                          </p>
                          <span className="shrink-0 text-xs text-zinc-600 font-mono">
                            {record.metadata?.hobbs_at_service
                              ? `${record.metadata.hobbs_at_service} hr`
                              : formatTimestamp(record.startTime ?? null)}
                          </span>
                        </div>
                        <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1.5">
                          <span className="text-xs text-zinc-600">
                            {formatDate(record.metadata?.date) ||
                              formatTimestamp(record.startTime ?? null)}
                          </span>
                          {record.metadata?.component_id && (
                            <span className="text-xs text-zinc-700 font-mono">
                              {record.metadata.component_id}
                            </span>
                          )}
                          {record.metadata?.mechanic && (
                            <span className="text-xs text-zinc-700 truncate max-w-48">
                              {record.metadata.mechanic.split("—")[0]?.trim()}
                            </span>
                          )}
                          {record.metadata?.ad_reference && (
                            <span className="text-xs text-violet-600 font-mono">
                              AD {record.metadata.ad_reference}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between pt-4">
                <p className="text-xs text-zinc-600">
                  Page {page} of {totalPages} · {total} total records
                </p>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    disabled={page === 1}
                    className="p-1.5 rounded-lg border border-zinc-700 text-zinc-400 hover:text-zinc-200
                      hover:border-zinc-600 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                  >
                    <ChevronLeft className="w-4 h-4" />
                  </button>
                  {/* Page number pills */}
                  <div className="flex gap-1">
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      const p =
                        totalPages <= 5
                          ? i + 1
                          : page <= 3
                          ? i + 1
                          : page >= totalPages - 2
                          ? totalPages - 4 + i
                          : page - 2 + i;
                      return (
                        <button
                          key={p}
                          onClick={() => setPage(p)}
                          className={cn(
                            "w-7 h-7 rounded-lg text-xs font-medium transition-colors",
                            p === page
                              ? "bg-sky-600 text-white"
                              : "text-zinc-500 hover:text-zinc-200 hover:bg-zinc-800"
                          )}
                        >
                          {p}
                        </button>
                      );
                    })}
                  </div>
                  <button
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                    disabled={page === totalPages}
                    className="p-1.5 rounded-lg border border-zinc-700 text-zinc-400 hover:text-zinc-200
                      hover:border-zinc-600 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                  >
                    <ChevronRight className="w-4 h-4" />
                  </button>
                </div>
              </div>
            )}
          </>
        )}
      </section>
    </div>
    </div>
  );
}
