import { useEffect, useState, useMemo } from "react";
import {
  History,
  ChevronLeft,
  ChevronRight,
  AlertTriangle,
  ChevronDown,
  ChevronUp,
  ChevronsUpDown,
} from "lucide-react";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import { useStore, TAILS } from "../lib/store";
import type { FlightRecord } from "../lib/types";

const YEARS = Array.from({ length: 3 }, (_, i) => 2026 - i);

type SortField = "timestamp" | "duration" | "cht_max" | "oil_temp_max" | "egt_max" | "fuel_used_gal";
type SortDir = "asc" | "desc";

interface Props {
  active: boolean;
}

function getValue(rec: FlightRecord, field: SortField): number {
  if (field === "timestamp") return new Date(rec.timestamp).getTime();
  const v = rec[field as keyof FlightRecord];
  if (v === null || v === undefined || typeof v === "string" || typeof v === "boolean") return -Infinity;
  return v as number;
}

export default function FlightHistory({ active }: Props) {
  const { selectedAircraft, setSelectedAircraft } = useStore();
  const tail = selectedAircraft ?? "N4798E";
  const [records, setRecords] = useState<FlightRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [page, setPage] = useState(1);
  const [yearFilter, setYearFilter] = useState<number | undefined>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [sortField, setSortField] = useState<SortField>("timestamp");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  useEffect(() => {
    if (!active) return;
    setLoading(true);
    setError(null);
    api
      .flights(tail, { page, per_page: 25, year: yearFilter })
      .then((res) => {
        setRecords(res.records);
        setTotal(res.total);
        setTotalPages(res.total_pages);
        setExpandedIdx(null);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [active, tail, page, yearFilter]);

  const sortedRecords = useMemo(() => {
    return [...records].sort((a, b) => {
      const av = getValue(a, sortField);
      const bv = getValue(b, sortField);
      return sortDir === "asc" ? av - bv : bv - av;
    });
  }, [records, sortField, sortDir]);

  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("desc");
    }
    setExpandedIdx(null);
  };

  const handleYearChange = (y: string) => {
    setPage(1);
    setYearFilter(y ? Number(y) : undefined);
  };

  return (
    <div className="h-full overflow-y-auto">
    <div className="max-w-5xl mx-auto py-6 px-2 space-y-4">
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

      <div className="flex items-center justify-between gap-4 flex-wrap">
        <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wide flex items-center gap-2">
          <History className="w-4 h-4" />
          Flight History — {tail}
          {total > 0 && (
            <span className="text-zinc-600 normal-case font-normal">
              — {total} flights
            </span>
          )}
        </h2>

        {/* Filters */}
        <div className="flex items-center gap-2 flex-wrap">
          <select
            value={yearFilter ?? ""}
            onChange={(e) => handleYearChange(e.target.value)}
            className="bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-sm text-zinc-300
              focus:outline-none focus:border-sky-600 appearance-none"
          >
            <option value="">All years</option>
            {YEARS.map((y) => (
              <option key={y} value={y}>
                {y}
              </option>
            ))}
          </select>
        </div>
      </div>

      {loading && (
        <div className="space-y-2 animate-pulse">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="h-12 bg-zinc-800 rounded-xl" />
          ))}
        </div>
      )}

      {error && (
        <div className="flex items-center gap-3 p-4 rounded-xl bg-red-950/20 border border-red-800/30">
          <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
          <p className="text-sm text-red-300">{error}</p>
        </div>
      )}

      {!loading && !error && records.length === 0 && (
        <div className="flex flex-col items-center justify-center py-16 text-zinc-600">
          <History className="w-10 h-10 mb-3" />
          <p className="text-sm">No flight records found</p>
          <p className="text-xs mt-1">Run ingestion to populate flight data</p>
        </div>
      )}

      {!loading && !error && records.length > 0 && (
        <div className="bg-zinc-900 rounded-xl border border-zinc-800 overflow-hidden">
          {/* Sortable table header */}
          <div className="grid grid-cols-[auto_1fr_1fr_1fr_1fr_1fr_1fr] gap-3 px-4 py-2.5 border-b border-zinc-800">
            <span className="w-5" />
            <SortHeader label="Date" field="timestamp" current={sortField} dir={sortDir} onSort={handleSort} />
            <SortHeader label="Duration" field="duration" current={sortField} dir={sortDir} onSort={handleSort} />
            <SortHeader label="CHT max" field="cht_max" current={sortField} dir={sortDir} onSort={handleSort} />
            <SortHeader label="Oil temp" field="oil_temp_max" current={sortField} dir={sortDir} onSort={handleSort} />
            <SortHeader label="EGT max" field="egt_max" current={sortField} dir={sortDir} onSort={handleSort} />
            <SortHeader label="Fuel" field="fuel_used_gal" current={sortField} dir={sortDir} onSort={handleSort} />
          </div>

          <div className="divide-y divide-zinc-800/60">
            {sortedRecords.map((rec, idx) => {
              const isExpanded = expandedIdx === idx;
              const date = new Date(rec.timestamp).toLocaleDateString("en-US", {
                month: "short",
                day: "numeric",
                year: "numeric",
              });
              const chtHigh = rec.cht_max !== null && rec.cht_max >= 420;
              const oilHigh = rec.oil_temp_max !== null && rec.oil_temp_max >= 215;

              return (
                <div key={idx}>
                  <button
                    className={cn(
                      "w-full grid grid-cols-[auto_1fr_1fr_1fr_1fr_1fr_1fr] gap-3 px-4 py-3 text-sm text-left",
                      "hover:bg-zinc-800/50 transition-colors",
                      isExpanded && "bg-zinc-800/40"
                    )}
                    onClick={() => setExpandedIdx(isExpanded ? null : idx)}
                  >
                    <span className="flex items-center text-zinc-600">
                      {isExpanded ? (
                        <ChevronUp className="w-3.5 h-3.5" />
                      ) : (
                        <ChevronDown className="w-3.5 h-3.5" />
                      )}
                    </span>
                    <span className="text-zinc-300">{date}</span>
                    <span className="text-zinc-400 font-mono text-xs">
                      {rec.duration.toFixed(1)} hr
                      <span className="text-zinc-600 ml-1">
                        ({rec.hobbs_start.toFixed(1)}→{rec.hobbs_end.toFixed(1)})
                      </span>
                    </span>
                    <span className={cn("font-mono text-xs", chtHigh ? "text-yellow-400" : "text-zinc-400")}>
                      {rec.cht_max !== null ? `${rec.cht_max}°F` : "—"}
                    </span>
                    <span className={cn("font-mono text-xs", oilHigh ? "text-yellow-400" : "text-zinc-400")}>
                      {rec.oil_temp_max !== null ? `${rec.oil_temp_max}°F` : "—"}
                    </span>
                    <span className="text-zinc-400 font-mono text-xs">
                      {rec.egt_max !== null ? `${rec.egt_max}°F` : "—"}
                    </span>
                    <span className="text-zinc-400 font-mono text-xs">
                      {rec.fuel_used_gal !== null ? `${rec.fuel_used_gal.toFixed(1)} gal` : "—"}
                    </span>
                  </button>

                  {isExpanded && (
                    <div className="px-4 pb-4 bg-zinc-800/20 border-t border-zinc-800/50">
                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 py-3 text-xs">
                        <Param label="Oil pressure min" value={rec.oil_pressure_min !== null ? `${rec.oil_pressure_min} psi` : "—"} />
                        <Param label="Oil pressure max" value={rec.oil_pressure_max !== null ? `${rec.oil_pressure_max} psi` : "—"} />
                        <Param label="EGT max" value={rec.egt_max !== null ? `${rec.egt_max}°F` : "—"} />
                        <Param label="Duration" value={`${rec.duration.toFixed(1)} hr`} />
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-xs text-zinc-600">
            Page {page} of {totalPages} · {total} total flights
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
    </div>
    </div>
  );
}

function SortHeader({
  label,
  field,
  current,
  dir,
  onSort,
}: {
  label: string;
  field: SortField;
  current: SortField;
  dir: SortDir;
  onSort: (f: SortField) => void;
}) {
  const active = field === current;
  return (
    <button
      onClick={() => onSort(field)}
      className={cn(
        "flex items-center gap-1 text-xs font-medium uppercase tracking-wide text-left transition-colors",
        active ? "text-sky-400" : "text-zinc-500 hover:text-zinc-300"
      )}
    >
      {label}
      <span className="shrink-0">
        {active ? (
          dir === "desc" ? (
            <ChevronDown className="w-3 h-3" />
          ) : (
            <ChevronUp className="w-3 h-3" />
          )
        ) : (
          <ChevronsUpDown className="w-3 h-3 opacity-40" />
        )}
      </span>
    </button>
  );
}

function Param({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-zinc-600">{label}</p>
      <p className="text-zinc-300 font-mono">{value}</p>
    </div>
  );
}
