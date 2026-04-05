import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle, Info, XCircle, Plane } from "lucide-react";
import { api } from "../lib/api";
import { useStore, TAILS, type TailNumber } from "../lib/store";
import type { FleetAircraft } from "../lib/types";
import { cn } from "../lib/utils";

type Tab = "fleet" | "dashboard" | "query" | "maintenance" | "flights" | "aircraft" | "graph";

interface FleetPageProps {
  onNavigate: (tab: Tab, tail: TailNumber) => void;
}

const AIRWORTHINESS_CONFIG = {
  AIRWORTHY: {
    label: "AIRWORTHY",
    icon: CheckCircle,
    dot: "bg-emerald-400",
    badge: "text-emerald-400 bg-emerald-950/40 border-emerald-800/50",
    card: "border-emerald-800/30",
  },
  FERRY_ONLY: {
    label: "FERRY ONLY",
    icon: Info,
    dot: "bg-yellow-400",
    badge: "text-yellow-400 bg-yellow-950/40 border-yellow-800/50",
    card: "border-yellow-800/30",
  },
  CAUTION: {
    label: "CAUTION",
    icon: AlertTriangle,
    dot: "bg-orange-400",
    badge: "text-orange-400 bg-orange-950/40 border-orange-800/50",
    card: "border-orange-800/30",
  },
  NOT_AIRWORTHY: {
    label: "NOT AIRWORTHY",
    icon: XCircle,
    dot: "bg-red-500",
    badge: "text-red-400 bg-red-950/40 border-red-800/50",
    card: "border-red-800/30",
  },
  UNKNOWN: {
    label: "UNKNOWN",
    icon: Info,
    dot: "bg-zinc-400",
    badge: "text-zinc-400 bg-zinc-800/40 border-zinc-700",
    card: "border-zinc-700",
  },
};

function AircraftCard({
  aircraft,
  onSelect,
}: {
  aircraft: FleetAircraft;
  onSelect: () => void;
}) {
  const cfg = AIRWORTHINESS_CONFIG[aircraft.airworthiness] ?? AIRWORTHINESS_CONFIG.UNKNOWN;
  const smoh = Number(aircraft.smoh) || 0;
  const smohPct = Math.min(100, Number(aircraft.smohPercent) || 0);
  const hobbs = Number(aircraft.hobbs) || 0;
  const oilOver = Number(aircraft.oilHoursOverdue) || 0;
  const loadErr = aircraft.metadata?.load_error;

  return (
    <button
      onClick={onSelect}
      className={cn(
        "text-left rounded-xl border bg-zinc-900/60 p-5 transition-all hover:bg-zinc-800/60 hover:border-zinc-600 group",
        cfg.card
      )}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-3 mb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-sky-500/10 rounded-lg border border-sky-500/20">
            <Plane className="w-4 h-4 text-sky-400" />
          </div>
          <div>
            <div className="font-bold text-base text-zinc-100 tracking-wide">{aircraft.tail}</div>
            <div className="text-xs text-zinc-500 mt-0.5">1978 Cessna 172N · KPHX</div>
          </div>
        </div>
        <span className={cn("flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border", cfg.badge)}>
          <span className={cn("w-1.5 h-1.5 rounded-full", cfg.dot)} />
          {cfg.label}
        </span>
      </div>

      {/* SMOH bar */}
      <div className="mb-4">
        <div className="flex justify-between text-xs mb-1.5">
          <span className="text-zinc-400">Engine SMOH</span>
          <span className="text-zinc-300 font-medium">{smoh.toFixed(0)} / {aircraft.tbo} hrs ({smohPct.toFixed(0)}%)</span>
        </div>
        <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
          <div
            className={cn(
              "h-full rounded-full transition-all",
              smohPct > 80 ? "bg-red-500" : smohPct > 60 ? "bg-yellow-500" : "bg-emerald-500"
            )}
            style={{ width: `${smohPct}%` }}
          />
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="bg-zinc-800/50 rounded-lg px-3 py-2">
          <div className="text-zinc-500 mb-0.5">Hobbs</div>
          <div className="text-zinc-200 font-medium">{hobbs.toFixed(1)} hrs</div>
        </div>
        <div className="bg-zinc-800/50 rounded-lg px-3 py-2">
          <div className="text-zinc-500 mb-0.5">Open Squawks</div>
          <div className={cn("font-medium", aircraft.groundingSquawkCount > 0 ? "text-red-400" : aircraft.openSquawkCount > 0 ? "text-yellow-400" : "text-emerald-400")}>
            {aircraft.openSquawkCount} {aircraft.groundingSquawkCount > 0 && `(${aircraft.groundingSquawkCount} grounding)`}
          </div>
        </div>
        <div className="bg-zinc-800/50 rounded-lg px-3 py-2">
          <div className="text-zinc-500 mb-0.5">Annual Due</div>
          <div className={cn("font-medium", aircraft.annualDaysRemaining !== null && aircraft.annualDaysRemaining < 0 ? "text-red-400" : aircraft.annualDaysRemaining !== null && aircraft.annualDaysRemaining < 30 ? "text-yellow-400" : "text-zinc-200")}>
            {aircraft.annualDueDate || "—"}
          </div>
        </div>
        <div className="bg-zinc-800/50 rounded-lg px-3 py-2">
          <div className="text-zinc-500 mb-0.5">Oil Overdue</div>
          <div className={cn("font-medium", oilOver > 0 ? "text-red-400" : "text-emerald-400")}>
            {oilOver > 0 ? `${oilOver.toFixed(1)} hrs` : "Current"}
          </div>
        </div>
      </div>

      {/* Symptoms/conditions if any */}
      {(aircraft.activeSymptoms > 0 || aircraft.activeConditions > 0) && (
        <div className="mt-3 flex gap-2 text-xs">
          {aircraft.activeSymptoms > 0 && (
            <span className="px-2 py-0.5 bg-orange-950/40 text-orange-400 border border-orange-800/40 rounded-full">
              {aircraft.activeSymptoms} symptom{aircraft.activeSymptoms !== 1 ? "s" : ""}
            </span>
          )}
          {aircraft.activeConditions > 0 && (
            <span className="px-2 py-0.5 bg-red-950/40 text-red-400 border border-red-800/40 rounded-full">
              {aircraft.activeConditions} condition{aircraft.activeConditions !== 1 ? "s" : ""}
            </span>
          )}
        </div>
      )}

      {loadErr && (
        <div className="mt-3 text-xs text-red-400/90 border border-red-900/50 rounded-lg px-2 py-1.5 bg-red-950/20">
          Could not load status: {loadErr}
        </div>
      )}

      <div className="mt-3 text-xs text-zinc-600 group-hover:text-zinc-500 transition-colors">
        View aircraft status →
      </div>
    </button>
  );
}

export default function FleetPage({ onNavigate }: FleetPageProps) {
  const [fleet, setFleet] = useState<FleetAircraft[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { setSelectedAircraft } = useStore();

  useEffect(() => {
    setLoading(true);
    api.fleet()
      .then(setFleet)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const handleSelect = (tail: TailNumber) => {
    setSelectedAircraft(tail);
    onNavigate("dashboard", tail);
  };

  const airworthyCounts = {
    AIRWORTHY: fleet.filter((a) => a.airworthiness === "AIRWORTHY").length,
    FERRY_ONLY: fleet.filter((a) => a.airworthiness === "FERRY_ONLY").length,
    CAUTION: fleet.filter((a) => a.airworthiness === "CAUTION").length,
    NOT_AIRWORTHY: fleet.filter((a) => a.airworthiness === "NOT_AIRWORTHY").length,
  };

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-4xl mx-auto py-6 px-2">
        {/* Fleet header stats */}
        {!loading && fleet.length > 0 && (
          <div className="grid grid-cols-4 gap-3 mb-6">
            {[
              { label: "Airworthy", count: airworthyCounts.AIRWORTHY, color: "text-emerald-400" },
              { label: "Ferry Only", count: airworthyCounts.FERRY_ONLY, color: "text-yellow-400" },
              { label: "Caution", count: airworthyCounts.CAUTION, color: "text-orange-400" },
              { label: "Grounded", count: airworthyCounts.NOT_AIRWORTHY, color: "text-red-400" },
            ].map((s) => (
              <div key={s.label} className="bg-zinc-900/60 border border-zinc-800 rounded-xl p-4 text-center">
                <div className={cn("text-2xl font-bold", s.color)}>{s.count}</div>
                <div className="text-xs text-zinc-500 mt-1">{s.label}</div>
              </div>
            ))}
          </div>
        )}

        {/* Loading */}
        {loading && (
          <div className="grid grid-cols-2 gap-4">
            {TAILS.map((t) => (
              <div key={t} className="h-52 rounded-xl border border-zinc-800 bg-zinc-900/40 animate-pulse" />
            ))}
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-red-950/30 border border-red-800/50 rounded-xl p-4 text-red-400 text-sm">
            Failed to load fleet data: {error}
          </div>
        )}

        {/* Aircraft cards — 2×2 grid */}
        {!loading && !error && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {fleet.map((aircraft) => (
              <AircraftCard
                key={aircraft.tail}
                aircraft={aircraft}
                onSelect={() => handleSelect(aircraft.tail as TailNumber)}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
