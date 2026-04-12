import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle, Info, XCircle, Plane, ArrowRight } from "lucide-react";
import { api } from "../lib/api";
import { useStore, TAILS, type TailNumber } from "../lib/store";
import type { FleetAircraft } from "../lib/types";
import {
  cn,
  CARD_SURFACE_A,
  CARD_SURFACE_B,
  CARD_SURFACE_C,
  formatSignedOilHoursCompact,
  TAB_PAGE_TOP_INSET,
  toneClasses,
  toneForAirworthiness,
  toneForDue,
  toneForOilLife,
  toneForSquawks,
} from "../lib/utils";

type Tab = "fleet" | "dashboard" | "query" | "maintenance" | "flights" | "aircraft" | "graph";

/** Mirrors StatusDashboard assistant footer inset: `left-2 bottom-2` there → `right-2 bottom-2` here (do not change Status). */
const FLEET_CARD_STATUS_FOOTER_ROW =
  "pointer-events-none absolute right-2 bottom-2 z-10 inline-flex shrink-0 items-center gap-0.5 text-sm leading-none text-zinc-500 transition-colors group-hover/card:text-sky-400";

interface FleetPageProps {
  onNavigate: (tab: Tab, tail: TailNumber) => void;
}

const AIRWORTHINESS_CONFIG = {
  AIRWORTHY: {
    label: "AIRWORTHY",
    icon: CheckCircle,
    dot: "bg-emerald-400",
    badge: toneClasses("ok").badge,
    card: "border-emerald-800/30",
  },
  FERRY_ONLY: {
    label: "FERRY ONLY",
    icon: Info,
    dot: "bg-yellow-400",
    badge: toneClasses("warn").badge,
    card: "border-yellow-800/30",
  },
  CAUTION: {
    label: "CAUTION",
    icon: AlertTriangle,
    dot: "bg-yellow-400",
    badge: toneClasses("warn").badge,
    card: "border-yellow-800/30",
  },
  NOT_AIRWORTHY: {
    label: "NOT AIRWORTHY",
    icon: XCircle,
    dot: "bg-red-500",
    badge: toneClasses("bad").badge,
    card: "border-red-800/30",
  },
  UNKNOWN: {
    label: "UNKNOWN",
    icon: Info,
    dot: "bg-zinc-400",
    badge: toneClasses("unknown").badge,
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
  const awTone = toneForAirworthiness(aircraft.airworthiness);
  const smoh = Number(aircraft.smoh) || 0;
  const smohPct = Math.min(100, Number(aircraft.smohPercent) || 0);
  const hobbs = Number(aircraft.hobbs) || 0;
  const tach = Number(aircraft.tach) || 0;
  const oilOver =
    Number(aircraft.oilTachHoursOverdue ?? aircraft.oilHoursOverdue) || 0;
  const rawUntil = Number(aircraft.oilTachHoursUntilDue);
  const oilUntil = Number.isFinite(rawUntil) ? rawUntil : 0;
  const oilDays = aircraft.oilDaysUntilDue;
  const loadErr = aircraft.metadata?.load_error;

  /** Signed tach hours until oil due (negative = overdue). Prefer overdue magnitude when both are present. */
  const signedOilHours = oilOver > 0 ? -oilOver : oilUntil;
  const hasOilHorizon = oilOver > 0 || oilUntil !== 0 || oilDays !== null;

  const formatSignedDays = (d: number | null) => {
    if (d === null) return "— d";
    const sign = d < 0 ? "-" : "";
    return `${sign}${Math.abs(d)} d`;
  };

  const oilLine = hasOilHorizon
    ? `${formatSignedOilHoursCompact(signedOilHours, " ")} / ${formatSignedDays(oilDays)}`
    : "— hr / — d";

  const oilTone = !hasOilHorizon
    ? toneForAirworthiness("UNKNOWN")
    : toneForOilLife({
        oilHoursOverdue: oilOver,
        oilTachHoursUntilDue: signedOilHours,
        oilDaysUntilDue: oilDays,
      });

  const annualTone = toneForDue(aircraft.annualDaysRemaining, "days");
  const squawkTone = toneForSquawks(aircraft.openSquawkCount, aircraft.groundingSquawkCount);
  const symptomTone = toneClasses(aircraft.activeSymptoms > 0 ? "warn" : "ok");
  const nbsp = "\u00a0";

  return (
    <button
      onClick={onSelect}
      className={cn(
        "relative text-left rounded-xl p-5 sm:p-6 pb-9 transition-all hover:bg-zinc-900/30 hover:border-zinc-700 group/card",
        CARD_SURFACE_A,
        "flex flex-col w-full min-w-0 overflow-hidden"
      )}
    >
      <div className="flex flex-col min-w-0">
      {/* Header */}
      <div className="flex items-start justify-between gap-3 mb-3 shrink-0 min-w-0">
        <div className="flex min-w-0 items-center gap-3">
          <div className="p-2 bg-sky-500/10 rounded-lg border border-sky-500/20 shrink-0">
            <Plane className="w-4 h-4 text-sky-400" />
          </div>
          <div className="min-w-0">
            <div className="font-bold text-base text-zinc-100 tracking-wide">{aircraft.tail}</div>
            <div className="text-xs text-zinc-400 mt-0.5">1978 Cessna 172N · KPHX</div>
          </div>
        </div>
        <span
          className={cn(
            "inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap px-2.5 py-1 rounded-full text-xs font-semibold border",
            awTone.badge
          )}
        >
          <span className={cn("w-1.5 h-1.5 shrink-0 rounded-full", awTone.dot)} />
          {cfg.label}
        </span>
      </div>

      {/* SMOH bar */}
      <div className="mb-3 shrink-0">
        <div className="flex justify-between items-baseline gap-2 text-sm mb-1.5">
          <span className="text-zinc-400 font-medium">Engine SMOH</span>
          <span className="tabular-nums shrink-0 text-sm">
            <span className="text-zinc-300 font-medium">
              {smoh.toFixed(0)} / {aircraft.tbo} hr
            </span>
            <span className="text-zinc-500 font-normal">
              {"\u00a0"}·{"\u00a0"}
              {smohPct.toFixed(0)}%
            </span>
          </span>
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

      {/* Stats grid — mb-3 separates tiles from absolute “View aircraft status” (pt on footer does not). */}
      <div className="grid grid-cols-2 gap-3 shrink-0 mb-3">
        <div className={cn("rounded-lg px-3 py-3", CARD_SURFACE_C)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Aircraft Time (Hobbs)</div>
          <div className="text-sm font-medium tabular-nums text-zinc-200 whitespace-nowrap">
            {hobbs.toFixed(1)}
            {"\u00a0"}
            hr
          </div>
        </div>
        <div className={cn("rounded-lg px-3 py-3", CARD_SURFACE_C)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Engine Time (Tach)</div>
          <div className="text-sm font-medium tabular-nums text-zinc-200 whitespace-nowrap">
            {tach.toFixed(1)}
            {"\u00a0"}
            hr
          </div>
        </div>

        {/* Annual (top-left under time cards) */}
        <div className={cn("rounded-lg px-3 py-3", annualTone.panel)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Annual Due</div>
          <div
            className={cn(
              "text-sm font-medium",
              annualTone.tone === "unknown" ? "text-zinc-200" : annualTone.text
            )}
          >
            {aircraft.annualDueDate || "—"}
          </div>
        </div>

        {/* Oil (top-right under time cards) */}
        <div className={cn("rounded-lg px-3 py-3", oilTone.panel)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Oil Life</div>
          <div
            className={cn(
              "text-sm font-medium leading-snug",
              !hasOilHorizon ? "text-zinc-200" : oilTone.text
            )}
          >
            {oilLine}
          </div>
        </div>

        {/* Squawks (bottom-left) */}
        <div className={cn("rounded-lg px-3 py-3", squawkTone.panel)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Squawks</div>
          <div className={cn("text-sm font-medium tabular-nums", squawkTone.text)}>
            {`${aircraft.openSquawkCount}${nbsp}open`}
            {aircraft.groundingSquawkCount > 0 && (
              <>
                {" "}
                ({aircraft.groundingSquawkCount} grounding)
              </>
            )}
          </div>
        </div>

        {/* Symptoms (bottom-right) */}
        <div className={cn("rounded-lg px-3 py-3", symptomTone.panel)}>
          <div className="text-xs font-medium text-zinc-400 mb-0.5">Symptoms</div>
          <div className={cn("text-sm font-medium tabular-nums", symptomTone.text)}>
            {`${aircraft.activeSymptoms}${nbsp}reported`}
          </div>
        </div>
      </div>

      {loadErr && (
        <div
          className={cn(
            "mt-3 text-xs text-red-400/90 rounded-lg px-2 py-1.5 border-l-[3px] border-l-red-500/60",
            CARD_SURFACE_C
          )}
        >
          Could not load status: {loadErr}
        </div>
      )}
      </div>

      <div className={FLEET_CARD_STATUS_FOOTER_ROW}>
        <span className="whitespace-nowrap">View aircraft status</span>
        <ArrowRight className="w-3.5 h-3.5 shrink-0" aria-hidden />
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
    NOT_AIRWORTHY: fleet.filter((a) => a.airworthiness === "NOT_AIRWORTHY").length,
  };

  return (
    <div className="flex flex-1 flex-col min-h-0 min-w-0 w-full overflow-hidden">
      <div
        className={cn(
          "flex flex-1 flex-col min-h-0 max-w-4xl mx-auto w-full min-w-0 px-4 sm:px-6 pb-2 overflow-hidden",
          TAB_PAGE_TOP_INSET
        )}
      >
        {/* Fleet header stats */}
        {!loading && fleet.length > 0 && (
          <div className="grid grid-cols-3 gap-2 sm:gap-3 mb-3 shrink-0">
            {[
              { label: "Airworthy", count: airworthyCounts.AIRWORTHY, color: "text-emerald-400" },
              { label: "Ferry Only", count: airworthyCounts.FERRY_ONLY, color: "text-yellow-400" },
              { label: "Grounded", count: airworthyCounts.NOT_AIRWORTHY, color: "text-red-400" },
            ].map((s) => (
              <div key={s.label} className={cn("rounded-xl p-3 sm:p-4 text-center", CARD_SURFACE_B)}>
                <div className={cn("text-2xl font-bold", s.color)}>{s.count}</div>
                <div className="text-xs text-zinc-500 mt-1">{s.label}</div>
              </div>
            ))}
          </div>
        )}

        {loading && (
          <div className="flex flex-col min-w-0">
            <div className="grid grid-cols-3 gap-2 sm:gap-3 mb-3 shrink-0 animate-pulse" aria-busy="true">
              {Array.from({ length: 3 }).map((_, i) => (
                <div
                  key={i}
                  className={cn("rounded-xl p-4 h-[5.25rem]", CARD_SURFACE_B)}
                >
                  <div className="h-8 bg-zinc-800 rounded mx-auto w-10 mb-2" />
                  <div className="h-3 bg-zinc-800/80 rounded mx-auto w-20" />
                </div>
              ))}
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4 w-full">
              {TAILS.map((t) => (
                <div
                  key={t}
                  className={cn(
                    "min-h-[14rem] rounded-xl p-5 sm:p-6 flex flex-col animate-pulse",
                    CARD_SURFACE_A
                  )}
                >
                  <div className="flex justify-between gap-3 mb-4">
                    <div className="flex gap-3 min-w-0">
                      <div className="w-10 h-10 rounded-lg bg-zinc-800 shrink-0" />
                      <div className="space-y-2 min-w-0 pt-0.5">
                        <div className="h-5 bg-zinc-800 rounded w-24" />
                        <div className="h-3 bg-zinc-800/80 rounded w-36 max-w-full" />
                      </div>
                    </div>
                    <div className="h-7 w-28 rounded-full bg-zinc-800 shrink-0" />
                  </div>
                  <div className="h-2 bg-zinc-800 rounded-full mb-4" />
                  <div className="grid grid-cols-2 gap-3 flex-1 content-start">
                    {Array.from({ length: 6 }).map((_, j) => (
                      <div key={j} className={cn("min-h-[4.25rem] rounded-lg", CARD_SURFACE_C)} />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Error */}
        {error && (
          <div
            className={cn(
              "shrink-0 rounded-xl p-4 text-red-400 text-sm mb-2",
              toneClasses("bad").bannerPanel
            )}
          >
            Failed to load fleet data: {error}
          </div>
        )}

        {/* Aircraft cards — 2×2; content-sized rows (no stretch); fleet column overflow-hidden = no scroll */}
        {!loading && !error && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4 w-full">
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
