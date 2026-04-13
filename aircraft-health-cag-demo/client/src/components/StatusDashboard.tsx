import { useEffect, useState } from "react";
import {
  Timer,
  Calendar,
  AlertTriangle,
  CheckCircle,
  Fan,
  Info,
  XCircle,
  Droplet,
  Wrench,
  MessageSquare,
} from "lucide-react";
import {
  cn,
  CARD_SURFACE_B,
  formatDate,
  formatDateMMDDYYHyphen,
  formatSignedOilHoursCompact,
  MAIN_TAB_CONTENT_FRAME,
  TAB_PAGE_READABLE_COLUMN,
  TAB_PAGE_TOP_INSET,
  severityPillClass,
  severityRowClass,
  toneClasses,
  toneForAirworthiness,
  toneForDue,
  toneForOilLife,
  toneForSquawks,
  type ToneClasses,
} from "../lib/utils";
import { api } from "../lib/api";
import { TAILS, type TailNumber } from "../lib/store";
import type { AircraftStatus, Squawk } from "../lib/types";

/** Inset from card edges; reserve matches bottom-2 + line + gap (~pb-8 after caller padding). */
const ASSISTANT_FOOTER_ROW =
  "pointer-events-none absolute left-2 bottom-2 z-10 inline-flex shrink-0 items-center gap-0.5 text-xs leading-none text-sky-400 opacity-0 transition-opacity duration-150 group-hover/card:pointer-events-auto group-hover/card:opacity-100";

type AirworthinessState = "AIRWORTHY" | "FERRY_ONLY" | "CAUTION" | "NOT_AIRWORTHY" | "UNKNOWN";

type OilLifeParts = {
  tone: ToneClasses;
  hasOilHorizon: boolean;
  hoursLine: string;
  daysLine: string;
};

function oilLifeParts(status: AircraftStatus): OilLifeParts {
  const oilOver = Number(status.oilTachHoursOverdue ?? status.oilHoursOverdue) || 0;
  const rawUntil = Number(status.oilTachHoursUntilDue);
  const oilUntil = Number.isFinite(rawUntil) ? rawUntil : 0;
  const oilDays = status.oilDaysUntilDue;

  const signedOilHours = oilOver > 0 ? -oilOver : oilUntil;
  const hasOilHorizon = oilOver > 0 || oilUntil !== 0 || oilDays !== null;

  const nbsp = "\u00a0";
  /** Compact "d" here only — annual and other copy still use full "days" where needed. */
  const formatSignedDays = (d: number | null) => {
    if (d === null) return `—${nbsp}d`;
    const sign = d < 0 ? "-" : "";
    return `${sign}${Math.abs(d)}${nbsp}d`;
  };

  const hoursLine = hasOilHorizon
    ? formatSignedOilHoursCompact(signedOilHours, nbsp)
    : `—${nbsp}hr`;
  const daysLine = hasOilHorizon ? formatSignedDays(oilDays) : `—${nbsp}d`;

  const tone = !hasOilHorizon
    ? toneClasses("unknown")
    : toneForOilLife({
        oilHoursOverdue: oilOver,
        oilTachHoursUntilDue: signedOilHours,
        oilDaysUntilDue: oilDays,
      });

  return { tone, hasOilHorizon, hoursLine, daysLine };
}

/** Prefer one row (hours / d); flex-wrap only if the tile is too narrow. */
function OilLifeMainValue({ parts }: { parts: OilLifeParts }) {
  const c = parts.tone.tone === "unknown" ? "text-zinc-200" : parts.tone.text;
  const nbsp = "\u00a0";
  return (
    <div
      className={cn(
        "flex flex-wrap items-baseline min-w-0 w-full text-lg sm:text-xl font-bold tabular-nums leading-tight text-left",
        c
      )}
    >
      <span className="whitespace-nowrap">{parts.hoursLine}</span>
      <span className="whitespace-nowrap">
        {nbsp}/{nbsp}
        {parts.daysLine}
      </span>
    </div>
  );
}

function deriveAirworthiness(status: AircraftStatus): AirworthinessState {
  return (status.airworthiness as AirworthinessState) || "UNKNOWN";
}

function formatBannerAnnual(status: AircraftStatus): string {
  const d = status.annualDaysRemaining;
  if (d === null) return "Annual unknown";
  if (d < 0) return "Annual expired";
  if (d <= 30) return "Annual due soon";
  return "Annual current";
}

function formatBannerOil(status: AircraftStatus): string {
  const oilOver = Number(status.oilTachHoursOverdue ?? status.oilHoursOverdue) || 0;
  const rawUntil = Number(status.oilTachHoursUntilDue);
  const oilUntil = Number.isFinite(rawUntil) ? rawUntil : 0;
  const oilDays = status.oilDaysUntilDue;

  const signedOilHours = oilOver > 0 ? -oilOver : oilUntil;
  const hasOilHorizon = oilOver > 0 || oilUntil !== 0 || oilDays !== null;
  if (!hasOilHorizon) return "Oil unknown";

  const tone = toneForOilLife({
    oilHoursOverdue: oilOver,
    oilTachHoursUntilDue: signedOilHours,
    oilDaysUntilDue: oilDays,
  });

  if (signedOilHours < 0 || (oilDays !== null && oilDays < 0)) {
    return tone.tone === "bad" ? "Oil overdue" : "Oil overdue (ferry only)";
  }
  if (signedOilHours === 0 || oilDays === 0) return "Oil due now";
  return tone.tone === "warn" ? "Oil due soon" : "Oil current";
}

function formatBannerSquawks(status: AircraftStatus): string {
  const n = status.groundingSquawkCount;
  if (n <= 0) return "No grounding squawks";
  if (n === 1) return "1 grounding squawk";
  return `${n} grounding squawks`;
}

function buildAirworthinessBannerLine2(status: AircraftStatus): string {
  return `${formatBannerAnnual(status)} · ${formatBannerOil(status)} · ${formatBannerSquawks(status)}`;
}

/** Shared clickable panel — AI hint only on hover, pinned bottom-left (same inset as stat tiles). */
function ClickPanel({
  chatHint,
  onClick,
  className,
  children,
}: {
  chatHint: string;
  onClick: () => void;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={chatHint}
      aria-label={chatHint}
      className={cn(
        "relative block w-full min-w-0 text-left cursor-pointer group/card",
        "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-500",
        className,
        "pb-8"
      )}
    >
      <div className="min-w-0">{children}</div>
      <div className={ASSISTANT_FOOTER_ROW}>
        <MessageSquare className="w-3 h-3 shrink-0" aria-hidden />
        <span>Ask in AI Assistant</span>
      </div>
    </button>
  );
}

interface Props {
  selectedAircraft: TailNumber | null;
  onSelectAircraft: (tail: TailNumber | null) => void;
  onOpenAssistant?: () => void;
}

export default function StatusDashboard({ selectedAircraft, onSelectAircraft, onOpenAssistant }: Props) {
  const tail = selectedAircraft ?? "N4798E";
  const [status, setStatus] = useState<AircraftStatus | null>(null);
  const [squawks, setSquawks] = useState<Squawk[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    Promise.all([api.status(tail), api.squawks(tail)])
      .then(([s, sq]) => {
        setStatus(s);
        setSquawks(sq);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [tail]);

  const open = onOpenAssistant ?? (() => {});

  return (
    <div
      className={cn(
        "flex flex-col flex-1 min-h-0 w-full min-w-0 pb-6",
        MAIN_TAB_CONTENT_FRAME,
        TAB_PAGE_TOP_INSET
      )}
    >
      <div className={cn("shrink-0 mb-3", TAB_PAGE_READABLE_COLUMN)}>
        <div className="flex items-center gap-2">
          <div className="flex gap-1 flex-wrap">
            {TAILS.map((t) => (
              <button
                key={t}
                type="button"
                onClick={() => onSelectAircraft(t)}
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

      <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain w-full min-w-0">
        <div className={cn(TAB_PAGE_READABLE_COLUMN, "pb-6")}>
          {error ? (
            <ErrorState message={error} />
          ) : loading || !status || status.tail !== tail ? (
            <DashboardSkeleton />
          ) : (
            <DashboardContent status={status} squawks={squawks} onOpenAssistant={open} />
          )}
        </div>
      </div>
    </div>
  );
}

function DashboardContent({
  status,
  squawks,
  onOpenAssistant,
}: {
  status: AircraftStatus;
  squawks: Squawk[];
  onOpenAssistant: () => void;
}) {
  const airworthiness = deriveAirworthiness(status);
  const smohPct = Math.min(100, status.engineSMOHPercent);
  const smohBarColor =
    smohPct >= 85 ? "bg-red-500" : smohPct >= 65 ? "bg-yellow-500" : "bg-emerald-500";
  const open = onOpenAssistant;

  const nbsp = "\u00a0";
  const tachPart =
    status.oilNextDueTach > 0 ? `${status.oilNextDueTach.toFixed(1)}${nbsp}hr` : "—";
  const datePart = status.oilNextDueDate
    ? formatDateMMDDYYHyphen(status.oilNextDueDate) ?? "—"
    : "—";
  const oilSub =
    status.oilNextDueTach > 0 || status.oilNextDueDate
      ? `${tachPart} / ${datePart}`
      : "Per maintenance log";

  const oilLife = oilLifeParts(status);
  const annualTone = toneForDue(status.annualDaysRemaining, "days");
  const squawkTone = toneForSquawks(status.openSquawkCount, status.groundingSquawkCount);

  const annualSub =
    status.annualDaysRemaining === null
      ? "—"
      : status.annualDaysRemaining < 0
      ? `-${Math.abs(status.annualDaysRemaining)}${nbsp}days`
      : `${status.annualDaysRemaining}${nbsp}days`;

  return (
    <div className="space-y-6">
      <AirworthinessBanner state={airworthiness} status={status} onOpenAssistant={open} />

      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        <NeutralTimeCard
          icon={<Timer className="w-4 h-4" aria-hidden />}
          title="Aircraft Time"
          value={status.hobbs}
          caption="Hobbs"
          chatHint="Ask about Hobbs (rental / display) time in the AI Assistant"
          onClick={open}
        />
        <NeutralTimeCard
          icon={<Fan className="w-4 h-4" aria-hidden />}
          title="Engine Time"
          value={status.tach}
          caption="Tach"
          chatHint="Ask about tach (maintenance) time in the AI Assistant"
          onClick={open}
        />
        <StatCard
          icon={<Droplet className="w-4 h-4" aria-hidden />}
          label="Oil Life"
          valueContent={<OilLifeMainValue parts={oilLife} />}
          sub={oilSub}
          tone={oilLife.tone}
          chatHint="Ask about oil change interval in the AI Assistant"
          onClick={open}
        />
        <StatCard
          icon={<Calendar className="w-4 h-4" aria-hidden />}
          label="Annual Due"
          value={formatDate(status.annualDueDate) ?? "Unknown"}
          sub={annualSub}
          tone={annualTone}
          chatHint="Ask about annual inspection currency in the AI Assistant"
          onClick={open}
        />
        <StatCard
          icon={<AlertTriangle className="w-4 h-4" />}
          label="Squawks"
          value={`${status.openSquawkCount}${nbsp}open`}
          sub={
            status.groundingSquawkCount > 0
              ? `${status.groundingSquawkCount}${nbsp}grounding`
              : `0${nbsp}grounding`
          }
          tone={squawkTone}
          chatHint="Ask about open squawks in the AI Assistant"
          onClick={open}
        />
      </div>

      <ClickPanel
        chatHint="Ask about engine time, TBOH, and H2AD maintenance in the AI Assistant"
        onClick={open}
        className={cn("rounded-xl p-4 hover:border-zinc-700", CARD_SURFACE_B)}
      >
        <div className="flex justify-between items-baseline gap-3 mb-3">
          <div className="min-w-0 flex items-baseline gap-1.5 flex-wrap">
            <span className="text-sm font-medium text-zinc-300 shrink-0">Engine Life</span>
            <span className="text-xs text-zinc-500 shrink-0" aria-hidden>
              ·
            </span>
            <span className="text-xs text-zinc-500 min-w-0 truncate" title="Lycoming O-320-H2AD">
              Lycoming O-320-H2AD
            </span>
          </div>
          <span className="text-sm tabular-nums shrink-0">
            <span className="text-zinc-300 font-medium">
              {status.engineSMOH.toFixed(0)} / {status.engineTBO} hr
            </span>
            <span className="text-zinc-500 font-normal">
              {"\u00a0"}·{"\u00a0"}
              {smohPct.toFixed(0)}%
            </span>
          </span>
        </div>
        <div className="h-3 bg-zinc-800 rounded-full overflow-hidden">
          <div
            className={cn("h-full rounded-full transition-all duration-700", smohBarColor)}
            style={{ width: `${smohPct}%` }}
          />
        </div>
      </ClickPanel>

      {squawks.length > 0 && (
        <div className="space-y-2">
          <h2 className="text-xs font-semibold text-zinc-500 uppercase tracking-widest flex items-center gap-2">
            <AlertTriangle className="w-3.5 h-3.5 text-yellow-400/70" aria-hidden />
            Open Squawks
          </h2>
          {squawks.map((sq) => (
            <ClickPanel
              key={sq.externalId}
              chatHint={`Ask about squawk: ${sq.description}`}
              onClick={open}
              className={cn("p-3 text-sm", severityRowClass(sq.severity))}
            >
              <div className="flex items-start gap-3 w-full min-w-0">
                <div className="flex-1 min-w-0">
                  <p className="font-medium leading-snug">{sq.description}</p>
                  <p className="text-xs opacity-60 mt-0.5">
                    Reported {formatDate(sq.dateIdentified)}
                  </p>
                </div>
                <span
                  className={cn(
                    "shrink-0 self-start text-xs px-2 py-0.5 rounded-full border uppercase tracking-wide font-medium",
                    severityPillClass(sq.severity)
                  )}
                >
                  {sq.severity}
                </span>
              </div>
            </ClickPanel>
          ))}
        </div>
      )}

      <ClickPanel
        chatHint="Ask about maintenance history and upcoming items in the AI Assistant"
        onClick={open}
        className={cn("rounded-xl p-4 hover:border-zinc-700", CARD_SURFACE_B)}
      >
        <div className="flex items-center gap-2 mb-1">
          <Wrench className="w-4 h-4 text-zinc-500" />
          <span className="text-sm font-medium text-zinc-400">Last Maintenance</span>
        </div>
        <p className="text-lg font-semibold text-zinc-200">
          {formatDate(status.lastMaintenanceDate)}
        </p>
      </ClickPanel>
    </div>
  );
}

const BANNER_HINTS: Record<AirworthinessState, string> = {
  AIRWORTHY: "Ask about airworthiness and operational status in the AI Assistant",
  FERRY_ONLY: "Ask about ferry policy and oil compliance in the AI Assistant",
  CAUTION: "Ask about caution items and required inspections in the AI Assistant",
  NOT_AIRWORTHY: "Ask about grounding reasons and next steps in the AI Assistant",
  UNKNOWN: "Ask about airworthiness status in the AI Assistant",
};

const BANNER_STYLE: Record<
  AirworthinessState,
  { Icon: typeof CheckCircle; line1: string; hoverBorder: string }
> = {
  AIRWORTHY: {
    Icon: CheckCircle,
    line1: "AIRWORTHY",
    hoverBorder: "hover:border-emerald-700",
  },
  FERRY_ONLY: {
    Icon: Info,
    line1: "FERRY ONLY",
    hoverBorder: "hover:border-yellow-600",
  },
  CAUTION: {
    Icon: AlertTriangle,
    line1: "CAUTION",
    hoverBorder: "hover:border-yellow-600",
  },
  NOT_AIRWORTHY: {
    Icon: XCircle,
    line1: "NOT AIRWORTHY",
    hoverBorder: "hover:border-red-600",
  },
  UNKNOWN: {
    Icon: Info,
    line1: "UNKNOWN",
    hoverBorder: "hover:border-zinc-600",
  },
};

function AirworthinessBanner({
  state,
  status,
  onOpenAssistant,
}: {
  state: AirworthinessState;
  status: AircraftStatus;
  onOpenAssistant: () => void;
}) {
  const cfg = BANNER_STYLE[state];
  const tone = toneForAirworthiness(state);
  const Icon = cfg.Icon;
  const line2 = buildAirworthinessBannerLine2(status);

  return (
    <ClickPanel
      chatHint={BANNER_HINTS[state]}
      onClick={onOpenAssistant}
      className={cn(
        "p-4 rounded-xl transition-colors",
        tone.bannerPanel,
        tone.text,
        cfg.hoverBorder
      )}
    >
      <div className="flex items-start gap-3">
        <Icon className="w-5 h-5 shrink-0 mt-0.5" aria-hidden />
        <div className="min-w-0">
          <p className="font-semibold text-sm">{cfg.line1}</p>
          <p className="text-xs opacity-70 mt-0.5 leading-snug">{line2}</p>
        </div>
      </div>
    </ClickPanel>
  );
}

/** Hobbs or tach reading — neutral (blue carries no meaning). */
function NeutralTimeCard({
  icon,
  title,
  value,
  caption,
  chatHint,
  onClick,
}: {
  icon: React.ReactNode;
  title: string;
  value: number;
  caption: string;
  chatHint: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={chatHint}
      aria-label={chatHint}
      className={cn(
        "relative flex flex-col rounded-xl p-3 text-left w-full min-h-0 h-full cursor-pointer transition-all group/card",
        "items-stretch min-w-0",
        CARD_SURFACE_B,
        "hover:border-zinc-700",
        "focus-visible:outline focus-visible:outline-2 focus-visible:outline-sky-500",
        "pb-8"
      )}
    >
      <div className="min-w-0 flex-1 flex flex-col min-h-0">
        <div className="flex items-center gap-1.5 mb-1 text-zinc-400 w-full min-w-0">
          {icon}
          <span className="text-[10px] font-semibold uppercase tracking-wide truncate">{title}</span>
        </div>
        <p className="text-lg sm:text-xl font-bold tabular-nums text-zinc-200 leading-tight text-left w-full min-w-0 whitespace-nowrap">
          {value.toFixed(1)}
          {"\u00a0"}
          hr
        </p>
        <p className="text-xs text-zinc-600 mt-0.5">{caption}</p>
      </div>
      <div className={ASSISTANT_FOOTER_ROW}>
        <MessageSquare className="w-3 h-3 shrink-0" aria-hidden />
        <span>Ask in AI Assistant</span>
      </div>
    </button>
  );
}

function StatCard({
  icon,
  label,
  value,
  valueContent,
  valueClassName,
  sub,
  tone,
  chatHint,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  value?: string;
  valueContent?: React.ReactNode;
  valueClassName?: string;
  sub: string;
  tone: ToneClasses;
  chatHint: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={chatHint}
      aria-label={chatHint}
      className={cn(
        "relative flex flex-col rounded-xl p-3 text-left w-full min-h-0 h-full cursor-pointer transition-all group/card",
        "items-stretch min-w-0",
        tone.panel,
        "focus-visible:outline focus-visible:outline-2 focus-visible:outline-sky-500",
        "pb-8"
      )}
    >
      <div className="min-w-0 flex-1 flex flex-col min-h-0">
        <div className={cn("flex items-center gap-1.5 mb-1 w-full min-w-0", tone.text)}>
          {icon}
          <span className="text-[10px] font-semibold uppercase tracking-wide truncate">{label}</span>
        </div>
        {valueContent ?? (
          <p
            className={cn(
              valueClassName ?? "text-lg sm:text-xl font-bold tabular-nums leading-tight",
              "text-left w-full min-w-0 whitespace-nowrap",
              tone.tone === "unknown" ? "text-zinc-200" : tone.text
            )}
          >
            {value}
          </p>
        )}
        <p className="text-xs text-zinc-500 mt-0.5 text-left break-words">{sub}</p>
      </div>
      <div className={ASSISTANT_FOOTER_ROW}>
        <MessageSquare className="w-3 h-3 shrink-0" aria-hidden />
        <span>Ask in AI Assistant</span>
      </div>
    </button>
  );
}

function DashboardSkeleton() {
  return (
    <div className="space-y-6" aria-busy="true">
      <div className={cn("rounded-xl p-4 pb-8 min-h-[5.5rem]", CARD_SURFACE_B)}>
        <div className="flex gap-3 animate-pulse">
          <div className="w-5 h-5 rounded-full bg-zinc-800 shrink-0 mt-0.5" />
          <div className="flex-1 space-y-2 min-w-0">
            <div className="h-4 bg-zinc-800 rounded w-40 max-w-full" />
            <div className="h-3 bg-zinc-800/80 rounded w-full max-w-md" />
          </div>
        </div>
      </div>
      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <div
            key={i}
            className={cn("rounded-xl p-3 pb-8 min-h-[5.5rem] animate-pulse", CARD_SURFACE_B)}
          >
            <div className="h-3 bg-zinc-800 rounded w-24 mb-2" />
            <div className="h-8 bg-zinc-800 rounded w-28 mb-2" />
            <div className="h-3 bg-zinc-800/80 rounded w-16" />
          </div>
        ))}
      </div>
      <div className={cn("rounded-xl p-4 min-h-[6.5rem] space-y-3 animate-pulse", CARD_SURFACE_B)}>
        <div className="flex justify-between gap-4">
          <div className="h-4 bg-zinc-800 rounded flex-1 max-w-xs" />
          <div className="h-4 bg-zinc-800 rounded w-28 shrink-0" />
        </div>
        <div className="h-3 bg-zinc-800 rounded-full w-full" />
        <div className="h-3 bg-zinc-800/80 rounded w-full max-w-lg" />
      </div>
      <div className={cn("rounded-xl p-4 min-h-[5.5rem] animate-pulse", CARD_SURFACE_B)}>
        <div className="h-4 bg-zinc-800 rounded w-36 mb-3" />
        <div className="h-6 bg-zinc-800 rounded w-28" />
        <div className="h-3 bg-zinc-800/80 rounded w-44 mt-2" />
      </div>
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-20 text-zinc-500">
      <XCircle className="w-8 h-8 mb-3 text-red-500" />
      <p className="font-medium text-zinc-300">Could not load aircraft status</p>
      <p className="text-sm mt-1 font-mono text-zinc-600 max-w-md text-center">{message}</p>
      <p className="text-xs mt-3 text-zinc-700">
        Make sure the mock CDF server (port 4001) and API server (port 8080) are running.
      </p>
    </div>
  );
}
