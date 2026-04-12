import { Fragment, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  Clock,
  Wrench,
  CheckCircle,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import {
  cn,
  CARD_SURFACE_B,
  formatDate,
  formatTimestamp,
  AD_ACCENT_TEXT,
  MAIN_TAB_CONTENT_FRAME,
  TAB_PAGE_READABLE_COLUMN,
  TAB_PAGE_TOP_INSET,
  toneClasses,
  toneForDue,
  formatMaintenanceTypeLabel,
  formatAdReferenceLine,
  type Tone,
  type ToneClasses,
} from "../lib/utils";
import { api } from "../lib/api";
import { useStore, TAILS } from "../lib/store";
import { MenuSelect } from "./MenuSelect";
import type { MaintenanceItem, MaintenanceRecord } from "../lib/types";

const COMPONENT_OPTIONS = [
  { value: "", label: "All components" },
  { value: "ENGINE", label: "Engine" },
  { value: "PROPELLER", label: "Propeller" },
  { value: "AIRFRAME", label: "Airframe" },
  { value: "AVIONICS", label: "Avionics" },
];

const TONE_RANK: Record<Tone, number> = { unknown: 0, ok: 1, warn: 2, bad: 3 };

function mergeDueTone(hu: number | null, dd: number | null): ToneClasses {
  const candidates: ToneClasses[] = [];
  if (hu !== null && Number.isFinite(hu)) candidates.push(toneForDue(hu, "hours"));
  if (dd !== null && Number.isFinite(dd)) candidates.push(toneForDue(dd, "days"));
  if (candidates.length === 0) return toneForDue(null, "days");
  return candidates.reduce((a, b) => (TONE_RANK[a.tone] >= TONE_RANK[b.tone] ? a : b));
}

function buildUpcomingPrimaryLine(item: MaintenanceItem): string {
  const label = formatMaintenanceTypeLabel(item.maintenanceType);
  const hu = item.hoursUntilDue;
  const dd = item.daysUntilDue;

  if (hu === null && dd === null) {
    return item.summary || item.description;
  }
  if (hu === null && dd !== null) {
    return dd < 0
      ? `${label} overdue by ${Math.abs(dd)} days`
      : `${label} due in ${dd} days`;
  }

  const huOver = hu! < 0;
  const ddOver = dd !== null && dd < 0;

  if (huOver && !ddOver) {
    return `${label} overdue by ${Math.abs(hu!).toFixed(1)} hr`;
  }
  if (ddOver && !huOver) {
    return `${label} overdue by ${Math.abs(dd!)} days`;
  }
  if (huOver && ddOver) {
    return `${label} overdue by ${Math.abs(hu!).toFixed(1)} hr / ${Math.abs(dd!)} days`;
  }
  if (dd !== null) {
    return `${label} due in ${hu!.toFixed(1)} hr / ${dd} d`;
  }
  return `${label} due in ${hu!.toFixed(1)} hr`;
}

function formatDueAtSubtext(item: MaintenanceItem): string {
  const datePart = formatDate(item.nextDueDate) ?? "—";
  const hasTach = item.nextDueTach != null && Number.isFinite(item.nextDueTach);
  if (!hasTach) {
    return datePart;
  }
  const tach = item.nextDueTach!.toFixed(1);
  return `${tach} hr / ${datePart}`;
}

function UpcomingMetricsColumn({
  item,
  dueTone,
}: {
  item: MaintenanceItem;
  dueTone: ToneClasses;
}) {
  const hu = item.hoursUntilDue;
  const dd = item.daysUntilDue;
  const huOver = hu !== null && hu < 0;
  const ddOver = dd !== null && dd < 0;

  const hrBlock = (overdue: boolean, value: number) => (
    <div className="flex flex-col items-center">
      <p
        className={cn(
          overdue ? "text-lg" : "text-xl",
          "font-bold tabular-nums leading-tight",
          dueTone.text
        )}
      >
        {overdue ? Math.abs(value).toFixed(1) : value.toFixed(1)}
      </p>
      {overdue && <p className="text-xs text-red-400 font-semibold leading-tight">OVERDUE</p>}
      <p className="text-[10px] text-zinc-600">hr</p>
    </div>
  );

  const dayBlock = (overdue: boolean, value: number, unit: "d" | "days" = "d") => (
    <div className="flex flex-col items-center">
      <p
        className={cn(
          overdue ? "text-lg" : "text-xl",
          "font-bold tabular-nums leading-tight",
          dueTone.text
        )}
      >
        {overdue ? Math.abs(value) : value}
      </p>
      {overdue && <p className="text-xs text-red-400 font-semibold leading-tight">OVERDUE</p>}
      <p className="text-[10px] text-zinc-600">{unit}</p>
    </div>
  );

  if (hu !== null && dd !== null) {
    const dualHrDaysRow = (hVal: number, dVal: number) => (
      <div className="flex items-center justify-center gap-2">
        <div className="flex flex-col items-center text-center min-w-[2.75rem]">
          <span className={cn("text-lg font-bold tabular-nums leading-tight", dueTone.text)}>
            {hVal.toFixed(1)}
          </span>
          <span className="text-[10px] text-zinc-600 leading-tight mt-0.5">hr</span>
        </div>
        <span className="text-zinc-500 text-sm font-medium shrink-0 self-center" aria-hidden>
          /
        </span>
        <div className="flex flex-col items-center text-center min-w-[2.75rem]">
          <span className={cn("text-lg font-bold tabular-nums leading-tight", dueTone.text)}>{dVal}</span>
          <span className="text-[10px] text-zinc-600 leading-tight mt-0.5">days</span>
        </div>
      </div>
    );

    if (huOver && ddOver) {
      return (
        <div className="flex flex-col items-center gap-1">
          {dualHrDaysRow(Math.abs(hu), Math.abs(dd))}
          <p className="text-[10px] text-red-400 font-semibold leading-tight">OVERDUE</p>
        </div>
      );
    }
    if (huOver) {
      return hrBlock(true, hu);
    }
    if (ddOver) {
      return dayBlock(true, dd, "days");
    }
    return dualHrDaysRow(hu, dd);
  }

  if (hu !== null) {
    return hrBlock(huOver, hu);
  }
  if (dd !== null) {
    return dayBlock(ddOver, dd, "days");
  }
  return <p className="text-xs text-zinc-500">—</p>;
}

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
  const [availableYears, setAvailableYears] = useState<number[]>([]);
  const [loading, setLoading] = useState(false);
  const [upcomingLoading, setUpcomingLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const yearMenuOptions = useMemo(
    () => [
      { value: "", label: "All years" },
      ...availableYears.map((y) => ({ value: String(y), label: String(y) })),
    ],
    [availableYears]
  );

  // Upcoming maintenance — refetch on tail change
  useEffect(() => {
    setUpcomingLoading(true);
    api
      .upcomingMaintenance(tail)
      .then(setUpcoming)
      .catch(() => setUpcoming([]))
      .finally(() => setUpcomingLoading(false));
  }, [tail]);

  useEffect(() => {
    setYearFilter("");
    setPage(1);
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
        setAvailableYears(res.available_years ?? []);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [active, tail, page, componentFilter, yearFilter]);

  useEffect(() => {
    if (yearFilter === "") return;
    const y = Number(yearFilter);
    if (availableYears.length === 0 || !Number.isFinite(y) || !availableYears.includes(y)) {
      setYearFilter("");
      setPage(1);
    }
  }, [availableYears, yearFilter]);

  const handleFilterChange = () => {
    setPage(1);
  };

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

      <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain w-full min-w-0">
      <div className={cn(TAB_PAGE_READABLE_COLUMN, "pb-6 space-y-6")}>
      {/* Upcoming maintenance */}
      <section>
        <h2 className="text-xs font-semibold text-zinc-500 uppercase tracking-widest mb-3 flex items-center gap-2">
          <Clock className="w-3.5 h-3.5" />
          Upcoming Maintenance
        </h2>

        {upcomingLoading ? (
          <div className="space-y-2" aria-busy="true">
            {Array.from({ length: 2 }).map((_, i) => (
              <div
                key={i}
                className={cn("flex items-center gap-4 p-4 rounded-xl", CARD_SURFACE_B)}
              >
                <div className="shrink-0 w-24 flex flex-col items-center gap-1.5 animate-pulse">
                  <div className="h-7 bg-zinc-800 rounded w-16" />
                  <div className="h-3 bg-zinc-800/80 rounded w-10" />
                </div>
                <div className="flex-1 min-w-0 space-y-2 animate-pulse">
                  <div className="h-4 bg-zinc-800 rounded max-w-lg w-full" />
                  <div className="h-3 bg-zinc-800/80 rounded max-w-md w-3/4" />
                </div>
                <div className="shrink-0 animate-pulse">
                  <div className="h-7 w-[4.5rem] bg-zinc-800 rounded-full" />
                </div>
              </div>
            ))}
          </div>
        ) : upcoming.length === 0 ? (
          <div
            className={cn(
              "flex items-center gap-3 p-4 rounded-xl text-emerald-400",
              toneClasses("ok").bannerPanel
            )}
          >
            <CheckCircle className="w-5 h-5 shrink-0" />
            <p className="text-sm">No maintenance due in the next 250 hr (or annual calendar window)</p>
          </div>
        ) : (
          <div className="space-y-2">
            {upcoming.map((item, idx) => {
              const hu = item.hoursUntilDue;
              const dd = item.daysUntilDue;
              const dueTone = mergeDueTone(hu, dd);

              return (
                <div
                  key={idx}
                  className={cn("flex items-center gap-4 p-4 rounded-xl", dueTone.bannerPanel)}
                >
                  <div className="shrink-0 text-center w-24 min-w-[5.5rem] flex items-center justify-center">
                    <UpcomingMetricsColumn item={item} dueTone={dueTone} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p
                      className={cn(
                        "text-sm font-medium leading-snug",
                        dueTone.tone === "unknown" ? "text-zinc-200" : dueTone.text
                      )}
                    >
                      {buildUpcomingPrimaryLine(item)}
                    </p>
                    <p className="text-xs text-zinc-600 mt-1">{formatDueAtSubtext(item)}</p>
                  </div>
                  <div className="shrink-0">
                    <span
                      className={cn(
                        "text-xs px-2 py-1 rounded-full border font-medium uppercase tracking-wide",
                        dueTone.badge
                      )}
                    >
                      {formatMaintenanceTypeLabel(item.maintenanceType)}
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
          <h2 className="text-xs font-semibold text-zinc-500 uppercase tracking-widest flex items-center gap-2">
            <Wrench className="w-3.5 h-3.5" />
            Maintenance History
            {total > 0 && (
              <span className="text-zinc-600 normal-case font-normal">· {total} records</span>
            )}
          </h2>

          {/* Filters — same custom MenuSelect as Flights sort/year */}
          <div className="flex items-center gap-2 flex-wrap">
            <MenuSelect<string>
              ariaLabel="Filter maintenance by component"
              value={componentFilter}
              options={COMPONENT_OPTIONS}
              onChange={(v) => {
                setComponentFilter(v);
                handleFilterChange();
              }}
            />
            <MenuSelect<string>
              ariaLabel="Filter maintenance by year"
              value={yearFilter}
              options={yearMenuOptions}
              onChange={(v) => {
                setYearFilter(v);
                handleFilterChange();
              }}
            />
          </div>
        </div>

        {loading ? (
          <div className="space-y-2" aria-busy="true">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className={cn("rounded-xl p-3 min-h-[3.75rem] animate-pulse", CARD_SURFACE_B)}>
                <div className="flex items-start justify-between gap-2">
                  <div className="h-4 bg-zinc-800 rounded flex-1 max-w-xl" />
                  <div className="h-3 bg-zinc-800 rounded w-14 shrink-0" />
                </div>
                <div className="h-3 bg-zinc-800/80 rounded w-40 mt-2" />
              </div>
            ))}
          </div>
        ) : error ? (
          <div
            className={cn(
              "flex items-center gap-3 p-4 rounded-xl",
              toneClasses("bad").bannerPanel
            )}
          >
            <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
            <p className="text-sm text-red-300">{error}</p>
          </div>
        ) : history.length === 0 ? (
          <div className={cn("p-8 rounded-xl text-zinc-500 text-sm text-center", CARD_SURFACE_B)}>
            <Wrench className="w-8 h-8 mx-auto mb-2 opacity-30" />
            No maintenance records found. Run ingestion first.
          </div>
        ) : (
          <>
            <div className="space-y-2">
              {history.map((record, idx) => {
                const dateLine =
                  formatDate(record.metadata?.date) || formatTimestamp(record.startTime ?? null);
                const shop = record.metadata?.mechanic?.split("—")[0]?.trim();
                const adRef = record.metadata?.ad_reference?.trim();
                const metaParts: { key: string; node: ReactNode }[] = [];
                if (dateLine) {
                  metaParts.push({
                    key: "date",
                    node: <span className="text-zinc-600">{dateLine}</span>,
                  });
                }
                if (shop) {
                  metaParts.push({
                    key: "shop",
                    node: (
                      <span className="text-zinc-600 truncate max-w-[min(12rem,100%)]">{shop}</span>
                    ),
                  });
                }
                if (adRef) {
                  metaParts.push({
                    key: "ad",
                    node: (
                      <span className={cn("font-mono", AD_ACCENT_TEXT)}>
                        {formatAdReferenceLine(adRef)}
                      </span>
                    ),
                  });
                }
                return (
                  <div key={record.externalId || idx} className={cn("rounded-xl p-3", CARD_SURFACE_B)}>
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
                    {metaParts.length > 0 && (
                      <div className="mt-1.5 flex flex-wrap items-baseline text-xs">
                        {metaParts.map((part, i) => (
                          <Fragment key={part.key}>
                            {i > 0 && (
                              <span
                                className="text-zinc-500 select-none px-1.5 shrink-0"
                                aria-hidden
                              >
                                ·
                              </span>
                            )}
                            {part.node}
                          </Fragment>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
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
    </div>
  );
}
