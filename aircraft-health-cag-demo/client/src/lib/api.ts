import type {
  HealthStatus,
  AircraftStatus,
  FleetAircraft,
  Squawk,
  MaintenanceItem,
  MaintenanceHistoryPage,
  FlightHistoryPage,
  ComponentNode,
  GraphData,
  OperationalPolicy,
} from "./types";

const BASE = "/api";

async function get<T>(path: string, init?: { signal?: AbortSignal }): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { signal: init?.signal });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

function withTail(path: string, tail: string | null, extra?: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  if (tail) params.set("aircraft", tail);
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      if (v !== undefined) params.set(k, String(v));
    }
  }
  const qs = params.toString();
  return `${path}${qs ? "?" + qs : ""}`;
}

export const api = {
  health: () => get<HealthStatus>("/health"),

  fleet: () => get<FleetAircraft[]>("/fleet"),

  policies: () => get<OperationalPolicy[]>("/policies"),

  status: (tail: string) =>
    get<AircraftStatus>(withTail("/status", tail)),

  squawks: (tail: string) =>
    get<Squawk[]>(withTail("/squawks", tail)),

  upcomingMaintenance: (tail: string) =>
    get<MaintenanceItem[]>(withTail("/maintenance/upcoming", tail)),

  maintenanceHistory: (
    tail: string,
    opts: {
      page?: number;
      per_page?: number;
      component?: string;
      year?: number;
      maint_type?: string;
      signal?: AbortSignal;
    } = {}
  ) => {
    const { signal, ...query } = opts;
    return get<MaintenanceHistoryPage>(
      withTail("/maintenance/history", tail, {
        page: query.page,
        per_page: query.per_page,
        component: query.component,
        year: query.year,
        maint_type: query.maint_type,
      }),
      { signal }
    );
  },

  flights: (
    tail: string,
    opts: {
      page?: number;
      per_page?: number;
      route?: string;
      year?: number;
      sort?: string;
      order?: "asc" | "desc";
    } = {}
  ) =>
    get<FlightHistoryPage>(
      withTail("/flights", tail, {
        page: opts.page,
        per_page: opts.per_page,
        route: opts.route,
        year: opts.year,
        sort: opts.sort,
        order: opts.order,
      })
    ),

  components: (tail: string, init?: { signal?: AbortSignal }) =>
    get<ComponentNode[]>(withTail("/components", tail), init),

  graph: () => get<GraphData>("/graph"),
};
