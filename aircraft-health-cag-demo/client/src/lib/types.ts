export type Airworthiness = "AIRWORTHY" | "FERRY_ONLY" | "CAUTION" | "NOT_AIRWORTHY" | "UNKNOWN";

export interface HealthStatus {
  status: "ok" | "degraded" | "mock_cdf_offline" | "api_key_missing" | "api_key_invalid";
  anthropic_api_key_configured: boolean;
  mock_cdf_reachable: boolean;
  store: Record<string, number>;
  checkedAt: string;
}

export interface FleetAircraft {
  tail: string;
  name: string;
  smoh: number;
  tbo: number;
  smohPercent: number;
  hobbs: number;
  airworthiness: Airworthiness;
  isAirworthy: boolean;
  openSquawkCount: number;
  groundingSquawkCount: number;
  oilHoursOverdue: number;
  annualDaysRemaining: number | null;
  annualDueDate: string;
  activeSymptoms: number;
  activeConditions: number;
  metadata: Record<string, string>;
}

export interface AircraftSymptom {
  externalId: string;
  aircraftId: string;
  title: string;
  description: string;
  observation: string;
  severity: string;
  firstObserved: string;
  type: string;
}

export interface AircraftStatus {
  tail: string;
  hobbs: number;
  tach: number;
  engineSMOH: number;
  engineTBO: number;
  engineSMOHPercent: number;
  annualDueDate: string;
  annualDaysRemaining: number | null;
  openSquawkCount: number;
  groundingSquawkCount: number;
  airworthiness: Airworthiness;
  isAirworthy: boolean;
  oilHoursOverdue: number;
  oilNextDueHobbs: number;
  lastMaintenanceDate: string | null;
  activeSymptoms: number;
  symptoms: AircraftSymptom[];
  dataFreshAt: string;
}

export interface Squawk {
  externalId: string;
  description: string;
  component: string;
  severity: "grounding" | "non-grounding" | "cosmetic" | string;
  status: "open" | "resolved" | "deferred" | string;
  dateIdentified: string;
  tail: string;
  metadata: Record<string, string>;
}

export interface MaintenanceItem {
  component: string;
  description: string;
  maintenanceType: string;
  nextDueHobbs: number;
  hoursUntilDue: number;
  isOverdue: boolean;
  nextDueDate: string | null;
  daysUntilDue: number | null;
}

export interface MaintenanceRecord {
  externalId: string;
  type: string;
  subtype: string;
  description: string;
  startTime: number | null;
  metadata: Record<string, string>;
}

export interface MaintenanceHistoryPage {
  records: MaintenanceRecord[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

export interface FlightRecord {
  timestamp: string;
  hobbs_start: number;
  hobbs_end: number;
  duration: number;
  route: string;
  cht_max: number | null;
  egt_max: number | null;
  oil_pressure_min: number | null;
  oil_pressure_max: number | null;
  oil_temp_max: number | null;
  fuel_used_gal: number | null;
  pilot_notes: string;
  anomalous: boolean;
  year: number;
}

export interface FlightHistoryPage {
  records: FlightRecord[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

export interface ComponentNode {
  externalId: string;
  name: string;
  description: string | null;
  parentExternalId: string | null;
  metadata: Record<string, string>;
  lastMaintenanceDate: string | null;
  nextDueHobbs: number | null;
  nextDueDate: string | null;
  currentHobbs: number;
  hoursUntilDue: number | null;
  status: "ok" | "due_soon" | "overdue";
  maintenanceCount: number;
}

export interface OperationalPolicy {
  externalId: string;
  title: string;
  description: string;
  rule: string;
  category: string;
  references: string;
}

export interface GraphNode {
  id: string;
  label: string;
  type:
    | "asset"
    | "timeseries"
    | "event"
    | "file"
    | "SymptomNode"
    | "EngineModel"
    | "OperationalPolicy"
    | "FleetOwner";
  group: number;
  linkCount: number;
  unit?: string;
  metadata?: Record<string, string>;
}

export interface GraphLink {
  source: string;
  target: string;
  type: string;
  color?: string;
}

export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
  stats: Record<string, number>;
}

// SSE event types from the agent streaming endpoint
export type AgentEventType =
  | "thinking"
  | "tool_call"
  | "tool_result"
  | "traversal"
  | "final"
  | "error"
  | "done";

export interface AgentEvent {
  type: AgentEventType;
  content?: string;
  tool_name?: string;
  args?: Record<string, unknown>;
  summary?: string;
  node?: string;
  message?: string;
  iteration?: number;
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  traversalEvents?: AgentEvent[];
}
