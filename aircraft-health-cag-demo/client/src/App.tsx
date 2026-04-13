import { useEffect, useState } from "react";
import {
  Plane,
  Activity,
  Wrench,
  MessageSquare,
  Waypoints,
  PlaneTakeoff,
  Loader2,
  Puzzle,
  Warehouse,
} from "lucide-react";
import { cn } from "./lib/utils";
import { api } from "./lib/api";
import { useStore, type TailNumber } from "./lib/store";
import type { HealthStatus } from "./lib/types";
import SetupBanner from "./components/SetupBanner";
import StatusDashboard from "./components/StatusDashboard";
import QueryInterface from "./components/QueryInterface";
import MaintenanceTimeline from "./components/MaintenanceTimeline";
import AircraftComponents from "./components/AircraftComponents";
import FlightHistory from "./components/FlightHistory";
import KnowledgeGraph from "./components/KnowledgeGraph";
import FleetPage from "./components/FleetPage";
import FloatingChatDock from "./components/FloatingChatDock";

type Tab = "fleet" | "dashboard" | "query" | "maintenance" | "aircraft" | "flights" | "graph";

/**
 * Tabs where the page implies a single tail — Fleet is disabled in the floating chat;
 * if scope were fleet-wide, we default the store to a tail for those UIs.
 */
const TABS_WITHOUT_FLEET_SCOPE: Tab[] = ["dashboard", "maintenance", "aircraft", "flights"];

/** Hangar / Knowledge Graph — no in-page tail strip; keep a tail only while floating chat is open. */
const TABS_WITHOUT_PAGE_AIRCRAFT_SELECTION: Tab[] = ["fleet", "graph"];

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("fleet");
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [healthError, setHealthError] = useState(false);
  const {
    isQuerying,
    selectedAircraft,
    setSelectedAircraft,
    floatingChatOpen,
    setFloatingChatOpen,
  } = useStore();

  useEffect(() => {
    api.health()
      .then(setHealth)
      .catch(() => setHealthError(true));
  }, []);

  useEffect(() => {
    if (activeTab === "query") {
      setFloatingChatOpen(false);
    }
  }, [activeTab, setFloatingChatOpen]);

  useEffect(() => {
    if (TABS_WITHOUT_FLEET_SCOPE.includes(activeTab) && selectedAircraft === null) {
      setSelectedAircraft("N4798E");
    }
  }, [activeTab, selectedAircraft, setSelectedAircraft]);

  useEffect(() => {
    if (
      TABS_WITHOUT_PAGE_AIRCRAFT_SELECTION.includes(activeTab) &&
      !floatingChatOpen &&
      selectedAircraft !== null
    ) {
      setSelectedAircraft(null);
    }
  }, [activeTab, floatingChatOpen, selectedAircraft, setSelectedAircraft]);

  const handleNavigate = (tab: Tab, tail?: TailNumber) => {
    if (tail) setSelectedAircraft(tail);
    setActiveTab(tab);
  };

  const apiKeyMissing = health ? !health.anthropic_api_key_configured : false;
  const mockCdfOffline = health ? !health.mock_cdf_reachable : false;
  const mockCdfNoFleetData =
    health?.mock_cdf_reachable === true &&
    health.mock_cdf_fleet_ready === false;

  const primaryTabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "fleet", label: "Hangar", icon: <Warehouse className="w-4 h-4 shrink-0" /> },
    { id: "dashboard", label: "Aircraft Status", icon: <Activity className="w-4 h-4 shrink-0" /> },
    { id: "maintenance", label: "Maintenance", icon: <Wrench className="w-4 h-4 shrink-0" /> },
    { id: "aircraft", label: "Components", icon: <Puzzle className="w-4 h-4 shrink-0" /> },
    { id: "flights", label: "Flights", icon: <PlaneTakeoff className="w-4 h-4 shrink-0" /> },
  ];

  const secondaryTabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    {
      id: "query",
      label: "AI Assistant",
      icon: isQuerying ? (
        <Loader2 className="w-4 h-4 shrink-0 animate-spin text-sky-400" />
      ) : (
        <MessageSquare className="w-4 h-4 shrink-0" />
      ),
    },
    { id: "graph", label: "Knowledge Graph", icon: <Waypoints className="w-4 h-4 shrink-0" /> },
  ];

  const tabButtonClass = (tabId: Tab) =>
    cn(
      "flex items-center gap-1.5 px-3 py-3 text-sm font-medium border-b-2 -mb-px whitespace-nowrap transition-colors shrink-0",
      activeTab === tabId
        ? "border-sky-500 text-sky-400"
        : "border-transparent text-zinc-500 hover:text-zinc-300 hover:border-zinc-600"
    );

  return (
    <div className="h-screen bg-zinc-950 text-zinc-100 flex flex-col overflow-hidden">
      {/* Header — 56px */}
      <header className="h-14 shrink-0 border-b border-zinc-800 bg-zinc-900/90 backdrop-blur-sm z-50">
        <div className="h-full max-w-screen-2xl mx-auto px-4 sm:px-6 flex items-center justify-between gap-4">
          {/* Left: fleet identity */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="p-2 bg-sky-500/10 rounded-lg border border-sky-500/20">
              <Plane className="w-4 h-4 text-sky-400" aria-hidden />
            </div>
            <div>
              <h1 className="text-sm font-semibold text-zinc-100 tracking-wide">Desert Sky Aviation</h1>
              <p className="text-xs text-zinc-500 hidden sm:block">KPHX · 4 × 1978 Cessna 172N</p>
            </div>
          </div>

          {/* Right: system status dots */}
          <div className="flex items-center gap-3 shrink-0">
            {health && (
              <div className="flex items-center gap-2">
                <StatusDot
                  label="LLM"
                  ok={health.anthropic_api_key_configured}
                  tooltip={health.anthropic_api_key_configured ? "Anthropic API key configured" : "API key missing"}
                />
                <StatusDot
                  label="KG"
                  ok={health.mock_cdf_reachable && (health.mock_cdf_fleet_ready ?? true)}
                  tooltip={
                    !health.mock_cdf_reachable
                      ? "Mock CDF offline"
                      : health.mock_cdf_fleet_ready === false
                        ? "Mock CDF (port 4001) responds but fleet data missing — free port 4001 and restart npm run dev"
                        : `Knowledge graph online — ${(health.store?.assets ?? 0)} assets`
                  }
                />
                <StatusDot
                  label="API"
                  ok={health.status === "ok"}
                  tooltip={health.status === "ok" ? "All systems online" : `Status: ${health.status}`}
                />
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Tab bar — primary left, AI + graph flush right (ml-auto); comfortable vertical padding */}
      <div className="shrink-0 border-b border-zinc-800 bg-zinc-900/70 z-40">
        <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 flex items-stretch min-w-0 min-h-[3.25rem]">
          <div className="flex items-stretch gap-0 overflow-x-auto scrollbar-hide min-w-0">
            {primaryTabs.map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => setActiveTab(tab.id)}
                className={tabButtonClass(tab.id)}
              >
                {tab.icon}
                {tab.label}
              </button>
            ))}
          </div>
          <div className="flex items-stretch gap-0 shrink-0 pl-2 sm:pl-3 ml-auto">
            {secondaryTabs.map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => setActiveTab(tab.id)}
                className={tabButtonClass(tab.id)}
              >
                {tab.icon}
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Setup banners */}
      {(apiKeyMissing || mockCdfOffline || mockCdfNoFleetData || healthError) && (
        <SetupBanner
          apiKeyMissing={apiKeyMissing}
          mockCdfOffline={mockCdfOffline}
          mockCdfNoFleetData={mockCdfNoFleetData}
          connectionError={healthError}
        />
      )}

      {/* Main content: no document scroll — each tab uses flex-1 min-h-0; scroll only inside panels/cards */}
      <main className="flex-1 min-h-0 flex flex-col overflow-hidden w-full min-w-0">
        <div className="flex-1 min-h-0 overflow-hidden w-full min-w-0 flex flex-col">
          {/* Full viewport width so page-level overflow-y (e.g. Aircraft Status) scrollbars sit on the browser edge */}
          <div className="relative flex-1 min-h-0 w-full min-w-0 flex flex-col overflow-hidden">
            <div
              className={cn(
                activeTab === "fleet"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <FleetPage onNavigate={handleNavigate} />
            </div>
            <div
              className={cn(
                activeTab === "dashboard"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <StatusDashboard
                selectedAircraft={selectedAircraft}
                onSelectAircraft={setSelectedAircraft}
                onOpenAssistant={() => setActiveTab("query")}
              />
            </div>
            <div
              className={cn(
                activeTab === "query"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <QueryInterface apiKeyMissing={apiKeyMissing} />
            </div>
            <div
              className={cn(
                activeTab === "maintenance"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <MaintenanceTimeline active={activeTab === "maintenance"} />
            </div>
            <div
              className={cn(
                activeTab === "aircraft"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <AircraftComponents active={activeTab === "aircraft"} />
            </div>
            <div
              className={cn(
                activeTab === "flights"
                  ? "flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "hidden"
              )}
            >
              <FlightHistory active={activeTab === "flights"} />
            </div>
            <div
              className={cn(
                activeTab === "graph"
                  ? "relative z-[1] flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden"
                  : "absolute inset-0 z-0 opacity-0 pointer-events-none min-h-0"
              )}
              aria-hidden={activeTab !== "graph"}
            >
              <KnowledgeGraph active={activeTab === "graph"} />
            </div>
          </div>
        </div>
      </main>

      <FloatingChatDock
        visible={activeTab !== "query"}
        apiKeyMissing={apiKeyMissing}
        fleetOptionDisabled={TABS_WITHOUT_FLEET_SCOPE.includes(activeTab)}
      />
    </div>
  );
}

function StatusDot({ label, ok, tooltip }: { label: string; ok: boolean; tooltip: string }) {
  return (
    <div className="flex items-center gap-1.5 group relative" title={tooltip}>
      <span className={cn("w-2 h-2 rounded-full", ok ? "bg-emerald-400" : "bg-red-500")} />
      <span className="text-xs text-zinc-500 hidden sm:inline">{label}</span>
    </div>
  );
}
