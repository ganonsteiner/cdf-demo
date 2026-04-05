import { useEffect, useState } from "react";
import {
  Plane,
  Activity,
  Wrench,
  MessageSquare,
  GitBranch,
  History,
  Loader2,
  Users,
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

type Tab = "fleet" | "dashboard" | "query" | "maintenance" | "aircraft" | "flights" | "graph";

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("fleet");
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [healthError, setHealthError] = useState(false);
  const { isQuerying, selectedAircraft, setSelectedAircraft } = useStore();

  useEffect(() => {
    api.health()
      .then(setHealth)
      .catch(() => setHealthError(true));
  }, []);

  const handleNavigate = (tab: Tab, tail?: TailNumber) => {
    if (tail) setSelectedAircraft(tail);
    setActiveTab(tab);
  };

  const apiKeyMissing = health ? !health.anthropic_api_key_configured : false;
  const mockCdfOffline = health ? !health.mock_cdf_reachable : false;

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "fleet", label: "Fleet", icon: <Users className="w-4 h-4 shrink-0" /> },
    { id: "dashboard", label: "Aircraft Status", icon: <Activity className="w-4 h-4 shrink-0" /> },
    { id: "aircraft", label: "Components", icon: <Plane className="w-4 h-4 shrink-0" /> },
    { id: "maintenance", label: "Maintenance", icon: <Wrench className="w-4 h-4 shrink-0" /> },
    { id: "flights", label: "Flights", icon: <History className="w-4 h-4 shrink-0" /> },
    {
      id: "query",
      label: "AI Assistant",
      icon: isQuerying ? (
        <Loader2 className="w-4 h-4 shrink-0 animate-spin text-sky-400" />
      ) : (
        <MessageSquare className="w-4 h-4 shrink-0" />
      ),
    },
    { id: "graph", label: "Knowledge Graph", icon: <GitBranch className="w-4 h-4 shrink-0" /> },
  ];

  return (
    <div className="h-screen bg-zinc-950 text-zinc-100 flex flex-col overflow-hidden">
      {/* Header — 56px */}
      <header className="h-14 shrink-0 border-b border-zinc-800 bg-zinc-900/90 backdrop-blur-sm z-50">
        <div className="h-full max-w-screen-2xl mx-auto px-4 sm:px-6 flex items-center justify-between gap-4">
          {/* Left: fleet identity */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="p-2 bg-sky-500/10 rounded-lg border border-sky-500/20">
              <Plane className="w-4 h-4 text-sky-400" />
            </div>
            <div>
              <h1 className="text-sm font-semibold text-zinc-100 tracking-wide">Desert Sky Aviation</h1>
              <p className="text-xs text-zinc-500 hidden sm:block">KPHX · 4 × 1978 Cessna 172N · CAG Demo</p>
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
                  ok={health.mock_cdf_reachable}
                  tooltip={health.mock_cdf_reachable ? `Knowledge graph online — ${(health.store?.assets ?? 0)} assets` : "Mock CDF offline"}
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

      {/* Tab bar — 48px */}
      <div className="h-12 shrink-0 border-b border-zinc-800 bg-zinc-900/70 z-40">
        <div className="h-full max-w-screen-2xl mx-auto px-4 sm:px-6 flex items-end gap-0 overflow-x-auto scrollbar-hide">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                "flex items-center gap-2 px-4 h-full text-sm font-medium border-b-2 whitespace-nowrap transition-colors shrink-0",
                activeTab === tab.id
                  ? "border-sky-500 text-sky-400"
                  : "border-transparent text-zinc-500 hover:text-zinc-300 hover:border-zinc-600"
              )}
            >
              {tab.icon}
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Setup banners */}
      {(apiKeyMissing || mockCdfOffline || healthError) && (
        <SetupBanner
          apiKeyMissing={apiKeyMissing}
          mockCdfOffline={mockCdfOffline}
          connectionError={healthError}
        />
      )}

      {/* Main content — fixed height, no page scroll */}
      <main className="flex-1 overflow-hidden max-w-screen-2xl mx-auto w-full">
        <div className={cn("h-full", activeTab === "fleet" ? "block" : "hidden")}>
          <FleetPage onNavigate={handleNavigate} />
        </div>
        <div className={cn("h-full", activeTab === "dashboard" ? "block" : "hidden")}>
          <StatusDashboard
            selectedAircraft={selectedAircraft}
            onSelectAircraft={setSelectedAircraft}
            onOpenAssistant={() => setActiveTab("query")}
          />
        </div>
        <div className={cn("h-full", activeTab === "query" ? "block" : "hidden")}>
          <QueryInterface apiKeyMissing={apiKeyMissing} />
        </div>
        <div className={cn("h-full", activeTab === "maintenance" ? "block" : "hidden")}>
          <MaintenanceTimeline active={activeTab === "maintenance"} />
        </div>
        <div className={cn("h-full", activeTab === "aircraft" ? "block" : "hidden")}>
          <AircraftComponents active={activeTab === "aircraft"} />
        </div>
        <div className={cn("h-full", activeTab === "flights" ? "block" : "hidden")}>
          <FlightHistory active={activeTab === "flights"} />
        </div>
        <div className={cn("h-full", activeTab === "graph" ? "block" : "hidden")}>
          <KnowledgeGraph active={activeTab === "graph"} />
        </div>
      </main>
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
