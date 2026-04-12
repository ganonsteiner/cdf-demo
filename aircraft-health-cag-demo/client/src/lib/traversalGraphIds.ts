import type { AgentEvent, GraphData } from "./types";

/** Strip trailing `(…)` / `[…]` suffixes from traversal payload fragments. */
function stripSuffixes(s: string): string {
  return s.replace(/\(.*$/, "").replace(/\[.*$/, "").trim();
}

/**
 * Extract CDF graph `node.id` values implied by one `[CAG] Traversed:` / SSE traversal line.
 * Shapes mirror `log_traversal(...)` in `src/agent/tools.py` and `context.py`.
 */
export function graphIdsFromTraversalNode(node: string): string[] {
  if (!node || !node.includes(":")) return [];
  const colon = node.indexOf(":");
  const kind = node.slice(0, colon);
  const rest = node.slice(colon + 1).trim();

  const arrowSplit = (s: string): string[] => {
    const a = s.indexOf("→");
    if (a < 0) return [];
    const left = s.slice(0, a).trim();
    const right = s.slice(a + 1).trim();
    return [left, right].filter(Boolean);
  };

  switch (kind) {
    case "Context":
    case "FleetContext":
    case "FleetOverview":
    case "FleetSearch":
    case "PolicyCompliance":
      return [];
    case "Asset":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "AssetSubtree":
    case "AssetSubgraph":
    case "AssetChildren":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "Sensor":
      if (rest.startsWith("latest:")) {
        const ts = rest.slice("latest:".length).trim();
        return ts ? [ts] : [];
      }
      return [];
    case "TimeSeries": {
      const id = rest.split("/")[0].split("(")[0].trim();
      return id ? [id] : [];
    }
    case "Datapoints":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "Events":
    case "Relationships":
      return rest.split("[")[0].trim() ? [rest.split("[")[0].trim()] : [];
    case "Documents":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "File":
      return rest ? [rest] : [];
    case "FleetPolicies":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "Symptoms":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "EngineTypeHistory":
      return stripSuffixes(rest) ? [stripSuffixes(rest)] : [];
    case "IS_TYPE":
    case "HAS_COMPONENT":
      return arrowSplit(rest);
    default:
      return arrowSplit(rest);
  }
}

type TraversalLike = Pick<AgentEvent, "type" | "node">;

/**
 * Traversal steps that correspond to nodes in the current `/api/graph` payload (highlight set).
 */
export function highlightedGraphIdsFromTraversal(
  events: TraversalLike[],
  graphData: GraphData | null
): Set<string> {
  const out = new Set<string>();
  if (!graphData?.nodes?.length) return out;
  const graphIdSet = new Set(graphData.nodes.map((n) => n.id));

  for (const evt of events) {
    if (evt.type !== "traversal" || !evt.node) continue;

    for (const id of graphIdsFromTraversalNode(evt.node)) {
      if (graphIdSet.has(id)) out.add(id);
    }

    if (evt.node.startsWith("Symptoms:")) {
      const tail = stripSuffixes(evt.node.slice("Symptoms:".length));
      if (!tail) continue;
      for (const n of graphData.nodes) {
        if (n.type === "SymptomNode" && n.metadata?.aircraft_id === tail) {
          out.add(n.id);
        }
      }
    }
  }

  return out;
}

/** Counts tool calls, traversal log lines, and distinct graph node IDs (when graph data is loaded). */
export function traversalActivityCounts(
  events: TraversalLike[],
  graphData: GraphData | null
): { toolCount: number; stepCount: number; graphNodeCount: number | null } {
  const toolCount = events.filter((e) => e.type === "tool_call").length;
  const stepCount = events.filter((e) => e.type === "traversal").length;
  if (!graphData?.nodes?.length) {
    return { toolCount, stepCount, graphNodeCount: null };
  }
  return {
    toolCount,
    stepCount,
    graphNodeCount: highlightedGraphIdsFromTraversal(events, graphData).size,
  };
}
