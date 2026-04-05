"""
Application API Server — FastAPI on port 3000.

Desert Sky Aviation Fleet CAG Demo endpoints:
  POST /api/query           — SSE-streamed agent responses (body: {question, aircraft?})
  GET  /api/fleet           — all four aircraft status summary
  GET  /api/status          — single aircraft status (?aircraft=N4798E required)
  GET  /api/squawks         — open squawks (?aircraft=N4798E)
  GET  /api/maintenance/upcoming  — upcoming maintenance (?aircraft=N4798E)
  GET  /api/maintenance/history   — paginated history (?aircraft=N4798E)
  GET  /api/flights         — paginated flight records (?aircraft=N4798E)
  GET  /api/components      — component hierarchy with status (?aircraft=N4798E)
  GET  /api/policies        — operational policy list
  GET  /api/graph           — full knowledge graph for visualization
  GET  /api/health          — API key + mock CDF reachability
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

from .agent.agent import run_agent_streaming  # noqa: E402
from .agent.context import assemble_aircraft_context  # noqa: E402

TAILS = ("N4798E", "N2251K", "N8834Q", "N1156P")
DEFAULT_TAIL = "N4798E"

app = FastAPI(
    title="Desert Sky Aviation Fleet CAG Demo",
    description="Fleet knowledge graph query API with CAG-powered agent",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str
    aircraft: Optional[str] = None


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

async def _check_mock_cdf() -> bool:
    base_url = os.getenv("CDF_BASE_URL", "http://localhost:4000")
    try:
        async with httpx.AsyncClient(timeout=2.0) as http:
            resp = await http.get(f"{base_url}/health")
            return resp.status_code == 200
    except Exception:
        return False


def _get_store_counts() -> dict[str, int]:
    try:
        base_url = os.getenv("CDF_BASE_URL", "http://localhost:4000")
        resp = httpx.get(f"{base_url}/health", timeout=2.0)
        if resp.status_code == 200:
            return resp.json().get("store", {})
    except Exception:
        pass
    return {}


@app.get("/api/health")
async def health_check() -> dict[str, Any]:
    """GET /api/health — frontend polls on load to check API key and services."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    key_configured = bool(api_key and not api_key.startswith("sk-ant-...") and len(api_key) > 20)
    mock_cdf_reachable = await _check_mock_cdf()
    store_counts = _get_store_counts()

    if not mock_cdf_reachable:
        status = "mock_cdf_offline"
    elif not api_key:
        status = "api_key_missing"
    elif not key_configured:
        status = "api_key_invalid"
    else:
        status = "ok"

    return {
        "status": status,
        "anthropic_api_key_configured": key_configured,
        "mock_cdf_reachable": mock_cdf_reachable,
        "store": store_counts,
        "checkedAt": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Agent query — SSE streaming
# ---------------------------------------------------------------------------

@app.post("/api/query")
async def query_agent(req: QueryRequest) -> EventSourceResponse:
    """
    POST /api/query — streams agent ReAct steps via Server-Sent Events.
    Body: { question: string, aircraft?: string }
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key.startswith("sk-ant-...") or len(api_key) <= 20:
        raise HTTPException(
            status_code=503,
            detail={"error": "ANTHROPIC_API_KEY not configured", "hint": "Add to backend/.env"},
        )

    async def event_stream() -> AsyncGenerator[dict[str, str], None]:
        async for step in run_agent_streaming(req.question, aircraft_id=req.aircraft):
            yield {"data": json.dumps(step)}

    return EventSourceResponse(event_stream())


# ---------------------------------------------------------------------------
# Fleet overview
# ---------------------------------------------------------------------------

@app.get("/api/fleet")
async def get_fleet() -> list[dict[str, Any]]:
    """GET /api/fleet — aggregate status for all four aircraft."""
    try:
        results = []
        for tail in TAILS:
            ctx = await asyncio.to_thread(assemble_aircraft_context, tail)
            if "error" in ctx:
                # Same shape as success so the UI never crashes on .toFixed / missing keys
                results.append({
                    "tail": tail,
                    "name": tail,
                    "smoh": 0.0,
                    "tbo": 2000,
                    "smohPercent": 0.0,
                    "hobbs": 0.0,
                    "airworthiness": "UNKNOWN",
                    "isAirworthy": False,
                    "openSquawkCount": 0,
                    "groundingSquawkCount": 0,
                    "oilHoursOverdue": 0.0,
                    "annualDaysRemaining": None,
                    "annualDueDate": "",
                    "activeSymptoms": 0,
                    "activeConditions": 0,
                    "metadata": {"load_error": str(ctx.get("error", "unknown"))},
                })
                continue

            results.append({
                "tail": tail,
                "name": ctx.get("aircraft", {}).get("name", tail),
                "smoh": ctx.get("engineSMOH", 0),
                "tbo": ctx.get("engineTBO", 2000),
                "smohPercent": ctx.get("engineSMOHPercent", 0),
                "hobbs": ctx.get("currentHobbs", 0),
                "airworthiness": ctx.get("airworthiness", "UNKNOWN"),
                "isAirworthy": ctx.get("isAirworthy", False),
                "openSquawkCount": len(ctx.get("openSquawks", [])),
                "groundingSquawkCount": len(ctx.get("groundingSquawks", [])),
                "oilHoursOverdue": ctx.get("oilHoursOverdue", 0),
                "annualDaysRemaining": ctx.get("annualDaysRemaining"),
                "annualDueDate": ctx.get("annualDueDate", ""),
                "activeSymptoms": len(ctx.get("symptoms", [])),
                "activeConditions": len(ctx.get("conditions", [])),
                "metadata": ctx.get("aircraft", {}).get("metadata", {}),
            })
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Per-aircraft endpoints (require ?aircraft= query param)
# ---------------------------------------------------------------------------

def _require_tail(aircraft: Optional[str]) -> str:
    if not aircraft:
        aircraft = DEFAULT_TAIL
    if aircraft not in TAILS:
        raise HTTPException(status_code=400, detail=f"Unknown aircraft '{aircraft}'. Valid: {TAILS}")
    return aircraft


@app.get("/api/status")
async def get_aircraft_status(aircraft: Optional[str] = Query(default=None)) -> dict[str, Any]:
    """GET /api/status?aircraft=N4798E — single aircraft health summary."""
    tail = _require_tail(aircraft)
    try:
        ctx = await asyncio.to_thread(assemble_aircraft_context, tail)
        if "error" in ctx:
            raise HTTPException(status_code=503, detail=ctx["error"])

        all_maint = ctx.get("allMaintenance", [])
        last_maint_date: Optional[str] = None
        if all_maint:
            most_recent = max(all_maint, key=lambda x: x.get("startTime") or 0)
            last_maint_date = most_recent.get("metadata", {}).get("date")

        raw_symptoms = ctx.get("symptoms", [])
        symptoms_out: list[dict[str, Any]] = []
        for s in raw_symptoms:
            if not isinstance(s, dict):
                continue
            symptoms_out.append({
                "externalId": s.get("externalId", ""),
                "aircraftId": s.get("aircraft_id", tail),
                "title": s.get("title", ""),
                "description": s.get("description", ""),
                "observation": s.get("observation", ""),
                "severity": s.get("severity", "caution"),
                "firstObserved": s.get("first_observed", ""),
                "type": s.get("type", "SymptomNode"),
            })

        return {
            "tail": tail,
            "hobbs": ctx.get("currentHobbs", 0),
            "tach": ctx.get("currentTach", 0),
            "engineSMOH": ctx.get("engineSMOH", 0),
            "engineTBO": ctx.get("engineTBO", 2000),
            "engineSMOHPercent": ctx.get("engineSMOHPercent", 0),
            "annualDueDate": ctx.get("annualDueDate", ""),
            "annualDaysRemaining": ctx.get("annualDaysRemaining"),
            "openSquawkCount": len(ctx.get("openSquawks", [])),
            "groundingSquawkCount": len(ctx.get("groundingSquawks", [])),
            "airworthiness": ctx.get("airworthiness", "UNKNOWN"),
            "isAirworthy": ctx.get("isAirworthy", False),
            "oilHoursOverdue": ctx.get("oilHoursOverdue", 0),
            "oilNextDueHobbs": ctx.get("oilNextDueHobbs", 0),
            "lastMaintenanceDate": last_maint_date,
            "activeSymptoms": len(symptoms_out),
            "symptoms": symptoms_out,
            "dataFreshAt": datetime.now(timezone.utc).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/squawks")
async def get_squawks(aircraft: Optional[str] = Query(default=None)) -> list[dict[str, Any]]:
    """GET /api/squawks?aircraft=N4798E — open squawks for one aircraft."""
    tail = _require_tail(aircraft)
    try:
        ctx = await asyncio.to_thread(assemble_aircraft_context, tail)
        if "error" in ctx:
            raise HTTPException(status_code=503, detail=ctx["error"])

        result = []
        for sq in ctx.get("allSquawks", []):
            meta = sq.get("metadata", {})
            result.append({
                "externalId": sq.get("externalId", ""),
                "description": sq.get("description", ""),
                "component": meta.get("component_id", ""),
                "severity": meta.get("severity", "non-grounding"),
                "status": meta.get("status", "open"),
                "dateIdentified": meta.get("date", ""),
                "tail": meta.get("tail", tail),
                "metadata": meta,
            })
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/maintenance/upcoming")
async def get_upcoming_maintenance(aircraft: Optional[str] = Query(default=None)) -> list[dict[str, Any]]:
    """GET /api/maintenance/upcoming?aircraft=N4798E"""
    tail = _require_tail(aircraft)
    try:
        ctx = await asyncio.to_thread(assemble_aircraft_context, tail)
        if "error" in ctx:
            raise HTTPException(status_code=503, detail=ctx["error"])
        return ctx.get("upcomingMaintenance", [])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/maintenance/history")
async def get_maintenance_history(
    aircraft: Optional[str] = Query(default=None),
    page: int = 1,
    per_page: int = 25,
    component: Optional[str] = None,
    year: Optional[int] = None,
    maint_type: Optional[str] = None,
) -> dict[str, Any]:
    """GET /api/maintenance/history?aircraft=N4798E — paginated maintenance records."""
    tail = _require_tail(aircraft)
    try:
        ctx = await asyncio.to_thread(assemble_aircraft_context, tail)
        if "error" in ctx:
            raise HTTPException(status_code=503, detail=ctx["error"])

        records = ctx.get("allMaintenance", []) + ctx.get("allInspections", [])
        records_sorted = sorted(records, key=lambda x: x.get("startTime") or 0, reverse=True)

        if component:
            comp_lower = component.lower()
            records_sorted = [
                r for r in records_sorted
                if comp_lower in (r.get("metadata", {}).get("component_id", "") or "").lower()
            ]
        if year:
            records_sorted = [r for r in records_sorted if _record_year(r) == year]
        if maint_type:
            mt_lower = maint_type.lower()
            records_sorted = [
                r for r in records_sorted
                if mt_lower in (r.get("subtype") or "").lower()
                or mt_lower in (r.get("metadata", {}).get("maintenance_type", "") or "").lower()
            ]

        total = len(records_sorted)
        total_pages = max(1, (total + per_page - 1) // per_page)
        start = (page - 1) * per_page

        return {
            "records": records_sorted[start:start + per_page],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _record_year(record: dict[str, Any]) -> Optional[int]:
    date_str = record.get("metadata", {}).get("date", "")
    if date_str:
        try:
            return int(date_str[:4])
        except (ValueError, TypeError):
            pass
    return None


@app.get("/api/flights")
async def get_flights(
    aircraft: Optional[str] = Query(default=None),
    page: int = 1,
    per_page: int = 25,
    route: Optional[str] = None,
    year: Optional[int] = None,
) -> dict[str, Any]:
    """GET /api/flights?aircraft=N4798E — paginated flight records."""
    tail = _require_tail(aircraft)
    try:
        return await asyncio.to_thread(_sync_get_flights, tail, page, per_page, route, year)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _sync_get_flights(
    tail: str,
    page: int,
    per_page: int,
    route: Optional[str],
    year: Optional[int],
) -> dict[str, Any]:
    from mock_cdf.store.store import store as cdf_store  # type: ignore[import]

    # Flight events have all the per-flight data we need
    all_events = cdf_store.get_events()
    flight_events = [
        e for e in all_events
        if e.type == "Flight" and (e.metadata or {}).get("tail") == tail
    ]

    flights: list[dict[str, Any]] = []
    for e in flight_events:
        meta = e.metadata or {}
        ts_ms = e.startTime or 0
        ts_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat() if ts_ms else ""
        flight_year = int(ts_iso[:4]) if ts_iso else 0

        try:
            hobbs_start = float(meta.get("hobbs_start", 0))
            hobbs_end = float(meta.get("hobbs_end", 0))
        except (ValueError, TypeError):
            hobbs_start = hobbs_end = 0.0

        try:
            duration = float(meta.get("duration", 0))
        except (ValueError, TypeError):
            duration = round(hobbs_end - hobbs_start, 2)

        def _f(key: str) -> Optional[float]:
            v = meta.get(key)
            if v and v != "nan":
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
            return None

        flights.append({
            "timestamp": ts_iso,
            "hobbs_start": hobbs_start,
            "hobbs_end": hobbs_end,
            "duration": duration,
            "route": meta.get("route", ""),
            "cht_max": _f("cht_max"),
            "egt_max": None,
            "oil_pressure_min": None,
            "oil_pressure_max": None,
            "oil_temp_max": _f("oil_temp_max"),
            "fuel_used_gal": None,
            "pilot_notes": meta.get("pilot_notes", ""),
            "anomalous": meta.get("anomalous", "") == "true",
            "year": flight_year,
        })

    # Apply filters
    filtered = flights
    if year:
        filtered = [f for f in filtered if f["year"] == year]
    if route:
        route_lower = route.lower()
        filtered = [f for f in filtered if route_lower in (f.get("route") or "").lower()]

    filtered = sorted(filtered, key=lambda f: f["timestamp"], reverse=True)

    total = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    start = (page - 1) * per_page

    return {
        "records": filtered[start:start + per_page],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


@app.get("/api/components")
async def get_components(aircraft: Optional[str] = Query(default=None)) -> list[dict[str, Any]]:
    """GET /api/components?aircraft=N4798E — asset hierarchy with maintenance status."""
    tail = _require_tail(aircraft)
    try:
        return await asyncio.to_thread(_sync_get_components, tail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _sync_get_components(tail: str) -> list[dict[str, Any]]:
    from mock_cdf.store.store import store as cdf_store  # type: ignore[import]

    # Get only assets that belong to this tail
    all_assets = cdf_store.get_assets()
    tail_assets = [
        a for a in all_assets
        if a.externalId == tail
        or (a.externalId or "").startswith(f"{tail}-")
    ]

    all_events = cdf_store.get_events()

    # Maintenance lookup by assetId
    maint_by_asset: dict[int, list[Any]] = {}
    for event in all_events:
        if event.type in ("MaintenanceRecord", "Inspection"):
            for aid in event.assetIds or []:
                maint_by_asset.setdefault(aid, []).append(event)

    # Current hobbs for this tail
    hobbs_ts_id = f"{tail}.aircraft.hobbs"
    hobbs_dps = cdf_store.get_datapoints(hobbs_ts_id)
    current_hobbs = max((dp.value for dp in hobbs_dps), default=0.0) if hobbs_dps else 0.0

    today = datetime.now(timezone.utc)
    result: list[dict[str, Any]] = []

    for asset in sorted(tail_assets, key=lambda a: a.externalId or ""):
        maint_records = sorted(
            maint_by_asset.get(asset.id, []),
            key=lambda e: e.startTime or 0,
            reverse=True,
        )

        last_maint_date: Optional[str] = None
        next_due_hobbs: Optional[float] = None
        next_due_date: Optional[str] = None

        if maint_records:
            last_maint_date = (maint_records[0].metadata or {}).get("date")
            for rec in maint_records:
                meta = rec.metadata or {}
                ndh = meta.get("next_due_hobbs")
                if ndh:
                    try:
                        next_due_hobbs = float(ndh)
                        next_due_date = meta.get("next_due_date")
                        break
                    except (ValueError, TypeError):
                        pass

        status = "ok"
        if next_due_hobbs is not None and current_hobbs > 0:
            hours_remaining = next_due_hobbs - current_hobbs
            if hours_remaining < 0:
                status = "overdue"
            elif hours_remaining <= 10:
                status = "due_soon"
        if next_due_date:
            try:
                due_dt = datetime.fromisoformat(next_due_date.replace("Z", "+00:00"))
                if due_dt.tzinfo is None:
                    due_dt = due_dt.replace(tzinfo=timezone.utc)
                days_remaining = (due_dt - today).days
                if days_remaining < 0:
                    status = "overdue"
                elif days_remaining <= 30 and status == "ok":
                    status = "due_soon"
            except (ValueError, TypeError):
                pass

        result.append({
            "externalId": asset.externalId,
            "name": asset.name,
            "description": asset.description,
            "parentExternalId": asset.parentExternalId,
            "metadata": asset.metadata or {},
            "lastMaintenanceDate": last_maint_date,
            "nextDueHobbs": next_due_hobbs,
            "nextDueDate": next_due_date,
            "currentHobbs": current_hobbs,
            "hoursUntilDue": round(next_due_hobbs - current_hobbs, 1) if next_due_hobbs and current_hobbs > 0 else None,
            "status": status,
            "maintenanceCount": len(maint_records),
        })

    return result


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------

@app.get("/api/policies")
async def get_policies() -> list[dict[str, Any]]:
    """GET /api/policies — list all fleet operational policies."""
    try:
        base_url = os.getenv("CDF_BASE_URL", "http://localhost:4000")
        project = os.getenv("CDF_PROJECT", "desert_sky")
        async with httpx.AsyncClient(timeout=5.0) as http:
            resp = await http.post(
                f"{base_url}/api/v1/projects/{project}/policies/list",
                json={},
            )
            resp.raise_for_status()
            return resp.json().get("items", [])
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Knowledge graph
# ---------------------------------------------------------------------------

@app.get("/api/graph")
async def get_graph_data() -> dict[str, Any]:
    """GET /api/graph — full knowledge graph for visualization."""
    try:
        return await asyncio.to_thread(_sync_get_graph_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


ENGINE_MODEL_GRAPH_ID = "ENGINE_MODEL_LYC_O320_H2AD"

# Node type color groups for the frontend
_NODE_COLORS = {
    "asset": 1,
    "timeseries": 2,
    "event": 3,
    "file": 4,
    "SymptomNode": 5,
    "EngineModel": 6,
    "OperationalPolicy": 7,
    "FleetOwner": 8,
}

# Relationship type colors for edges
_EDGE_COLORS = {
    "HAS_COMPONENT": "#4B9CD3",
    "HAS_TIMESERIES": "#2E8B57",
    "GOVERNED_BY": "#9B59B6",
    "HAS_POLICY": "#9B59B6",
    "EXHIBITED": "#E67E22",
    "IS_TYPE": "#e0f2fe",
    "PERFORMED_ON": "#95A5A6",
    "REFERENCES_AD": "#F39C12",
    "IDENTIFIED_ON": "#E67E22",
    "LINKED_TO": "#1ABC9C",
}


def _sync_get_graph_data() -> dict[str, Any]:
    from mock_cdf.store.store import store as cdf_store  # type: ignore[import]

    nodes: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()
    seen_links: set[tuple[str, str, str]] = set()

    def _add_node(node_id: str, label: str, node_type: str, meta: dict[str, Any] | None = None) -> None:
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            nodes.append({
                "id": node_id,
                "label": label,
                "type": node_type,
                "group": _NODE_COLORS.get(node_type, 1),
                "metadata": meta or {},
            })

    def _add_link(src: str, tgt: str, rel_type: str) -> None:
        key = (src, tgt, rel_type)
        if key not in seen_links and src in seen_nodes and tgt in seen_nodes:
            seen_links.add(key)
            links.append({
                "source": src,
                "target": tgt,
                "type": rel_type,
                "color": _EDGE_COLORS.get(rel_type, "#666"),
            })

    # Assets (engine model is a distinct node type for visualization)
    for asset in cdf_store.get_assets():
        node_id = asset.externalId or str(asset.id)
        meta = asset.metadata or {}
        node_type = "EngineModel" if node_id == ENGINE_MODEL_GRAPH_ID or meta.get("type") == "EngineModel" else "asset"
        _add_node(node_id, asset.name or node_id, node_type, meta)

    # TimeSeries
    for ts in cdf_store.get_timeseries():
        node_id = ts.externalId or str(ts.id)
        _add_node(node_id, ts.name or node_id, "timeseries", {"unit": ts.unit or ""})

    # Fleet extended resources
    for sym in cdf_store.get_symptoms():
        _add_node(sym.externalId, sym.title, "SymptomNode", {"aircraft_id": sym.aircraft_id, "severity": sym.severity})

    for pol in cdf_store.get_policies():
        _add_node(pol.externalId, pol.title, "OperationalPolicy", {"category": pol.category})

    for fo in cdf_store.get_fleet_owners():
        _add_node(fo.externalId, fo.name, "FleetOwner", {"location": fo.location})

    # Files
    for f in cdf_store.get_files():
        node_id = f.externalId or str(f.id)
        _add_node(node_id, f.name or node_id, "file")

    # Relationships → links
    for rel in cdf_store.get_relationships():
        src = rel.sourceExternalId
        tgt = rel.targetExternalId
        if src and tgt and src in seen_nodes and tgt in seen_nodes:
            _add_link(src, tgt, rel.relationshipType or "RELATED_TO")

    # Asset parent→child links from parentExternalId field
    for asset in cdf_store.get_assets():
        if asset.parentExternalId:
            _add_link(asset.parentExternalId, asset.externalId or str(asset.id), "HAS_COMPONENT")

    # TS → asset links
    for ts in cdf_store.get_timeseries():
        node_id = ts.externalId or str(ts.id)
        if ts.assetId:
            for asset in cdf_store.get_assets():
                if asset.id == ts.assetId:
                    _add_link(asset.externalId or str(asset.id), node_id, "HAS_TIMESERIES")
                    break

    link_counts: dict[str, int] = {}
    for link in links:
        link_counts[link["source"]] = link_counts.get(link["source"], 0) + 1
        link_counts[link["target"]] = link_counts.get(link["target"], 0) + 1
    for node in nodes:
        node["linkCount"] = link_counts.get(node["id"], 0)

    return {
        "nodes": nodes,
        "links": links,
        "stats": {
            "assets": sum(1 for n in nodes if n["type"] == "asset"),
            "timeseries": sum(1 for n in nodes if n["type"] == "timeseries"),
            "symptoms": sum(1 for n in nodes if n["type"] == "SymptomNode"),
            "engine_models": sum(1 for n in nodes if n["type"] == "EngineModel"),
            "policies": sum(1 for n in nodes if n["type"] == "OperationalPolicy"),
            "fleet_owners": sum(1 for n in nodes if n["type"] == "FleetOwner"),
            "files": sum(1 for n in nodes if n["type"] == "file"),
            "relationships": len(links),
        },
    }


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    key_ok = bool(api_key and not api_key.startswith("sk-ant-...") and len(api_key) > 20)
    mock_cdf_ok = await _check_mock_cdf()

    print("\n✈  Desert Sky Aviation Fleet CAG API — port 3000")
    print(f"   ANTHROPIC_API_KEY: {'✓ configured' if key_ok else '✗ MISSING — add to backend/.env'}")
    print(f"   Mock CDF server:   {'✓ reachable' if mock_cdf_ok else '✗ not reachable'}")
    print("   Fleet: N4798E (AIRWORTHY) N2251K (FERRY) N8834Q (CAUTION) N1156P (GROUNDED)\n")
