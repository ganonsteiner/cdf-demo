"""
Application API Server — FastAPI on port 8080.

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
from .agent.context import (  # noqa: E402
    _date_after_calendar_months,
    _oil_change_calendar_months_from_policy,
    assemble_aircraft_context,
)
from .date_only import calendar_days_until_iso  # noqa: E402
from .aircraft_times import (  # noqa: E402
    current_hobbs_from_cdf_store,
    current_tach_from_cdf_store,
    next_due_tach_from_meta,
)

TAILS = ("N4798E", "N2251K", "N8834Q", "N1156P")
DEFAULT_TAIL = "N4798E"

app = FastAPI(
    title="Desert Sky Aviation Fleet CAG Demo",
    description="Fleet knowledge graph query API with CAG-powered agent",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4000",
        "http://127.0.0.1:4000",
    ],
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
    base_url = os.getenv("CDF_BASE_URL", "http://localhost:4001")
    try:
        async with httpx.AsyncClient(timeout=2.0) as http:
            resp = await http.get(f"{base_url}/health")
            return resp.status_code == 200
    except Exception:
        return False


def _mock_cdf_fleet_ready_sync() -> bool:
    """
    True only if we can retrieve a fleet aircraft via the same byids path the SDK uses.

    Port 4001 may be occupied by a non-mock process that still returns HTTP 200 on /health,
    which would make _check_mock_cdf True while assets.retrieve returns None.
    """
    base_url = os.getenv("CDF_BASE_URL", "http://localhost:4001").rstrip("/")
    project = os.getenv("CDF_PROJECT", "desert_sky")
    try:
        h = httpx.get(f"{base_url}/health", timeout=2.0)
        if h.status_code != 200:
            return False
        store = h.json().get("store") or {}
        if int(store.get("assets", 0) or 0) >= 4:
            return True
        url = f"{base_url}/api/v1/projects/{project}/assets/byids"
        r = httpx.post(
            url,
            json={"items": [{"externalId": "N4798E"}]},
            headers={"Content-Type": "application/json"},
            timeout=3.0,
        )
        if r.status_code != 200:
            return False
        items = r.json().get("items") or []
        return len(items) > 0
    except Exception:
        return False


async def _mock_cdf_fleet_ready() -> bool:
    return await asyncio.to_thread(_mock_cdf_fleet_ready_sync)


def _get_store_counts() -> dict[str, int]:
    try:
        base_url = os.getenv("CDF_BASE_URL", "http://localhost:4001")
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
    mock_cdf_fleet_ready = await _mock_cdf_fleet_ready() if mock_cdf_reachable else False
    store_counts = _get_store_counts()

    if not mock_cdf_reachable:
        status = "mock_cdf_offline"
    elif not mock_cdf_fleet_ready:
        status = "degraded"
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
        "mock_cdf_fleet_ready": mock_cdf_fleet_ready,
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
            detail={"error": "ANTHROPIC_API_KEY not configured", "hint": "Add to .env in project root"},
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
                    "tach": 0.0,
                    "airworthiness": "UNKNOWN",
                    "isAirworthy": False,
                    "openSquawkCount": 0,
                    "groundingSquawkCount": 0,
                    "oilHoursOverdue": 0.0,
                    "oilTachHoursOverdue": 0.0,
                    "oilTachHoursUntilDue": 0.0,
                    "oilDaysUntilDue": None,
                    "annualDaysRemaining": None,
                    "annualDueDate": "",
                    "lastMaintenanceDate": None,
                    "metadata": {"load_error": str(ctx.get("error", "unknown"))},
                })
                continue

            all_maint = ctx.get("allMaintenance", [])
            last_maint_date: Optional[str] = None
            if all_maint:
                most_recent = max(all_maint, key=lambda x: x.get("startTime") or 0)
                last_maint_date = most_recent.get("metadata", {}).get("date")

            results.append({
                "tail": tail,
                "name": ctx.get("aircraft", {}).get("name", tail),
                "smoh": ctx.get("engineSMOH", 0),
                "tbo": ctx.get("engineTBO", 2000),
                "smohPercent": ctx.get("engineSMOHPercent", 0),
                "hobbs": ctx.get("currentHobbs", 0),
                "tach": ctx.get("currentTach", 0),
                "airworthiness": ctx.get("airworthiness", "UNKNOWN"),
                "isAirworthy": ctx.get("isAirworthy", False),
                "openSquawkCount": len(ctx.get("openSquawks", [])),
                "groundingSquawkCount": len(ctx.get("groundingSquawks", [])),
                "oilHoursOverdue": ctx.get("oilTachHoursOverdue", ctx.get("oilHoursOverdue", 0)),
                "oilTachHoursOverdue": ctx.get("oilTachHoursOverdue", 0),
                "oilTachHoursUntilDue": ctx.get("oilTachHoursUntilDue", 0),
                "oilDaysUntilDue": ctx.get("oilDaysUntilDue"),
                "annualDaysRemaining": ctx.get("annualDaysRemaining"),
                "annualDueDate": ctx.get("annualDueDate", ""),
                "lastMaintenanceDate": last_maint_date,
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
            "oilHoursOverdue": ctx.get("oilTachHoursOverdue", ctx.get("oilHoursOverdue", 0)),
            "oilTachHoursOverdue": ctx.get("oilTachHoursOverdue", 0),
            "oilTachHoursUntilDue": ctx.get("oilTachHoursUntilDue", 0),
            "oilNextDueTach": ctx.get("oilNextDueTach", 0),
            "oilNextDueDate": ctx.get("oilNextDueDate", ""),
            "oilDaysUntilDue": ctx.get("oilDaysUntilDue"),
            "oilNextDueHobbs": ctx.get("oilNextDueTach", ctx.get("oilNextDueHobbs", 0)),
            "lastMaintenanceDate": last_maint_date,
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
    """GET /api/maintenance/history?aircraft=N4798E — paginated maintenance records.

    Response includes ``available_years``: calendar years present after component/type
    filters (before ``year``), for populating the year filter UI.
    """
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

        years_seen: set[int] = set()
        for r in records_sorted:
            y = _record_year(r)
            if y is not None:
                years_seen.add(y)
        available_years = sorted(years_seen, reverse=True)

        if year:
            records_sorted = [r for r in records_sorted if _record_year(r) == year]

        total = len(records_sorted)
        total_pages = max(1, (total + per_page - 1) // per_page)
        start = (page - 1) * per_page

        return {
            "records": records_sorted[start:start + per_page],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "available_years": available_years,
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


_FLIGHT_SORT_FIELDS = frozenset({
    "timestamp",
    "duration",
    "route",
    "cht_max",
    "oil_temp_max",
    "oil_pressure_min",
    "oil_pressure_max",
    "egt_max",
    "fuel_used_gal",
})


@app.get("/api/flights")
async def get_flights(
    aircraft: Optional[str] = Query(default=None),
    page: int = 1,
    per_page: int = 25,
    route: Optional[str] = None,
    year: Optional[int] = None,
    sort: str = Query(default="timestamp"),
    order: str = Query(default="desc"),
) -> dict[str, Any]:
    """GET /api/flights?aircraft=N4798E — paginated flight records."""
    tail = _require_tail(aircraft)
    try:
        return await asyncio.to_thread(_sync_get_flights, tail, page, per_page, route, year, sort, order)
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
    sort: str,
    order: str,
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

        def _f_optional(key: str) -> Optional[float]:
            """Float from metadata when present; None if missing (e.g. tach before re-ingest)."""
            v = meta.get(key)
            if v is None or v == "" or v == "nan":
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        op_min = _f("oil_pressure_min")
        op_max = _f("oil_pressure_max")
        if op_min is not None and op_max is not None and op_min > op_max:
            op_min, op_max = op_max, op_min

        flights.append({
            "timestamp": ts_iso,
            "hobbs_start": hobbs_start,
            "hobbs_end": hobbs_end,
            "tach_start": _f_optional("tach_start"),
            "tach_end": _f_optional("tach_end"),
            "duration": duration,
            "route": meta.get("route", ""),
            "cht_max": _f("cht_max"),
            "egt_max": _f("egt_max"),
            "oil_pressure_min": op_min,
            "oil_pressure_max": op_max,
            "oil_temp_max": _f("oil_temp_max"),
            "fuel_used_gal": _f("fuel_used_gal"),
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

    sort_key = sort if sort in _FLIGHT_SORT_FIELDS else "timestamp"
    descending = (order or "desc").lower() != "asc"

    def _flight_sort_tuple(f: dict[str, Any]) -> tuple[Any, ...]:
        """Primary sort plus timestamp tie-breaker (ISO strings sort chronologically)."""
        ts = f.get("timestamp") or ""
        if sort_key == "timestamp":
            return (ts,)
        if sort_key == "route":
            return ((f.get("route") or "").lower(), ts)
        v = f.get(sort_key)
        if v is None:
            num = float("-inf") if descending else float("inf")
        else:
            try:
                num = float(v)
            except (TypeError, ValueError):
                num = float("-inf") if descending else float("inf")
        return (num, ts)

    filtered.sort(key=_flight_sort_tuple, reverse=descending)

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

    all_assets = cdf_store.get_assets()
    tail_assets = [
        a for a in all_assets
        if a.externalId == tail
        or (a.externalId or "").startswith(f"{tail}-")
    ]

    all_events = cdf_store.get_events()

    maint_by_asset: dict[int, list[Any]] = {}
    for event in all_events:
        if event.type in ("MaintenanceRecord", "Inspection"):
            for aid in event.assetIds or []:
                maint_by_asset.setdefault(aid, []).append(event)

    current_hobbs = current_hobbs_from_cdf_store(cdf_store, tail)
    current_tach = current_tach_from_cdf_store(cdf_store, tail)

    result: list[dict[str, Any]] = []

    for asset in sorted(tail_assets, key=lambda a: a.externalId or ""):
        ext_id = asset.externalId or ""
        raw_records = maint_by_asset.get(asset.id, [])
        maint_records = [
            e for e in raw_records
            if (e.metadata or {}).get("component_id", "") == ext_id
        ]
        maint_records = sorted(maint_records, key=lambda e: e.startTime or 0, reverse=True)

        last_maint_date: Optional[str] = None
        next_due_tach: Optional[float] = None
        next_due_date: Optional[str] = None
        oil_next_due_tach: Optional[float] = None
        oil_next_due_date: Optional[str] = None

        if maint_records:
            last_maint_date = (maint_records[0].metadata or {}).get("date")

        is_engine = ext_id == f"{tail}-ENGINE"
        is_root = ext_id == tail

        if is_engine:
            oil_recs = [
                e for e in maint_records
                if e.type == "MaintenanceRecord"
                and "oil_change" in (e.subtype or "").lower()
            ]
            if oil_recs:
                om = oil_recs[0].metadata or {}
                oil_next_due_tach = next_due_tach_from_meta(om)
                oil_next_due_date = (om.get("next_due_date") or "").strip() or None
                next_due_tach = oil_next_due_tach
                next_due_date = oil_next_due_date
                # IT rows often omit next_due_date; calendar leg still applies (policy months from sign-off).
                if not oil_next_due_date:
                    svc = str(om.get("date", "") or "").strip()
                    if svc:
                        try:
                            oil_next_due_date = _date_after_calendar_months(
                                svc, _oil_change_calendar_months_from_policy()
                            )
                            next_due_date = oil_next_due_date
                        except ValueError:
                            pass

        if is_root:
            annual_recs = [
                e for e in maint_records
                if e.type == "Inspection" and (e.subtype or "").lower() == "annual"
            ]
            if annual_recs:
                am = annual_recs[0].metadata or {}
                next_due_date = (am.get("next_due_date") or "").strip() or None
                next_due_tach = None

        status = "ok"
        hours_until_tach: Optional[float] = None

        if is_engine and next_due_tach is not None and current_tach > 0:
            hours_until_tach = round(next_due_tach - current_tach, 1)
            if hours_until_tach < 0:
                status = "overdue"
            elif hours_until_tach <= 10:
                status = "due_soon"
            if oil_next_due_date:
                days_remaining = calendar_days_until_iso(oil_next_due_date)
                if days_remaining is not None:
                    if days_remaining < 0 and hours_until_tach is not None and hours_until_tach > 0:
                        pass
                    elif days_remaining < 0:
                        status = "overdue"
                    elif days_remaining <= 30 and status == "ok":
                        status = "due_soon"

        if is_root and next_due_date:
            days_remaining = calendar_days_until_iso(next_due_date)
            if days_remaining is not None:
                if days_remaining < 0:
                    status = "overdue"
                elif days_remaining <= 30 and status == "ok":
                    status = "due_soon"

        result.append({
            "externalId": ext_id,
            "name": asset.name,
            "description": asset.description,
            "parentExternalId": asset.parentExternalId,
            "metadata": asset.metadata or {},
            "lastMaintenanceDate": last_maint_date,
            "nextDueTach": next_due_tach,
            "nextDueHobbs": next_due_tach,
            "nextDueDate": next_due_date,
            "currentHobbs": current_hobbs,
            "currentTach": current_tach,
            "hoursUntilDue": hours_until_tach,
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
        base_url = os.getenv("CDF_BASE_URL", "http://localhost:4001")
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


# Node type color groups for the frontend
_NODE_COLORS = {
    "asset": 1,
    "timeseries": 2,
    "event": 3,
    "file": 4,
    "OperationalPolicy": 7,
}

# Relationship type colors — same hex as the node type each edge relates to.
# LINKED_TO bridges doc↔asset with no single owner, so it uses a neutral zinc.
_EDGE_COLORS = {
    "HAS_COMPONENT":  "#38bdf8",  # sky-400    — same as asset nodes
    "GOVERNED_BY":    "#818cf8",  # indigo-400 — same as FleetOwner nodes
    "HAS_POLICY":     "#f472b6",  # pink-400   — same as OperationalPolicy nodes
    "HAS_TIMESERIES": "#34d399",  # emerald-400 — same as timeseries nodes
    "IS_TYPE":        "#38bdf8",  # sky-400    — same as asset nodes
    "PERFORMED_ON":   "#fb923c",  # orange-400 — same as event nodes
    "IDENTIFIED_ON":  "#fb923c",  # orange-400 — same as event nodes
    "REFERENCES_AD":  "#c084fc",  # purple-400 — same as file nodes
    "LINKED_TO":      "#71717a",  # zinc-500   — neutral, no single node owner
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

    # Assets (all rendered as "asset", including the engine model node)
    for asset in cdf_store.get_assets():
        node_id = asset.externalId or str(asset.id)
        meta = asset.metadata or {}
        _add_node(node_id, asset.name or node_id, "asset", meta)

    # TimeSeries
    for ts in cdf_store.get_timeseries():
        node_id = ts.externalId or str(ts.id)
        _add_node(node_id, ts.name or node_id, "timeseries", {"unit": ts.unit or ""})

    for pol in cdf_store.get_policies():
        _add_node(pol.externalId, pol.title, "OperationalPolicy", {"category": pol.category})

    # Files
    for f in cdf_store.get_files():
        node_id = f.externalId or str(f.id)
        _add_node(node_id, f.name or node_id, "file")

    # Maintenance / squawk / inspection events that participate in REFERENCES_AD or PERFORMED_ON
    graph_event_external_ids: set[str] = set()
    for rel in cdf_store.get_relationships():
        if rel.relationshipType in ("REFERENCES_AD", "PERFORMED_ON") and rel.sourceType == "event":
            graph_event_external_ids.add(rel.sourceExternalId)

    for ev in cdf_store.get_events():
        if ev.externalId not in graph_event_external_ids:
            continue
        if ev.type == "Flight":
            continue
        sub = (ev.subtype or ev.type or "event").strip()
        desc = (ev.description or "").strip()
        body = desc[:40] + ("…" if len(desc) > 40 else "")
        label = f"{sub} {body}".strip()[:60]
        if not label:
            label = ev.externalId
        _add_node(
            ev.externalId,
            label,
            "event",
            {"eventType": ev.type or "", "subtype": sub, "tail": (ev.metadata or {}).get("tail", "")},
        )

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
            "events": sum(1 for n in nodes if n["type"] == "event"),
            "policies": sum(1 for n in nodes if n["type"] == "OperationalPolicy"),
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
    fleet_ready = await _mock_cdf_fleet_ready() if mock_cdf_ok else False

    print("\n✈  Desert Sky Aviation Fleet CAG API — port 8080")
    print(f"   ANTHROPIC_API_KEY: {'✓ configured' if key_ok else '✗ MISSING — add ANTHROPIC_API_KEY to .env'}")
    print(f"   Mock CDF server:   {'✓ reachable' if mock_cdf_ok else '✗ not reachable'}")
    if mock_cdf_ok and not fleet_ready:
        print(
            "   ⚠ Mock /health OK but fleet assets missing — port 4001 may be another app.\n"
            "     Stop the process on 4001 and restart so `npm run mock-cdf` can bind."
        )
    print("   Fleet: N4798E  N2251K  N8834Q  N1156P  (airworthiness derived at query time)\n")
