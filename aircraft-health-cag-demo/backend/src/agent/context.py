"""
CAG Context Assembly — Desert Sky Aviation Fleet.

assemble_aircraft_context(aircraft_id) builds structured context for one
aircraft by traversing the knowledge graph. Used by /api/status and the
agent's assemble_aircraft_context tool.

Context is assembled by graph traversal only — no vector store, no embeddings.
This mirrors exactly how Cognite's Atlas AI works.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import Token
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from .tools import (  # noqa: E402
    client,
    log_traversal,
    clear_traversal_log,
    get_linked_documents,
    symptom_fleet_deep_dive,
    _cdf_post,
    _CDF_PROJECT,
    _CDF_BASE_URL,
)

_NOW = datetime.now(timezone.utc)

TAILS = ("N4798E", "N2251K", "N8834Q", "N1156P")


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _days_until(date_str: str) -> Optional[int]:
    if not date_str:
        return None
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (target - _NOW).days
    except ValueError:
        return None


def _get_latest_sensor(tail: str, suffix: str) -> Optional[float]:
    """Retrieve latest value for a per-tail time series."""
    ts_ext_id = f"{tail}.{suffix}"
    try:
        dp = client.time_series.data.retrieve_latest(external_id=ts_ext_id)
        if dp and len(dp) > 0:
            return float(dp[0].value)
    except Exception:
        pass
    return None


def derive_upcoming_maintenance(
    all_events: list[dict[str, Any]],
    current_hobbs: float,
    window_hours: float = 250.0,
    overdue_lookback: float = 500.0,
) -> list[dict[str, Any]]:
    """
    Find maintenance items due within the next window_hours hobbs hours.
    Uses most-recent record per component:maintenance_type key.
    """
    best: dict[str, tuple[int, dict[str, Any]]] = {}
    for event in all_events:
        meta = event.get("metadata", {})
        component = meta.get("component_id", "")
        maint_type = meta.get("maintenance_type", event.get("subtype", ""))
        key = f"{component}:{maint_type}"
        next_due_str = meta.get("next_due_hobbs", "")
        if not next_due_str:
            continue
        try:
            float(next_due_str)
        except ValueError:
            continue
        start_time = event.get("startTime") or 0
        existing = best.get(key)
        if existing is None or start_time > existing[0]:
            best[key] = (start_time, event)

    upcoming = []
    for _, (_, event) in best.items():
        meta = event.get("metadata", {})
        next_due = float(meta.get("next_due_hobbs", 0))
        component = meta.get("component_id", "")
        maint_type = meta.get("maintenance_type", event.get("subtype", ""))
        hours_until = next_due - current_hobbs
        if -overdue_lookback <= hours_until <= window_hours:
            upcoming.append({
                "component": component,
                "description": event.get("description", maint_type),
                "maintenanceType": maint_type,
                "nextDueHobbs": next_due,
                "hoursUntilDue": round(hours_until, 1),
                "isOverdue": hours_until < 0,
                "nextDueDate": meta.get("next_due_date", ""),
                "daysUntilDue": _days_until(meta.get("next_due_date", "")),
            })
    return sorted(upcoming, key=lambda x: x["hoursUntilDue"])


def assemble_aircraft_context(aircraft_id: str) -> dict[str, Any]:
    """
    Full CAG context assembly for one aircraft by traversing the knowledge graph.

    Used by /api/status?aircraft={tail} and the agent's assemble_aircraft_context tool.
    """
    clear_traversal_log()
    log_traversal(f"Context:{aircraft_id}(start)")

    # 1. Root asset
    log_traversal(f"Asset:{aircraft_id}")
    try:
        root = client.assets.retrieve(external_id=aircraft_id)
        root_dict = {
            "id": root.id,
            "externalId": root.external_id,
            "name": root.name,
            "description": root.description,
            "metadata": root.metadata or {},
        }
    except Exception as e:
        return {"error": f"Could not retrieve root asset {aircraft_id}: {e}"}

    meta = root_dict.get("metadata", {})
    overhaul_hobbs_str = meta.get("overhaul_hobbs", "")

    # 2. Full component hierarchy
    log_traversal(f"AssetSubtree:{aircraft_id}")
    try:
        subtree = client.assets.retrieve_subtree(external_id=aircraft_id)
        all_components = [
            {
                "id": a.id,
                "externalId": a.external_id,
                "name": a.name,
                "description": a.description,
                "parentExternalId": a.parent_external_id,
                "metadata": a.metadata or {},
            }
            for a in subtree
        ]
    except Exception:
        all_components = []

    # 3. OT sensors — per-tail time series IDs
    sensor_suffixes = [
        "aircraft.hobbs", "aircraft.tach", "aircraft.cycles", "aircraft.fuel_used",
        "engine.oil_pressure_min", "engine.oil_pressure_max",
        "engine.oil_temp_max", "engine.cht_max", "engine.egt_max",
    ]
    sensors: dict[str, Any] = {}
    for suffix in sensor_suffixes:
        ts_ext_id = f"{aircraft_id}.{suffix}"
        log_traversal(f"Sensor:latest:{ts_ext_id}")
        try:
            dp = client.time_series.data.retrieve_latest(external_id=ts_ext_id)
            if dp and len(dp) > 0:
                sensors[suffix] = {
                    "timestamp": int(dp[0].timestamp),
                    "value": float(dp[0].value),
                }
        except Exception:
            pass

    current_hobbs = _safe_float(sensors.get("aircraft.hobbs", {}).get("value"))
    current_tach = _safe_float(sensors.get("aircraft.tach", {}).get("value"))

    # Derive SMOH from metadata
    engine_smoh_str = meta.get("engine_smoh", "")
    try:
        engine_smoh = float(engine_smoh_str)
    except (ValueError, TypeError):
        engine_smoh = 0.0

    # 4. IT event layer
    all_events_flat: list[dict[str, Any]] = []
    try:
        events = client.events.list(asset_ids=[root.id], limit=1000)
        for e in events:
            all_events_flat.append({
                "id": e.id,
                "externalId": e.external_id,
                "type": e.type,
                "subtype": e.subtype,
                "description": e.description,
                "startTime": e.start_time,
                "metadata": e.metadata or {},
                "source": e.source,
            })
    except Exception:
        pass

    # 5. Open squawks
    all_squawks = [e for e in all_events_flat if e.get("type") == "Squawk"]
    open_squawks = [e for e in all_squawks if e.get("metadata", {}).get("status") == "open"]
    grounding_squawks = [e for e in open_squawks if e.get("metadata", {}).get("severity") == "grounding"]

    # 6. Annual inspection currency
    annual_inspections = [
        e for e in all_events_flat
        if e.get("type") == "Inspection" and (e.get("subtype") or "").lower() == "annual"
    ]
    last_annual: Optional[dict[str, Any]] = None
    if annual_inspections:
        last_annual = max(annual_inspections, key=lambda x: x.get("startTime") or 0)

    annual_due_date = ""
    annual_days_remaining: Optional[int] = None
    if last_annual:
        annual_due_date = last_annual.get("metadata", {}).get("next_due_date", "")
        annual_days_remaining = _days_until(annual_due_date)

    # 7. Oil change status
    oil_changes = [
        e for e in all_events_flat
        if e.get("type") == "MaintenanceRecord" and "oil_change" in (e.get("subtype") or "").lower()
    ]
    last_oil_change = max(oil_changes, key=lambda x: x.get("startTime") or 0) if oil_changes else None
    oil_next_due_hobbs = 0.0
    if last_oil_change:
        try:
            oil_next_due_hobbs = float(last_oil_change.get("metadata", {}).get("next_due_hobbs", 0))
        except (ValueError, TypeError):
            pass
    oil_hours_overdue = max(0.0, round(current_hobbs - oil_next_due_hobbs, 1)) if oil_next_due_hobbs > 0 else 0.0

    # 8. Upcoming maintenance
    maint_events = [e for e in all_events_flat if e.get("type") in ("MaintenanceRecord", "Inspection")]
    upcoming = derive_upcoming_maintenance(maint_events, current_hobbs)

    # 9. Symptoms for this aircraft
    log_traversal(f"Symptoms:{aircraft_id}")
    sym_data = _cdf_post("symptoms/list", {"filter": {"aircraft_id": aircraft_id}})
    sym_items = sym_data.get("items", [])
    symptoms_payload = {
        "aircraft_id": aircraft_id,
        "symptom_count": len(sym_items),
        "symptoms": sym_items,
    }
    symptom_deep_dive = symptom_fleet_deep_dive(aircraft_id, symptoms_payload)

    # 10. ET documents
    aircraft_docs = get_linked_documents(aircraft_id)
    engine_docs = get_linked_documents(f"{aircraft_id}-ENGINE")

    # 11. Airworthiness determination
    annual_expired = annual_days_remaining is not None and annual_days_remaining < 0
    has_grounding_squawk = len(grounding_squawks) > 0
    oil_5hr_overdue = oil_hours_overdue > 5.0
    oil_1hr_overdue = oil_hours_overdue >= 1.0

    if has_grounding_squawk or annual_expired or oil_5hr_overdue:
        airworthiness = "NOT_AIRWORTHY"
    elif oil_1hr_overdue:
        airworthiness = "FERRY_ONLY"
    elif len(open_squawks) > 0 and len(sym_items) > 0:
        airworthiness = "CAUTION"
    else:
        airworthiness = "AIRWORTHY"

    log_traversal(f"Context:{aircraft_id}(complete)")

    out: dict[str, Any] = {
        "aircraft": root_dict,
        "totalComponents": len(all_components),
        "components": all_components,
        "sensors": sensors,
        "currentHobbs": current_hobbs,
        "currentTach": current_tach,
        "engineSMOH": engine_smoh,
        "engineTBO": 2000,
        "engineSMOHPercent": round((engine_smoh / 2000.0) * 100, 1) if engine_smoh > 0 else 0.0,
        "allMaintenance": [e for e in all_events_flat if e.get("type") == "MaintenanceRecord"],
        "allInspections": annual_inspections,
        "openSquawks": open_squawks,
        "groundingSquawks": grounding_squawks,
        "allSquawks": all_squawks,
        "lastAnnual": last_annual,
        "annualDueDate": annual_due_date,
        "annualDaysRemaining": annual_days_remaining,
        "oilNextDueHobbs": oil_next_due_hobbs,
        "oilHoursOverdue": oil_hours_overdue,
        "upcomingMaintenance": upcoming,
        "symptoms": sym_items,
        "conditions": [],
        "documents": aircraft_docs.get("documents", []) + engine_docs.get("documents", []),
        "airworthiness": airworthiness,
        "isAirworthy": airworthiness == "AIRWORTHY",
    }
    if symptom_deep_dive is not None:
        out["symptomDeepDive"] = symptom_deep_dive
    return out
