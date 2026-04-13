"""
Agent Tools — Desert Sky Aviation Fleet CAG.

CDF graph traversal tools for the ReAct agent, using:
  - cognite-sdk Python client for standard CDF resources
  - httpx for custom fleet resource routes (policies, fleet_owners)

Key additions over the single-aircraft version:
  - get_fleet_overview: aggregate factual metadata for all four aircraft
  - get_fleet_policies: HTTP list operational policies
  - get_time_series_trend: last-N engine sensor window with trend stats and caution check
  - compare_engine_sensor_across_fleet: IS_TYPE peers + pre-failure datapoint windows
  - get_engine_type_history: same engine model → peer aircraft chronological events
  - search_fleet_for_similar_events: full-text search across pilot_notes and squawks
  - check_fleet_policy_compliance: evaluate policy rules against each aircraft
  - get_relationships: queries both outbound AND inbound edges so
    Aircraft → FleetOwner → Policy traversal works in both directions
"""

from __future__ import annotations

import contextvars
import os
from datetime import datetime, timezone
from typing import Any, Optional

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import Token
import httpx
from dotenv import load_dotenv

from ..aircraft_times import next_due_tach_from_meta  # noqa: E402
from ..date_only import calendar_days_until_iso  # noqa: E402

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# ---------------------------------------------------------------------------
# CDF client
# ---------------------------------------------------------------------------

_CDF_PROJECT = os.getenv("CDF_PROJECT", "desert_sky")
_CDF_BASE_URL = os.getenv("CDF_BASE_URL", "http://localhost:4001")
_CDF_TOKEN = os.getenv("CDF_TOKEN", "mock-token")

_config = ClientConfig(
    client_name="aircraft-health-cag-demo",
    project=_CDF_PROJECT,
    base_url=_CDF_BASE_URL,
    credentials=Token(_CDF_TOKEN),
)
client = CogniteClient(_config)

ENGINE_MODEL_EXT_ID = "ENGINE_MODEL_LYC_O320_H2AD"

DEFAULT_TREND_LOOKBACK = 10

ENGINE_METRIC_RANGES: dict[str, dict[str, Any]] = {
    "engine.cht_max":          {"normal_max": 400, "caution": 430, "unit": "°F"},
    "engine.egt_max":          {"normal_min": 1200, "normal_max": 1450, "unit": "°F"},
    "engine.oil_temp_max":     {"normal_min": 180, "normal_max": 245, "caution": 245, "unit": "°F"},
    "engine.oil_pressure_max": {"normal_min": 60, "normal_max": 90, "unit": "PSI"},
    "engine.oil_pressure_min": {"normal_min": 25, "caution_low": 25, "unit": "PSI"},
}

# ---------------------------------------------------------------------------
# Traversal log — per-request scoped via ContextVar
# ---------------------------------------------------------------------------

# Each async request gets its own list via the ContextVar, preventing concurrent
# requests from interleaving their traversal entries in the shared log.
_traversal_log_var: contextvars.ContextVar[list[str]] = contextvars.ContextVar(
    "traversal_log", default=None  # type: ignore[arg-type]
)


def _get_log() -> list[str]:
    log = _traversal_log_var.get(None)
    if log is None:
        log = []
        _traversal_log_var.set(log)
    return log


def log_traversal(message: str) -> None:
    """Record a graph traversal step for CAG visibility."""
    _get_log().append(message)
    print(f"[CAG] Traversed: {message}")


def get_traversal_log() -> list[str]:
    """Return the traversal log for the current request context."""
    return list(_get_log())


def clear_traversal_log() -> None:
    """Reset the traversal log for the current request context."""
    _traversal_log_var.set([])


# ---------------------------------------------------------------------------
# HTTP helper for custom mock CDF routes
# ---------------------------------------------------------------------------

def _cdf_post(path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """POST to mock CDF custom route, return parsed JSON."""
    url = f"{_CDF_BASE_URL}/api/v1/projects/{_CDF_PROJECT}/{path}"
    try:
        resp = httpx.post(url, json=body or {}, timeout=5.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e), "items": []}


# ---------------------------------------------------------------------------
# Standard CDF tools
# ---------------------------------------------------------------------------

def get_asset(asset_id: str) -> dict[str, Any]:
    """
    Retrieve a single asset node by externalId.
    Mirrors CDF Assets.retrieve() — entry point for any graph traversal.
    """
    log_traversal(f"Asset:{asset_id}")
    assets = client.assets.retrieve_multiple(external_ids=[asset_id], ignore_unknown_ids=True)
    if not assets:
        return {"error": f"Asset {asset_id} not found"}
    a = assets[0]
    return {
        "id": a.id,
        "externalId": a.external_id,
        "name": a.name,
        "description": a.description,
        "parentExternalId": a.parent_external_id,
        "metadata": a.metadata or {},
    }


def get_asset_children(asset_id: str) -> dict[str, Any]:
    """
    Retrieve direct children of an asset in the hierarchy.
    Mirrors CDF Assets.list(filter={parentExternalIds:[...]}).
    """
    log_traversal(f"AssetChildren:{asset_id}")
    children = client.assets.list(parent_external_ids=[asset_id], limit=100)
    return {
        "parentExternalId": asset_id,
        "children": [
            {
                "id": c.id,
                "externalId": c.external_id,
                "name": c.name,
                "description": c.description,
                "metadata": c.metadata or {},
            }
            for c in children
        ],
    }


def get_asset_subgraph(asset_id: str, depth: int = 2) -> dict[str, Any]:
    """
    Traverse the asset hierarchy to the given depth via subtree endpoint.
    Mirrors CDF Assets subtree — used for broad context assembly.
    """
    log_traversal(f"AssetSubgraph:{asset_id}(depth={depth})")
    try:
        subtree = client.assets.retrieve_subtree(external_id=asset_id)
        return {
            "rootExternalId": asset_id,
            "nodes": [
                {
                    "id": a.id,
                    "externalId": a.external_id,
                    "name": a.name,
                    "description": a.description,
                    "parentExternalId": a.parent_external_id,
                    "metadata": a.metadata or {},
                }
                for a in subtree
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def get_time_series(asset_id: str, metric: Optional[str] = None) -> dict[str, Any]:
    """
    Retrieve time series metadata associated with an asset.
    Mirrors CDF TimeSeries.list(filter={assetExternalIds:[...]}).
    """
    log_traversal(f"TimeSeries:{asset_id}" + (f"/{metric}" if metric else ""))
    try:
        asset = client.assets.retrieve(external_id=asset_id)
        if not asset or not asset.id:
            return {"error": f"Asset {asset_id} not found"}
        ts_list = client.time_series.list(asset_ids=[asset.id], limit=20)
        results = []
        for ts in ts_list:
            if metric and ts.external_id and metric.lower() not in ts.external_id.lower():
                continue
            results.append({
                "id": ts.id,
                "externalId": ts.external_id,
                "name": ts.name,
                "unit": ts.unit,
                "metadata": ts.metadata or {},
            })
        return {"assetId": asset_id, "timeSeries": results}
    except Exception as e:
        return {"error": str(e)}


def get_datapoints(
    ts_external_id: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    limit: int = 100,
) -> dict[str, Any]:
    """
    Retrieve actual OT sensor readings for a time series.
    Mirrors CDF Datapoints.retrieve() — the raw instrument data layer.
    """
    log_traversal(f"Datapoints:{ts_external_id}(limit={limit})")
    try:
        query: dict[str, Any] = {"externalId": ts_external_id, "limit": limit}
        if start is not None:
            query["start"] = start
        if end is not None:
            query["end"] = end
        raw = _cdf_post("timeseries/data/list", {"items": [query]})
        dp_list = (raw.get("items") or [{}])[0].get("datapoints", [])
        points = [
            {"timestamp": int(p["timestamp"]), "value": float(p["value"])}
            for p in dp_list
        ]
        return {"externalId": ts_external_id, "count": len(points), "datapoints": points}
    except Exception as e:
        return {"error": str(e)}


def get_events(
    asset_id: str,
    event_type: Optional[str] = None,
    status: Optional[str] = None,
) -> dict[str, Any]:
    """
    Retrieve IT records (maintenance, squawks, inspections, flights) for an asset.
    Mirrors CDF Events.list() — the work order / logbook layer.
    """
    log_traversal(
        f"Events:{asset_id}"
        + (f"[type={event_type}]" if event_type else "")
        + (f"[status={status}]" if status else "")
    )
    try:
        asset = client.assets.retrieve(external_id=asset_id)
        if not asset or not asset.id:
            return {"error": f"Asset {asset_id} not found"}
        events = client.events.list(asset_ids=[asset.id], type=event_type, limit=500)
        results = []
        for e in events:
            meta = e.metadata or {}
            if status and meta.get("status") != status:
                continue
            results.append({
                "id": e.id,
                "externalId": e.external_id,
                "type": e.type,
                "subtype": e.subtype,
                "description": e.description,
                "startTime": e.start_time,
                "metadata": meta,
                "source": e.source,
            })
        results.sort(key=lambda x: x.get("startTime") or 0, reverse=True)
        return {"assetId": asset_id, "count": len(results), "events": results}
    except Exception as e:
        return {"error": str(e)}


def get_relationships(
    asset_id: str,
    relationship_type: Optional[str] = None,
    direction: str = "both",
) -> dict[str, Any]:
    """
    Traverse graph edges from a given resource node.
    Mirrors CDF Relationships.list() — the core CAG traversal primitive.

    direction='both' (default) returns edges where the node is source OR target,
    enabling bidirectional traversal:  Aircraft → FleetOwner → Policy and reverse.
    """
    log_traversal(
        f"Relationships:{asset_id}"
        + (f"[type={relationship_type}]" if relationship_type else "")
        + f"[dir={direction}]"
    )
    try:
        # Use the bidirectional endpoint for 'both' direction
        if direction == "both":
            data = _cdf_post("relationships/bidirectional", {
                "externalId": asset_id,
                "relationshipType": relationship_type,
                "direction": "both",
            })
            results = []
            for r in data.get("items", []):
                results.append({
                    "externalId": r.get("externalId"),
                    "sourceExternalId": r.get("sourceExternalId"),
                    "sourceType": r.get("sourceType"),
                    "targetExternalId": r.get("targetExternalId"),
                    "targetType": r.get("targetType"),
                    "relationshipType": r.get("relationshipType"),
                })
            return {"resourceId": asset_id, "count": len(results), "relationships": results}

        # Outbound-only: use SDK
        rels = client.relationships.list(
            source_external_ids=[asset_id],
            fetch_resources=True,
            limit=200,
        )
        results = []
        for r in rels:
            try:
                raw_dict = r.dump()
                rel_type_val = raw_dict.get("relationshipType")
            except Exception:
                rel_type_val = None
            if relationship_type and rel_type_val != relationship_type:
                continue
            results.append({
                "externalId": r.external_id,
                "sourceExternalId": r.source_external_id,
                "sourceType": r.source_type,
                "targetExternalId": r.target_external_id,
                "targetType": r.target_type,
                "relationshipType": rel_type_val,
                "source": r.source.dump() if r.source else None,
                "target": r.target.dump() if r.target else None,
            })
        return {"resourceId": asset_id, "count": len(results), "relationships": results}
    except Exception as e:
        return {"error": str(e)}


def get_linked_documents(asset_id: str) -> dict[str, Any]:
    """
    Retrieve ET documents (POH sections, ADs, SBs) linked to an asset.
    Traverses LINKED_TO relationships → File nodes → downloads document text.
    """
    log_traversal(f"Documents:{asset_id}")
    try:
        rels = client.relationships.list(
            source_external_ids=[asset_id],
            fetch_resources=True,
            limit=50,
        )
        documents = []
        for r in rels:
            if (r.target_type or "").lower() != "file":
                continue
            target_ext_id = r.target_external_id
            log_traversal(f"File:{target_ext_id}")
            try:
                file_meta = client.files.retrieve(external_id=target_ext_id)
                filename = (file_meta.metadata or {}).get("filename", "")
                if filename:
                    resp = httpx.get(f"{_CDF_BASE_URL}/documents/{filename}", timeout=5.0)
                    if resp.status_code == 200:
                        documents.append({
                            "externalId": target_ext_id,
                            "name": file_meta.name,
                            "filename": filename,
                            "content": resp.text,
                        })
            except Exception:
                pass
        return {"assetId": asset_id, "count": len(documents), "documents": documents}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Fleet-specific tools
# ---------------------------------------------------------------------------

def get_fleet_overview() -> dict[str, Any]:
    """
    Aggregate factual metadata for all four Desert Sky Aviation aircraft.
    Traverses each aircraft root asset — no pre-labeled health status included.
    Agent derives airworthiness from maintenance/squawk data using CDF resources.
    """
    log_traversal("FleetOverview:Desert_Sky_Aviation")
    tails = ["N4798E", "N2251K", "N8834Q", "N1156P"]
    fleet = []
    for tail in tails:
        asset_info = get_asset(tail)
        meta = asset_info.get("metadata", {})
        fleet.append({
            "tail": tail,
            "name": asset_info.get("name", tail),
            "smoh": meta.get("engine_smoh", ""),
            "description": asset_info.get("description", ""),
        })
    return {
        "fleet": fleet,
        "totalAircraft": len(fleet),
        "operator": "Desert Sky Aviation",
        "base": "KPHX",
    }


def get_fleet_policies() -> dict[str, Any]:
    """
    Retrieve all Desert Sky Aviation operational policies.
    Calls the custom /policies/list route via httpx.
    """
    log_traversal("FleetPolicies:Desert_Sky_Aviation")
    data = _cdf_post("policies/list", {})
    policies = data.get("items", [])
    return {"count": len(policies), "policies": policies}


def get_time_series_trend(
    aircraft_id: str,
    metric: str,
    last_n: int = DEFAULT_TREND_LOOKBACK,
) -> dict[str, Any]:
    """
    Retrieve the last last_n datapoints for {aircraft_id}.{metric} and compute trend stats.

    Mirrors CDF Datapoints.retrieve() — the OT sensor layer. Returns trend_direction
    (compare mean of first-third vs last-third of the window), min/max/mean,
    and whether the latest value exceeds the caution threshold from ENGINE_METRIC_RANGES.
    Use before calling compare_engine_sensor_across_fleet when readings look anomalous.
    """
    ts_ext_id = f"{aircraft_id}.{metric}"
    log_traversal(f"Trend:{ts_ext_id}(last_n={last_n})")
    try:
        raw = _cdf_post("timeseries/data/list", {
            "items": [{"externalId": ts_ext_id, "limit": 1000}]
        })
        all_dps = (raw.get("items") or [{}])[0].get("datapoints", [])
        if not all_dps:
            return {"aircraft_id": aircraft_id, "metric": metric, "error": "No datapoints found"}
        points = [
            {"timestamp": int(p["timestamp"]), "value": float(p["value"])}
            for p in all_dps[-last_n:]
        ]
        values = [p["value"] for p in points]
        n = len(values)
        third = max(1, n // 3)
        first_mean = sum(values[:third]) / third
        last_mean = sum(values[-third:]) / third
        delta = last_mean - first_mean
        if delta > 2.0:
            trend_direction = "increasing"
        elif delta < -2.0:
            trend_direction = "decreasing"
        else:
            trend_direction = "stable"

        current_value = values[-1]
        limits = ENGINE_METRIC_RANGES.get(metric, {})
        caution = limits.get("caution")
        caution_low = limits.get("caution_low")
        exceeds_caution = False
        if caution is not None and current_value > caution:
            exceeds_caution = True
        if caution_low is not None and current_value < caution_low:
            exceeds_caution = True

        return {
            "aircraft_id": aircraft_id,
            "metric": metric,
            "ts_external_id": ts_ext_id,
            "datapoints": points,
            "current_value": current_value,
            "min": min(values),
            "max": max(values),
            "mean": round(sum(values) / n, 2),
            "trend_direction": trend_direction,
            "window_size": n,
            "normal_range": limits,
            "exceeds_caution": exceeds_caution,
        }
    except Exception as e:
        return {"aircraft_id": aircraft_id, "metric": metric, "error": str(e)}


def compare_engine_sensor_across_fleet(
    aircraft_id: str,
    metric: str,
    last_n: int = DEFAULT_TREND_LOOKBACK,
) -> dict[str, Any]:
    """
    Traverses IS_TYPE to find all peer aircraft sharing the same engine model, then for each
    peer retrieves the last last_n datapoints of {peer}.{metric} — or, if the peer has a
    grounding/failure event with no subsequent flights, the last last_n datapoints ending
    at the failure timestamp (pre-failure window).

    Mirrors cross-asset pattern discovery in CDF via IS_TYPE relationships. Use when
    get_time_series_trend shows an anomalous reading to check if peer aircraft showed
    the same pattern before a known engine failure.
    """
    log_traversal(f"FleetSensorCompare:{aircraft_id}.{metric}(start)")

    # Resolve engine node for the querying aircraft
    children = get_asset_children(aircraft_id).get("children", [])
    engine_ext: Optional[str] = None
    for c in children:
        eid = c.get("externalId") or ""
        if eid == f"{aircraft_id}-ENGINE":
            engine_ext = eid
            break
    if not engine_ext:
        return {"error": f"No {aircraft_id}-ENGINE child asset found"}

    # Walk IS_TYPE outbound from engine → engine model
    is_type_out = _cdf_post("relationships/bidirectional", {
        "externalId": engine_ext,
        "relationshipType": "IS_TYPE",
        "direction": "outbound",
    }).get("items", [])
    model_id: Optional[str] = None
    for r in is_type_out:
        if r.get("relationshipType") == "IS_TYPE" and r.get("sourceExternalId") == engine_ext:
            model_id = r.get("targetExternalId")
            break
    if not model_id:
        return {"error": "No IS_TYPE relationship from engine to engine model"}
    log_traversal(f"IS_TYPE:{engine_ext}→{model_id}")

    # Walk IS_TYPE inbound from engine model → all peer engines
    is_type_in = _cdf_post("relationships/bidirectional", {
        "externalId": model_id,
        "relationshipType": "IS_TYPE",
        "direction": "inbound",
    }).get("items", [])
    peer_engines: list[str] = []
    for r in is_type_in:
        if r.get("relationshipType") == "IS_TYPE" and r.get("targetExternalId") == model_id:
            src = r.get("sourceExternalId")
            if src and str(src).endswith("-ENGINE"):
                peer_engines.append(str(src))
    peer_engines = sorted(set(peer_engines))

    tails_known = {"N4798E", "N2251K", "N8834Q", "N1156P"}
    comparisons: list[dict[str, Any]] = []

    for peer_eng in peer_engines:
        if peer_eng == engine_ext:
            continue
        # Find the parent aircraft tail via HAS_COMPONENT inbound edge
        has_comp = _cdf_post("relationships/bidirectional", {
            "externalId": peer_eng,
            "relationshipType": "HAS_COMPONENT",
            "direction": "inbound",
        }).get("items", [])
        peer_tail: Optional[str] = None
        for r in has_comp:
            if r.get("relationshipType") == "HAS_COMPONENT" and r.get("targetExternalId") == peer_eng:
                p = r.get("sourceExternalId")
                if p in tails_known:
                    peer_tail = p
                    break
        if not peer_tail:
            continue
        log_traversal(f"FleetSensorCompare:peer={peer_tail}")

        # Detect failure/grounding: open grounding squawk with no subsequent flights
        failure_ts_ms: Optional[int] = None
        try:
            peer_asset = client.assets.retrieve(external_id=peer_tail)
            if peer_asset and peer_asset.id:
                all_evs = list(client.events.list(asset_ids=[peer_asset.id], limit=500))
                all_evs.sort(key=lambda e: e.start_time or 0)
                grounding_ts: Optional[int] = None
                for e in all_evs:
                    meta = e.metadata or {}
                    if (
                        e.type == "Squawk"
                        and meta.get("severity") == "grounding"
                        and meta.get("status") == "open"
                    ):
                        grounding_ts = e.start_time
                if grounding_ts is not None:
                    # Check if any Flight event occurred after grounding
                    post_flights = [
                        e for e in all_evs
                        if e.type == "Flight" and (e.start_time or 0) > grounding_ts
                    ]
                    if not post_flights:
                        failure_ts_ms = grounding_ts
                        log_traversal(f"FleetSensorCompare:{peer_tail}:failure_at={failure_ts_ms}")
        except Exception:
            pass

        # Retrieve pre-failure window or current window
        ts_ext_id = f"{peer_tail}.{metric}"
        try:
            if failure_ts_ms is not None:
                raw = _cdf_post("timeseries/data/list", {
                    "items": [{"externalId": ts_ext_id, "end": failure_ts_ms, "limit": 1000}]
                })
                window_label = "pre_failure"
            else:
                raw = _cdf_post("timeseries/data/list", {
                    "items": [{"externalId": ts_ext_id, "limit": 1000}]
                })
                window_label = "current"

            all_dps = (raw.get("items") or [{}])[0].get("datapoints", [])
            if not all_dps:
                comparisons.append({
                    "peer_tail": peer_tail,
                    "window": window_label,
                    "failure_date": (
                        datetime.fromtimestamp(failure_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                        if failure_ts_ms is not None else None
                    ),
                    "error": "No datapoints",
                })
                continue

            points = [
                {"timestamp": int(p["timestamp"]), "value": float(p["value"])}
                for p in all_dps[-last_n:]
            ]
            values = [p["value"] for p in points]
            n = len(values)
            third = max(1, n // 3)
            first_mean = sum(values[:third]) / third
            last_mean = sum(values[-third:]) / third
            delta = last_mean - first_mean
            trend_dir = "increasing" if delta > 2.0 else ("decreasing" if delta < -2.0 else "stable")

            # Collect pilot notes from Flight events in the same timestamp window
            start_ts = points[0]["timestamp"] if points else None
            end_ts = points[-1]["timestamp"] if points else None
            pilot_notes_in_window: list[dict[str, Any]] = []
            try:
                if peer_asset and peer_asset.id and start_ts is not None:
                    flight_evs = list(client.events.list(
                        asset_ids=[peer_asset.id], type="Flight", limit=500
                    ))
                    for fe in flight_evs:
                        fts = fe.start_time or 0
                        if start_ts <= fts <= (end_ts or fts):
                            notes = (fe.metadata or {}).get("pilot_notes", "")
                            if notes:
                                pilot_notes_in_window.append({
                                    "timestamp": fts,
                                    "pilot_notes": notes,
                                })
                    pilot_notes_in_window.sort(key=lambda x: x["timestamp"])
            except Exception:
                pass

            failure_date = (
                datetime.fromtimestamp(failure_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                if failure_ts_ms is not None else None
            )
            comparisons.append({
                "peer_tail": peer_tail,
                "window": window_label,
                "failure_date": failure_date,
                "current_value": values[-1],
                "min": min(values),
                "max": max(values),
                "mean": round(sum(values) / n, 2),
                "trend_direction": trend_dir,
                "pilot_notes_in_window": pilot_notes_in_window[-3:],
            })
        except Exception as exc:
            comparisons.append({"peer_tail": peer_tail, "error": str(exc)})

    log_traversal(f"FleetSensorCompare:{aircraft_id}.{metric}(complete)")
    return {
        "aircraft_id": aircraft_id,
        "metric": metric,
        "engine_model": model_id,
        "window_size": last_n,
        "comparisons": comparisons,
        "note": (
            "For peers with a grounding/failure squawk and no subsequent flights, "
            "the window shows the last datapoints BEFORE the failure. "
            "Compare the trend pattern to the current aircraft's readings."
        ),
    }


def get_engine_type_history(aircraft_id: str) -> dict[str, Any]:
    """
    Traverses IS_TYPE relationship to find all aircraft sharing the same engine model,
    then returns their time-ordered maintenance events, flight events, squawks, and
    any failure events. Enables the agent to discover historical patterns on similar
    engines without hardcoded causal relationships. Mirrors how Cognite CDF enables
    cross-asset pattern discovery via asset class relationships.
    """
    log_traversal(f"EngineTypeHistory:{aircraft_id}(start)")

    children = get_asset_children(aircraft_id).get("children", [])
    engine_ext: Optional[str] = None
    for c in children:
        eid = c.get("externalId") or ""
        if eid == f"{aircraft_id}-ENGINE":
            engine_ext = eid
            break
    if not engine_ext:
        return {"error": f"No {aircraft_id}-ENGINE child asset found"}

    is_type_out = _cdf_post("relationships/bidirectional", {
        "externalId": engine_ext,
        "relationshipType": "IS_TYPE",
        "direction": "outbound",
    }).get("items", [])
    model_id: Optional[str] = None
    for r in is_type_out:
        if r.get("relationshipType") == "IS_TYPE" and r.get("sourceExternalId") == engine_ext:
            model_id = r.get("targetExternalId")
            break
    if not model_id:
        return {"error": "No IS_TYPE relationship from engine to engine model"}

    log_traversal(f"IS_TYPE:{engine_ext}→{model_id}")

    is_type_in = _cdf_post("relationships/bidirectional", {
        "externalId": model_id,
        "relationshipType": "IS_TYPE",
        "direction": "inbound",
    }).get("items", [])
    peer_engines: list[str] = []
    for r in is_type_in:
        if r.get("relationshipType") == "IS_TYPE" and r.get("targetExternalId") == model_id:
            src = r.get("sourceExternalId")
            if src and str(src).endswith("-ENGINE"):
                peer_engines.append(str(src))
    peer_engines = sorted(set(peer_engines))

    history_by_tail: dict[str, list[dict[str, Any]]] = {}
    tails_known = {"N4798E", "N2251K", "N8834Q", "N1156P"}

    for peer_eng in peer_engines:
        if peer_eng == engine_ext:
            continue
        has_comp = _cdf_post("relationships/bidirectional", {
            "externalId": peer_eng,
            "relationshipType": "HAS_COMPONENT",
            "direction": "inbound",
        }).get("items", [])
        parent_tail: Optional[str] = None
        for r in has_comp:
            if r.get("relationshipType") == "HAS_COMPONENT" and r.get("targetExternalId") == peer_eng:
                p = r.get("sourceExternalId")
                if p in tails_known:
                    parent_tail = p
                    break
        if not parent_tail:
            continue
        log_traversal(f"HAS_COMPONENT:{parent_tail}→{peer_eng}")

        try:
            asset = client.assets.retrieve(external_id=parent_tail)
        except Exception:
            asset = None
        if not asset or not asset.id:
            continue
        evs = list(client.events.list(asset_ids=[asset.id], limit=500))
        evs.sort(key=lambda e: e.start_time or 0)
        rows: list[dict[str, Any]] = []
        for e in evs:
            meta = e.metadata or {}
            rows.append({
                "externalId": e.external_id,
                "type": e.type,
                "subtype": e.subtype,
                "description": e.description,
                "startTime": e.start_time,
                "metadata": dict(meta),
            })
        history_by_tail[parent_tail] = rows

    log_traversal(f"EngineTypeHistory:{aircraft_id}(complete)")
    return {
        "aircraft_id": aircraft_id,
        "engine_external_id": engine_ext,
        "engine_model_external_id": model_id,
        "peer_engine_assets_excluding_self": [e for e in peer_engines if e != engine_ext],
        "history_by_tail": history_by_tail,
        "note": (
            "Events are chronological (oldest first) per aircraft. "
            "Compare pilot_notes and descriptions across tails before any failure."
        ),
    }


def search_fleet_for_similar_events(description: str) -> dict[str, Any]:
    """
    Full-text search across all fleet events for patterns similar to the given description.

    Searches the following free-text fields:
      - Flight events: pilot_notes metadata field + event description
      - Squawk events: event description field
      - MaintenanceRecord events: description field

    Use to find cross-aircraft patterns — e.g. 'elevated CHT rough running' will
    match N8834Q squawks and N1156P pre-failure pilot notes.
    """
    log_traversal(f"FleetSearch:{description[:40]}")
    query_lower = description.lower()
    keywords = [w for w in query_lower.split() if len(w) > 2]
    matches = []

    def _score(text: str) -> int:
        if not text:
            return 0
        text_lower = text.lower()
        score = sum(1 for kw in keywords if kw in text_lower)
        if query_lower in text_lower:
            score += 3
        return score

    try:
        all_events = client.events.list(limit=2000)
        for e in all_events:
            if e.type not in ("Flight", "Squawk", "MaintenanceRecord"):
                continue
            meta = e.metadata or {}
            pilot_notes = meta.get("pilot_notes", "") or ""
            desc = e.description or ""
            tail = meta.get("tail", "")
            meta_text = " ".join(str(v) for v in meta.values() if v)
            score = _score(pilot_notes) + _score(desc) + _score(meta_text)
            if score > 0:
                matches.append({
                    "score": score,
                    "type": e.type,
                    "externalId": e.external_id,
                    "tail": tail,
                    "description": desc[:200],
                    "pilot_notes": pilot_notes[:200],
                    "startTime": e.start_time,
                    "metadata": {k: v for k, v in meta.items() if k in ("route", "severity", "status", "tail", "cht_max", "oil_temp_max")},
                })
    except Exception:
        pass

    # Sort by score descending, take top 20
    matches.sort(key=lambda x: x["score"], reverse=True)
    top_matches = matches[:20]

    return {
        "query": description,
        "matchCount": len(top_matches),
        "matches": top_matches,
    }


def check_fleet_policy_compliance(policy_id: Optional[str] = None) -> dict[str, Any]:
    """
    Evaluate fleet policy compliance for all aircraft.
    If policy_id provided, evaluates only that policy.
    """
    log_traversal(f"PolicyCompliance:{policy_id or 'all'}")
    tails = ["N4798E", "N2251K", "N8834Q", "N1156P"]

    # Get policies
    pol_data = get_fleet_policies()
    policies = pol_data.get("policies", [])
    if policy_id:
        policies = [p for p in policies if p.get("externalId") == policy_id]

    results = []
    for tail in tails:
        asset_info = get_asset(tail)
        meta = asset_info.get("metadata", {})

        for policy in policies:
            pol_ext_id = policy.get("externalId", "")
            category = policy.get("category", "")

            compliant = True
            notes = ""

            if pol_ext_id == "Policy_OilChangeInterval" or category == "engine_maintenance":
                # Check oil change status from squawks
                squawks = get_events(tail, "Squawk", "open")
                oil_overdue = any(
                    "oil" in (e.get("description") or "").lower() and "overdue" in (e.get("description") or "").lower()
                    for e in squawks.get("events", [])
                )
                if oil_overdue:
                    compliant = False
                    notes = "Oil change overdue"

            elif pol_ext_id == "Policy_AnnualInspection":
                grounding_sq = get_events(tail, "Squawk", "open")
                has_grounding = any(
                    e.get("metadata", {}).get("severity") == "grounding"
                    for e in grounding_sq.get("events", [])
                )
                if has_grounding:
                    compliant = False
                    notes = "Open grounding squawk"

            results.append({
                "tail": tail,
                "policy": pol_ext_id,
                "policy_title": policy.get("title", ""),
                "compliant": compliant,
                "notes": notes,
            })

    return {"evaluatedTails": tails, "results": results}


def assemble_aircraft_context(aircraft_id: str) -> dict[str, Any]:
    """
    Master CAG tool — builds complete connected context for one aircraft.

    Traverses: root asset → components → maintenance events → open squawks →
    sensor trend windows for key engine metrics → linked documents → policies.
    Returns sensor trend windows for key engine metrics (not snapshot values).
    """
    log_traversal(f"Context:{aircraft_id}(start)")

    root = get_asset(aircraft_id)
    if "error" in root:
        return root

    subgraph = get_asset_subgraph(aircraft_id, depth=2)
    all_assets = subgraph.get("nodes", [])

    # Latest Hobbs/tach for maintenance math (point-in-time only)
    hobbs_val = 0.0
    tach_val = 0.0
    for suffix, store_key in [("aircraft.hobbs", "hobbs"), ("aircraft.tach", "tach")]:
        ts_ext_id = f"{aircraft_id}.{suffix}"
        log_traversal(f"Sensor:latest:{ts_ext_id}")
        try:
            dp = client.time_series.data.retrieve_latest(external_id=ts_ext_id)
            if dp and len(dp) > 0:
                v = float(dp[0].value)
                if store_key == "hobbs":
                    hobbs_val = v
                else:
                    tach_val = v
        except Exception:
            pass

    # Engine sensor trends (last N datapoints with stats)
    engine_trends: dict[str, Any] = {}
    for metric in ("engine.cht_max", "engine.oil_temp_max", "engine.oil_pressure_max", "engine.egt_max"):
        engine_trends[metric] = get_time_series_trend(aircraft_id, metric)

    # IT events
    maintenance = get_events(aircraft_id, "MaintenanceRecord")
    inspections = get_events(aircraft_id, "Inspection")
    squawks = get_events(aircraft_id, "Squawk")
    flights = get_events(aircraft_id, "Flight")

    open_squawks = [e for e in squawks.get("events", []) if e.get("metadata", {}).get("status") == "open"]
    grounding_squawks = [e for e in open_squawks if e.get("metadata", {}).get("severity") == "grounding"]

    # Annual inspection
    annual_inspections = [e for e in inspections.get("events", []) if (e.get("subtype") or "").lower() == "annual"]
    last_annual = max(annual_inspections, key=lambda x: x.get("startTime") or 0) if annual_inspections else None
    annual_due_date = (last_annual or {}).get("metadata", {}).get("next_due_date", "") if last_annual else ""

    # Oil change status
    all_maint = maintenance.get("events", [])
    oil_changes = [
        e for e in all_maint
        if "oil_change" in (e.get("subtype") or "").lower()
    ]
    oil_next_due_tach: Optional[float] = None
    if oil_changes:
        last_oil = max(oil_changes, key=lambda x: x.get("startTime") or 0)
        oil_next_due_tach = next_due_tach_from_meta(last_oil.get("metadata", {}))

    oil_hours_until_due: Optional[float] = None
    oil_tach_hours_overdue = 0.0
    if oil_next_due_tach is not None and tach_val > 0:
        oil_hours_until_due = round(oil_next_due_tach - tach_val, 1)
        oil_tach_hours_overdue = max(0.0, -oil_hours_until_due)

    # Annual days remaining
    annual_days_remaining = calendar_days_until_iso(annual_due_date) if annual_due_date else None

    # Airworthiness derivation (mechanical compliance only — agent adds sensor assessment)
    has_grounding = len(grounding_squawks) > 0
    annual_expired = annual_days_remaining is not None and annual_days_remaining < 0
    oil_tach_not_airworthy = oil_tach_hours_overdue > 5.0
    oil_tach_ferry = 0.0 < oil_tach_hours_overdue <= 5.0
    if has_grounding or annual_expired or oil_tach_not_airworthy:
        derived_airworthiness = "NOT_AIRWORTHY"
    elif oil_tach_ferry:
        derived_airworthiness = "FERRY_ONLY"
    else:
        derived_airworthiness = "AIRWORTHY"

    policies = get_fleet_policies()

    # ET documents
    docs = get_linked_documents(aircraft_id)

    log_traversal(f"Context:{aircraft_id}(complete)")

    return {
        "aircraft": root,
        "totalComponents": len(all_assets),
        "components": all_assets,
        "currentHobbs": hobbs_val,
        "currentTach": tach_val,
        "oilNextDueTach": oil_next_due_tach,
        "oilHoursUntilDue": oil_hours_until_due,
        "oilTachHoursOverdue": oil_tach_hours_overdue,
        "annualDueDate": annual_due_date,
        "annualDaysRemaining": annual_days_remaining,
        "airworthiness": derived_airworthiness,
        "engineTrends": engine_trends,
        "maintenance": maintenance.get("events", [])[:10],
        "inspections": inspections.get("events", [])[:5],
        "openSquawks": open_squawks,
        "groundingSquawks": grounding_squawks,
        "allSquawks": squawks.get("events", []),
        "recentFlights": flights.get("events", [])[:10],
        "lastAnnualDueDate": annual_due_date,
        "policies": [
            {k: v for k, v in p.items() if k != "externalId"}
            for p in policies.get("policies", [])
        ],
        "documents": docs.get("documents", []),
        "traversalLog": get_traversal_log(),
    }


def assemble_fleet_context() -> dict[str, Any]:
    """
    Fleet-wide context assembly — four aircraft summaries + policies + alerts.

    Mirrors how Cognite Atlas AI would assemble context across multiple
    assets in an industrial setting — one connected traversal across the fleet.
    """
    log_traversal("FleetContext:Desert_Sky_Aviation(start)")

    fleet = get_fleet_overview()
    policies = get_fleet_policies()

    aircraft_summaries = []
    for info in fleet.get("fleet", []):
        tail = info["tail"]
        squawks = get_events(tail, "Squawk", "open")
        open_squawk_list = squawks.get("events", [])
        open_grounding = [e for e in open_squawk_list if e.get("metadata", {}).get("severity") == "grounding"]
        has_grounding = len(open_grounding) > 0

        # Annual inspection currency
        inspections = get_events(tail, "Inspection")
        annual_events = [
            e for e in inspections.get("events", [])
            if (e.get("subtype") or "").lower() == "annual"
        ]
        annual_due_date = ""
        annual_days_remaining: Optional[int] = None
        if annual_events:
            last_annual = max(annual_events, key=lambda x: x.get("startTime") or 0)
            annual_due_date = last_annual.get("metadata", {}).get("next_due_date", "")
            if annual_due_date:
                annual_days_remaining = calendar_days_until_iso(annual_due_date)

        # Oil change status
        maintenance_events = get_events(tail, "MaintenanceRecord")
        oil_changes = [
            e for e in maintenance_events.get("events", [])
            if "oil_change" in (e.get("subtype") or "").lower()
        ]
        oil_next_due_tach: Optional[float] = None
        oil_hours_until_due: Optional[float] = None
        oil_tach_hours_overdue = 0.0
        if oil_changes:
            last_oil = max(oil_changes, key=lambda x: x.get("startTime") or 0)
            oil_next_due_tach = next_due_tach_from_meta(last_oil.get("metadata", {}))

        # Current tach for oil math
        try:
            dp = client.time_series.data.retrieve_latest(external_id=f"{tail}.aircraft.tach")
            current_tach = float(dp[0].value) if dp and len(dp) > 0 else 0.0
        except Exception:
            current_tach = 0.0

        if oil_next_due_tach is not None and current_tach > 0:
            oil_hours_until_due = round(oil_next_due_tach - current_tach, 1)
            oil_tach_hours_overdue = max(0.0, -oil_hours_until_due)

        # Airworthiness derivation (mechanical compliance only — agent adds sensor assessment)
        annual_expired = annual_days_remaining is not None and annual_days_remaining < 0
        oil_tach_not_airworthy = oil_tach_hours_overdue > 5.0
        oil_tach_ferry = 0.0 < oil_tach_hours_overdue <= 5.0
        if has_grounding or annual_expired or oil_tach_not_airworthy:
            derived_airworthiness = "NOT_AIRWORTHY"
        elif oil_tach_ferry:
            derived_airworthiness = "FERRY_ONLY"
        else:
            derived_airworthiness = "AIRWORTHY"

        # Engine sensor trends — flag anomalous metrics
        anomalous_metrics: list[dict[str, Any]] = []
        fleet_comparisons: list[dict[str, Any]] = []
        for metric in ENGINE_METRIC_RANGES:
            trend = get_time_series_trend(tail, metric)
            if trend.get("exceeds_caution"):
                anomalous_metrics.append({
                    "metric": metric,
                    "current_value": trend.get("current_value"),
                    "trend_direction": trend.get("trend_direction"),
                    "exceeds_caution": trend.get("exceeds_caution"),
                })
                comparison = compare_engine_sensor_across_fleet(tail, metric)
                fleet_comparisons.append(comparison)

        aircraft_summaries.append({
            "tail": tail,
            "smoh": info.get("smoh"),
            "openGroundingSquawks": len(open_grounding),
            "groundingSquawks": [
                {
                    "description": e.get("description", ""),
                    "dateIdentified": (e.get("metadata") or {}).get("date", ""),
                    "severity": (e.get("metadata") or {}).get("severity", "grounding"),
                }
                for e in open_grounding
            ],
            "openSquawks": squawks.get("count", 0),
            "annualDueDate": annual_due_date,
            "annualDaysRemaining": annual_days_remaining,
            "oilHoursUntilDue": oil_hours_until_due,
            "oilTachHoursOverdue": oil_tach_hours_overdue,
            "airworthiness": derived_airworthiness,
            "anomalousMetrics": anomalous_metrics,
            "fleetSensorComparisons": fleet_comparisons,
        })

    log_traversal("FleetContext:assembled")
    return {
        "operator": "Desert Sky Aviation",
        "base": "KPHX",
        "aircraftCount": len(aircraft_summaries),
        "aircraft": aircraft_summaries,
        "policies": [
            {k: v for k, v in p.items() if k != "externalId"}
            for p in policies.get("policies", [])
        ],
        "traversalLog": get_traversal_log(),
    }


# ---------------------------------------------------------------------------
# Tool definitions — Claude function calling schemas
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "get_asset",
        "description": (
            "Retrieve a specific CDF asset node by its externalId. "
            "Aircraft roots: N4798E, N2251K, N8834Q, N1156P. "
            "Components: {TAIL}-ENGINE, {TAIL}-ENGINE-CYLINDERS, {TAIL}-ENGINE-OIL, "
            "{TAIL}-PROPELLER, {TAIL}-AIRFRAME, {TAIL}-AVIONICS, {TAIL}-FUEL-SYSTEM. "
            "Fleet owner: Desert_Sky_Aviation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string", "description": "Asset externalId"}
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_asset_children",
        "description": "Get direct child assets in the component hierarchy.",
        "input_schema": {
            "type": "object",
            "properties": {"asset_id": {"type": "string"}},
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_asset_subgraph",
        "description": "Traverse the asset hierarchy to the specified depth.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "depth": {"type": "integer", "default": 2},
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_time_series",
        "description": "Retrieve OT time series metadata for an asset.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "metric": {"type": "string"},
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_datapoints",
        "description": (
            "Retrieve OT sensor readings from a time series. "
            "Per-tail format: {TAIL}.aircraft.hobbs, {TAIL}.engine.cht_max, etc."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ts_external_id": {"type": "string"},
                "start": {"type": "integer"},
                "end": {"type": "integer"},
                "limit": {"type": "integer", "default": 100},
            },
            "required": ["ts_external_id"],
        },
    },
    {
        "name": "get_events",
        "description": (
            "Retrieve IT maintenance records, squawks, inspections, or flights for an asset. "
            "Types: MaintenanceRecord, Squawk, Inspection, Flight. "
            "Status filter for squawks: open, resolved, deferred."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "event_type": {
                    "type": "string",
                    "enum": ["MaintenanceRecord", "Squawk", "Inspection", "Flight"],
                },
                "status": {"type": "string"},
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_relationships",
        "description": (
            "Traverse graph edges from a resource node. direction='both' returns inbound and outbound edges — "
            "use this for Aircraft → FleetOwner → Policy traversal. "
            "Relationship types: HAS_COMPONENT, IS_TYPE, GOVERNED_BY, HAS_POLICY, "
            "PERFORMED_ON, REFERENCES_AD, IDENTIFIED_ON, LINKED_TO."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "relationship_type": {"type": "string"},
                "direction": {
                    "type": "string",
                    "enum": ["both", "outbound", "inbound"],
                    "default": "both",
                },
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_linked_documents",
        "description": "Retrieve ET documents (POH, ADs, SBs) linked to an asset via LINKED_TO relationships.",
        "input_schema": {
            "type": "object",
            "properties": {"asset_id": {"type": "string"}},
            "required": ["asset_id"],
        },
    },
    {
        "name": "get_fleet_overview",
        "description": (
            "Get factual metadata for all four Desert Sky Aviation aircraft: tail, SMOH, description. "
            "No pre-labeled health status — agent derives airworthiness from maintenance/squawk data."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_fleet_policies",
        "description": "List all Desert Sky Aviation operational policies: oil change intervals, ferry authorization, annual requirements.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_time_series_trend",
        "description": (
            "Retrieve the last last_n datapoints for a key engine sensor and compute trend stats. "
            "Returns current_value, min, max, mean, trend_direction (increasing/decreasing/stable), "
            "normal_range, and exceeds_caution flag. "
            "Use when an aircraft shows elevated readings or pilot notes mention sensor anomalies. "
            "Metrics: engine.cht_max, engine.egt_max, engine.oil_temp_max, "
            "engine.oil_pressure_max, engine.oil_pressure_min."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "aircraft_id": {"type": "string", "description": "Tail number, e.g. N8834Q"},
                "metric": {"type": "string", "description": "Metric suffix, e.g. engine.cht_max"},
                "last_n": {"type": "integer", "description": "Number of datapoints to retrieve (default 10)"},
            },
            "required": ["aircraft_id", "metric"],
        },
    },
    {
        "name": "compare_engine_sensor_across_fleet",
        "description": (
            "Traverses IS_TYPE to find all peer aircraft sharing the same engine model, then "
            "retrieves the last last_n datapoints of the given metric for each peer. "
            "For peers with a grounding squawk and no subsequent flights (failure event), "
            "the window shows the last datapoints BEFORE the failure. "
            "Use when a sensor is anomalous to check if peer aircraft showed the same pattern "
            "before a known engine failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "aircraft_id": {"type": "string", "description": "Tail number, e.g. N8834Q"},
                "metric": {"type": "string", "description": "Metric suffix, e.g. engine.cht_max"},
                "last_n": {"type": "integer", "description": "Window size (default 10)"},
            },
            "required": ["aircraft_id", "metric"],
        },
    },
    {
        "name": "get_engine_type_history",
        "description": (
            "Given a tail number, find {TAIL}-ENGINE, follow IS_TYPE to the shared engine model asset, "
            "then list all other engines of that model and return chronological events "
            "(flights, squawks, maintenance) for each peer aircraft. "
            "Use for in-depth chronological review of what happened to peer aircraft over time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "aircraft_id": {
                    "type": "string",
                    "description": "Aircraft root externalId, e.g. N8834Q",
                }
            },
            "required": ["aircraft_id"],
        },
    },
    {
        "name": "search_fleet_for_similar_events",
        "description": (
            "Full-text search across all fleet events (flights, squawks, maintenance records). "
            "Searches pilot_notes, squawk descriptions, and maintenance descriptions. "
            "Use to find patterns across aircraft — e.g. 'elevated CHT rough running' "
            "will match N8834Q squawks AND N1156P pre-failure pilot notes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "Description or phrase to search for",
                }
            },
            "required": ["description"],
        },
    },
    {
        "name": "check_fleet_policy_compliance",
        "description": "Evaluate which aircraft are in compliance with Desert Sky Aviation operational policies.",
        "input_schema": {
            "type": "object",
            "properties": {
                "policy_id": {
                    "type": "string",
                    "description": "Optional policy externalId to check. If omitted, checks all policies.",
                }
            },
            "required": [],
        },
    },
    {
        "name": "assemble_aircraft_context",
        "description": (
            "Master context tool — assembles full connected context for ONE aircraft. "
            "Returns: components, sensor trend windows for key engine metrics (engineTrends), "
            "maintenance, squawks, 10 recent flights, policies, documents. "
            "Start here for any single-aircraft question before calling more specific tools."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "aircraft_id": {
                    "type": "string",
                    "description": "Tail number: N4798E, N2251K, N8834Q, or N1156P",
                }
            },
            "required": ["aircraft_id"],
        },
    },
    {
        "name": "assemble_fleet_context",
        "description": (
            "Fleet-wide context tool — summaries for all four aircraft + policies. "
            "For each aircraft: squawk counts, engine sensor trends for all key metrics, "
            "and cross-fleet sensor comparison when anomalies are detected. "
            "Use for questions about the whole fleet or comparative analysis."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
]


def execute_tool(tool_name: str, tool_input: dict[str, Any]) -> Any:
    """Dispatch a tool call from the agent's ReAct loop."""
    dispatch: dict[str, Any] = {
        "get_asset": lambda: get_asset(tool_input["asset_id"]),
        "get_asset_children": lambda: get_asset_children(tool_input["asset_id"]),
        "get_asset_subgraph": lambda: get_asset_subgraph(
            tool_input["asset_id"], tool_input.get("depth", 2)
        ),
        "get_time_series": lambda: get_time_series(
            tool_input["asset_id"], tool_input.get("metric")
        ),
        "get_datapoints": lambda: get_datapoints(
            tool_input["ts_external_id"],
            tool_input.get("start"),
            tool_input.get("end"),
            tool_input.get("limit", 100),
        ),
        "get_events": lambda: get_events(
            tool_input["asset_id"],
            tool_input.get("event_type"),
            tool_input.get("status"),
        ),
        "get_relationships": lambda: get_relationships(
            tool_input["asset_id"],
            tool_input.get("relationship_type"),
            tool_input.get("direction", "both"),
        ),
        "get_linked_documents": lambda: get_linked_documents(tool_input["asset_id"]),
        "get_fleet_overview": lambda: get_fleet_overview(),
        "get_fleet_policies": lambda: get_fleet_policies(),
        "get_time_series_trend": lambda: get_time_series_trend(
            tool_input["aircraft_id"],
            tool_input["metric"],
            tool_input.get("last_n", DEFAULT_TREND_LOOKBACK),
        ),
        "compare_engine_sensor_across_fleet": lambda: compare_engine_sensor_across_fleet(
            tool_input["aircraft_id"],
            tool_input["metric"],
            tool_input.get("last_n", DEFAULT_TREND_LOOKBACK),
        ),
        "get_engine_type_history": lambda: get_engine_type_history(tool_input["aircraft_id"]),
        "search_fleet_for_similar_events": lambda: search_fleet_for_similar_events(
            tool_input["description"]
        ),
        "check_fleet_policy_compliance": lambda: check_fleet_policy_compliance(
            tool_input.get("policy_id")
        ),
        "assemble_aircraft_context": lambda: assemble_aircraft_context(
            tool_input.get("aircraft_id", "N4798E")
        ),
        "assemble_fleet_context": lambda: assemble_fleet_context(),
    }
    fn = dispatch.get(tool_name)
    if fn is None:
        return {"error": f"Unknown tool: {tool_name}"}
    return fn()
