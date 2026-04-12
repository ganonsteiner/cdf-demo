"""
Agent Tools — Desert Sky Aviation Fleet CAG.

CDF graph traversal tools for the ReAct agent, using:
  - cognite-sdk Python client for standard CDF resources
  - httpx for custom fleet resource routes (policies, fleet_owners)

Key additions over the single-aircraft version:
  - get_fleet_overview: aggregate all four aircraft status
  - get_fleet_policies: HTTP list operational policies
  - get_aircraft_symptoms: symptoms for a specific tail
  - get_engine_type_history: same engine model → peer aircraft chronological events
  - search_fleet_for_similar_events: full-text search across pilot_notes,
    squawk descriptions, and symptom text fields
  - check_fleet_policy_compliance: evaluate policy rules against each aircraft
  - get_relationships: now queries both outbound AND inbound edges so
    Aircraft → FleetOwner → Policy traversal works in both directions
"""

from __future__ import annotations

import os
from typing import Any, Optional

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import Token
import httpx
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# ---------------------------------------------------------------------------
# CDF client
# ---------------------------------------------------------------------------

_CDF_PROJECT = os.getenv("CDF_PROJECT", "desert_sky")
_CDF_BASE_URL = os.getenv("CDF_BASE_URL", "http://localhost:4000")
_CDF_TOKEN = os.getenv("CDF_TOKEN", "mock-token")

_config = ClientConfig(
    client_name="aircraft-health-cag-demo",
    project=_CDF_PROJECT,
    base_url=_CDF_BASE_URL,
    credentials=Token(_CDF_TOKEN),
)
client = CogniteClient(_config)

ENGINE_MODEL_EXT_ID = "ENGINE_MODEL_LYC_O320_H2AD"

# ---------------------------------------------------------------------------
# Traversal log
# ---------------------------------------------------------------------------

traversal_log: list[str] = []


def log_traversal(message: str) -> None:
    """Record a graph traversal step for CAG visibility."""
    traversal_log.append(message)
    print(f"[CAG] Traversed: {message}")


def clear_traversal_log() -> None:
    traversal_log.clear()


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
        dps = client.time_series.data.retrieve(
            external_id=ts_external_id,
            start=start,
            end=end,
            limit=limit,
        )
        if dps is None or len(dps) == 0:
            return {"externalId": ts_external_id, "datapoints": []}
        points = [
            {"timestamp": int(ts), "value": float(v)}
            for ts, v in zip(dps.timestamp, dps.value)
        ]
        return {"externalId": ts_external_id, "count": len(points), "datapoints": points[-20:]}
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
    Aggregate status summary for all four Desert Sky Aviation aircraft.
    Traverses each aircraft root asset and collects key metadata.
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
            "status": meta.get("airworthiness_status", "UNKNOWN"),
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


def observation_symptom_event_to_legacy_dict(e: Any) -> dict[str, Any]:
    """
    Map a CDF Event (type Observation, subtype Symptom) to the legacy symptom dict shape
    used by symptom_fleet_deep_dive, /api/status, and the UI.
    """
    meta = dict(e.metadata or {})
    return {
        "externalId": e.external_id,
        "aircraft_id": str(meta.get("aircraft_id", "") or ""),
        "title": str(meta.get("title", "") or ""),
        "description": e.description or "",
        "observation": str(meta.get("observation", "") or ""),
        "severity": str(meta.get("severity", "caution") or "caution"),
        "first_observed": str(meta.get("first_observed", "") or ""),
        "type": "Observation",
    }


def fetch_aircraft_symptoms_payload(aircraft_id: str) -> dict[str, Any]:
    """
    Retrieve Observation/Symptom typed events for this aircraft using the standard CDF Events API.
    Symptoms are modeled as Events with type='Observation', subtype='Symptom' (not a custom resource).
    """
    log_traversal(f"Symptoms:{aircraft_id}")
    try:
        asset = client.assets.retrieve(external_id=aircraft_id)
    except Exception:
        asset = None
    if not asset or not asset.id:
        return {"aircraft_id": aircraft_id, "symptom_count": 0, "symptoms": []}
    evs = list(
        client.events.list(
            type="Observation",
            subtype="Symptom",
            asset_ids=[asset.id],
            limit=100,
        )
    )
    symptoms = [observation_symptom_event_to_legacy_dict(e) for e in evs]
    return {
        "aircraft_id": aircraft_id,
        "symptom_count": len(symptoms),
        "symptoms": symptoms,
    }


def get_aircraft_symptoms(aircraft_id: str) -> dict[str, Any]:
    """
    Retrieve observed symptoms for a specific aircraft as CDF Observation/Symptom events (SDK).
    """
    return fetch_aircraft_symptoms_payload(aircraft_id)


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
    Full-text search across all fleet events and symptoms for patterns
    similar to the given description.

    Searches the following free-text fields:
      - Flight events: pilot_notes metadata field + event description
      - Squawk events: event description field
      - Observation/Symptom events: description and metadata (title, observation, etc.)
      - MaintenanceRecord events: description field

    This enables discovery of the N8834Q / N1156P pattern:
    similar CHT elevation and roughness symptoms that preceded engine failure.
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
        # Boost for exact phrase match
        if query_lower in text_lower:
            score += 3
        return score

    # Search flight events (full text of pilot_notes + description)
    try:
        all_events = client.events.list(limit=2000)
        for e in all_events:
            meta = e.metadata or {}
            pilot_notes = meta.get("pilot_notes", "") or ""
            desc = e.description or ""
            tail = meta.get("tail", "")

            meta_text = " ".join(str(v) for v in meta.values() if v)
            score = _score(pilot_notes) + _score(desc) + _score(meta_text)
            if score > 0 and e.type in ("Flight", "Squawk", "MaintenanceRecord"):
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

    # Observation / Symptom events (description + metadata text)
    try:
        obs_events = list(
            client.events.list(type="Observation", subtype="Symptom", limit=100)
        )
    except Exception:
        obs_events = []
    for e in obs_events:
        meta = e.metadata or {}
        desc = e.description or ""
        meta_text = " ".join(str(v) for v in meta.values() if v)
        score = _score(desc) + _score(meta_text)
        if score > 0:
            matches.append({
                "score": score,
                "type": "Observation",
                "subtype": "Symptom",
                "externalId": e.external_id,
                "tail": str(meta.get("aircraft_id", "") or ""),
                "description": desc[:200],
                "observation": str(meta.get("observation", ""))[:200],
                "title": str(meta.get("title", "")),
                "severity": str(meta.get("severity", "")),
                "startTime": e.start_time,
            })

    # Sort by score descending, take top 20
    matches.sort(key=lambda x: x["score"], reverse=True)
    top_matches = matches[:20]

    return {
        "query": description,
        "matchCount": len(top_matches),
        "matches": top_matches,
    }


def _build_symptom_fleet_search_query(symptoms: dict[str, Any]) -> str:
    """Concatenate symptom titles and narrative fields for fleet keyword search."""
    parts: list[str] = []
    for s in symptoms.get("symptoms", []):
        for key in ("title", "description", "observation"):
            v = s.get(key)
            if v:
                parts.append(str(v))
    text = " ".join(parts).strip()
    return text[:600] if len(text) > 600 else text


def symptom_fleet_deep_dive(aircraft_id: str, symptoms: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    When an aircraft has Observation/Symptom events, fetch cross-asset context: same engine model
    (IS_TYPE) peer timelines and keyword matches across pilot notes, squawks, and
    symptom text. Mirrors fleet-scale pattern discovery in CDF without vector search.
    """
    items = symptoms.get("symptoms") or []
    if len(items) == 0:
        return None

    query = _build_symptom_fleet_search_query(symptoms)
    if len(query.strip()) < 3:
        query = " ".join(str(s.get("title") or "") for s in items).strip() or "engine symptom"

    return {
        "triggered": True,
        "guidance": (
            "This aircraft has observed symptoms. Below: peer aircraft with the same engine model "
            "(IS_TYPE → engine model) and their time-ordered events — read chronologically; the "
            "sequence before any failure is the causal story (no PRECEDED edges required). "
            "Fleet search matches pilot notes, squawk text, and symptom descriptions. Call "
            "search_fleet_for_similar_events() again with narrower phrases if you need more focus."
        ),
        "engineTypePeerHistory": get_engine_type_history(aircraft_id),
        "fleetSearchFromSymptoms": search_fleet_for_similar_events(query),
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
                status = meta.get("airworthiness_status", "")
                if status == "NOT_AIRWORTHY":
                    compliant = False
                    notes = "Annual expired or aircraft not airworthy"

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
    sensor summaries → symptoms → linked documents → policies
    """
    log_traversal(f"Context:{aircraft_id}(start)")

    root = get_asset(aircraft_id)
    if "error" in root:
        return root

    subgraph = get_asset_subgraph(aircraft_id, depth=2)
    all_assets = subgraph.get("nodes", [])

    # OT sensors
    sensors: dict[str, Any] = {}
    for suffix in ["aircraft.hobbs", "aircraft.tach", "engine.cht_max", "engine.oil_pressure_max", "engine.oil_temp_max"]:
        ts_ext_id = f"{aircraft_id}.{suffix}"
        log_traversal(f"Sensor:latest:{ts_ext_id}")
        try:
            dp = client.time_series.data.retrieve_latest(external_id=ts_ext_id)
            if dp and len(dp) > 0:
                sensors[suffix] = {"timestamp": int(dp[0].timestamp), "value": float(dp[0].value)}
        except Exception:
            pass

    current_hobbs = sensors.get("aircraft.hobbs", {}).get("value", 0.0)

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

    # Fleet policies and symptoms
    symptoms = get_aircraft_symptoms(aircraft_id)
    symptom_deep_dive = symptom_fleet_deep_dive(aircraft_id, symptoms)
    policies = get_fleet_policies()

    # ET documents
    docs = get_linked_documents(aircraft_id)

    log_traversal(f"Context:{aircraft_id}(complete)")

    out: dict[str, Any] = {
        "aircraft": root,
        "totalComponents": len(all_assets),
        "components": all_assets,
        "sensors": sensors,
        "currentHobbs": current_hobbs,
        "maintenance": maintenance.get("events", [])[:10],
        "inspections": inspections.get("events", [])[:5],
        "openSquawks": open_squawks,
        "groundingSquawks": grounding_squawks,
        "allSquawks": squawks.get("events", []),
        "recentFlights": flights.get("events", [])[:5],
        "lastAnnualDueDate": annual_due_date,
        "symptoms": symptoms,
        "policies": policies.get("policies", []),
        "documents": docs.get("documents", []),
        "traversalLog": list(traversal_log),
    }
    if symptom_deep_dive is not None:
        out["symptomDeepDive"] = symptom_deep_dive
    return out


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
        # Quick status per aircraft
        squawks = get_events(tail, "Squawk", "open")
        open_grounding = [e for e in squawks.get("events", []) if e.get("metadata", {}).get("severity") == "grounding"]
        symptoms_data = get_aircraft_symptoms(tail)

        aircraft_summaries.append({
            "tail": tail,
            "status": info.get("status"),
            "smoh": info.get("smoh"),
            "openGroundingSquawks": len(open_grounding),
            "openSquawks": squawks.get("count", 0),
            "activeSymptoms": symptoms_data.get("symptom_count", 0),
        })

    log_traversal("FleetContext:assembled")
    return {
        "operator": "Desert Sky Aviation",
        "base": "KPHX",
        "aircraftCount": len(aircraft_summaries),
        "aircraft": aircraft_summaries,
        "policies": policies.get("policies", []),
        "traversalLog": list(traversal_log),
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
            "Relationship types: HAS_COMPONENT, PERFORMED_ON, REFERENCES_AD, IDENTIFIED_ON, "
            "HAS_COMPONENT, IS_TYPE, GOVERNED_BY, HAS_POLICY, EXHIBITED, OBSERVED_ON, LINKED_TO, PERFORMED_ON, IDENTIFIED_ON."
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
        "description": "Get aggregate status of all four Desert Sky Aviation aircraft: tail, SMOH, airworthiness status.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_fleet_policies",
        "description": "List all Desert Sky Aviation operational policies: oil change intervals, ferry authorization, annual requirements.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_aircraft_symptoms",
        "description": "Get observed symptoms for a specific aircraft (CDF Events type Observation, subtype Symptom) via SDK.",
        "input_schema": {
            "type": "object",
            "properties": {
                "aircraft_id": {"type": "string", "description": "Tail number, e.g. N8834Q"}
            },
            "required": ["aircraft_id"],
        },
    },
    {
        "name": "get_engine_type_history",
        "description": (
            "Given a tail number, find {TAIL}-ENGINE, follow IS_TYPE to the shared engine model asset, "
            "then list all other engines of that model and return chronological events "
            "(flights, squawks, maintenance) for each peer aircraft. "
            "Use when comparing in-flight symptoms to historical patterns on the same engine type."
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
            "Full-text search across all fleet events and symptoms. "
            "Searches pilot_notes, squawk descriptions, symptom text, observation fields. "
            "Use to find patterns across aircraft — e.g. 'elevated CHT rough running' "
            "will match N8834Q squawks AND N1156P pre-failure pilot notes and symptoms."
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
            "Returns: components, sensors, maintenance, squawks, symptoms, policies, documents. "
            "If the aircraft has any symptoms, the payload also includes symptomDeepDive: "
            "engineTypePeerHistory (IS_TYPE → shared engine model, chronological peer events) and "
            "fleetSearchFromSymptoms (keyword match across pilot notes, squawks, symptom text). "
            "Pass aircraft_id (tail number) to focus on a specific aircraft."
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
            "Fleet-wide context tool — summaries for all four aircraft + policies + alerts. "
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
        "get_aircraft_symptoms": lambda: get_aircraft_symptoms(tool_input["aircraft_id"]),
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
