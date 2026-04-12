"""
Fleet Graph Ingestion — extended knowledge graph nodes and relationships.

Creates the fleet-level knowledge graph structure from dataset constants:
  - FleetOwner node (Desert_Sky_Aviation)
  - OperationalPolicy nodes (oil change, grace period, annual, ferry)
  - Observation/Symptom CDF Events for N8834Q and N1156P (from get_symptom_nodes())
  - GOVERNED_BY: each aircraft → FleetOwner
  - HAS_POLICY: FleetOwner → each policy
  - EXHIBITED: relevant flight events → Observation events
  - OBSERVED_ON: aircraft root asset → Observation/Symptom event (graph + traversal)
  - IS_TYPE: each {TAIL}-ENGINE → ENGINE_MODEL_LYC_O320_H2AD
  - HAS_COMPONENT: explicit hierarchy edges for graph traversal

Causal ordering is not encoded as graph edges — the agent uses
get_engine_type_history and chronological events instead.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from mock_cdf.store.store import (  # type: ignore[import]
    store,
    Relationship,
    CdfEvent,
    OperationalPolicy,
    FleetOwner,
)
from dataset import (  # type: ignore[import]
    TAILS,
    get_symptom_nodes,
    OPERATIONAL_POLICIES,
    FLEET_OWNER,
    SYM_N8834Q_CHT,
    SYM_N8834Q_MAG,
    SYM_N1156P_CHT,
    SYM_N1156P_OIL,
    SYM_N1156P_ROUGH,
    SYM_N1156P_POWER,
    FLEET_OWNER_ID,
    N1156P_EXHIBITED_FLIGHT_RANGE,
    N8834Q_EXHIBITED_FLIGHT_RANGE,
)

ENGINE_MODEL_EXT_ID = "ENGINE_MODEL_LYC_O320_H2AD"

NOW_MS = int(time.time() * 1000)


def _flights_affected_for_symptom(external_id: str) -> str:
    """Approximate flights_affected metadata per symptom external ID."""
    if external_id in (SYM_N8834Q_CHT, SYM_N8834Q_MAG):
        return "3"
    if external_id == SYM_N1156P_CHT:
        return "9"
    if external_id == SYM_N1156P_OIL:
        return "3"
    if external_id == SYM_N1156P_ROUGH:
        return "5"
    if external_id == SYM_N1156P_POWER:
        return "3"
    return "1"


def _symptom_nodes_to_observation_events() -> list[CdfEvent]:
    """Build CDF Observation/Symptom events from dataset get_symptom_nodes()."""
    existing = store.get_events()
    max_id = max((e.id for e in existing), default=0)
    events: list[CdfEvent] = []
    for i, s in enumerate(get_symptom_nodes()):
        ext = s["externalId"]
        tail = s["aircraft_id"]
        asset = store.get_asset(tail)
        if not asset:
            raise RuntimeError(f"Ingest fleet graph: asset {tail} not found for symptom {ext}")
        first = str(s.get("first_observed") or "")
        try:
            dt = datetime.strptime(first, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            start_ms = int(dt.timestamp() * 1000)
        except ValueError:
            start_ms = NOW_MS
        sev = str(s.get("severity") or "caution")
        policy_trigger = "SYMPTOM_ESCALATION" if sev in ("warning", "critical") else "SYMPTOM_MONITOR"
        meta = {
            "aircraft_id": tail,
            "title": str(s.get("title") or ""),
            "observation": str(s.get("observation") or ""),
            "severity": sev,
            "first_observed": first,
            "policy_trigger": policy_trigger,
            "flights_affected": _flights_affected_for_symptom(ext),
        }
        events.append(CdfEvent(
            id=max_id + 1 + i,
            externalId=ext,
            type="Observation",
            subtype="Symptom",
            description=str(s.get("description") or ""),
            startTime=start_ms,
            endTime=start_ms,
            assetIds=[asset.id],
            metadata=meta,
            source="fleet_graph_observation",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))
    return events


def ingest_fleet_graph() -> None:
    """Ingest all fleet-level graph nodes and relationships."""

    fleet_owners = [FleetOwner(**FLEET_OWNER)]
    store.upsert_fleet_owners(fleet_owners)
    print(f"  Upserted {len(fleet_owners)} fleet owner nodes")

    policies = [OperationalPolicy(**p) for p in OPERATIONAL_POLICIES]
    store.upsert_policies(policies)
    print(f"  Upserted {len(policies)} operational policy nodes")

    obs_events = _symptom_nodes_to_observation_events()
    store.upsert_events(obs_events)
    print(f"  Upserted {len(obs_events)} Observation/Symptom events")

    rels: list[Relationship] = []

    for tail in TAILS:
        rels.append(Relationship(
            externalId=f"REL-{tail}-GOVERNED_BY-{FLEET_OWNER_ID}",
            sourceExternalId=tail,
            sourceType="asset",
            targetExternalId=FLEET_OWNER_ID,
            targetType="asset",
            relationshipType="GOVERNED_BY",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

    for policy in OPERATIONAL_POLICIES:
        pol_id = policy["externalId"]
        rels.append(Relationship(
            externalId=f"REL-{FLEET_OWNER_ID}-HAS_POLICY-{pol_id}",
            sourceExternalId=FLEET_OWNER_ID,
            sourceType="asset",
            targetExternalId=pol_id,
            targetType="asset",
            relationshipType="HAS_POLICY",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

    n8834q_symptoms = [SYM_N8834Q_CHT, SYM_N8834Q_MAG]
    for fi in range(*N8834Q_EXHIBITED_FLIGHT_RANGE):
        flight_ext_id = f"FLIGHT-N8834Q-{fi:04d}"
        for sym_id in n8834q_symptoms:
            rels.append(Relationship(
                externalId=f"REL-{flight_ext_id}-EXHIBITED-{sym_id}",
                sourceExternalId=flight_ext_id,
                sourceType="event",
                targetExternalId=sym_id,
                targetType="event",
                relationshipType="EXHIBITED",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

    n1156p_ex_lo, n1156p_ex_hi = N1156P_EXHIBITED_FLIGHT_RANGE
    n1156p_n_exhibit = n1156p_ex_hi - n1156p_ex_lo
    for fi in range(*N1156P_EXHIBITED_FLIGHT_RANGE):
        flight_ext_id = f"FLIGHT-N1156P-{fi:04d}"
        rel = fi - n1156p_ex_lo
        if rel >= n1156p_n_exhibit - 3:
            syms = [SYM_N1156P_ROUGH, SYM_N1156P_POWER, SYM_N1156P_CHT]
        elif rel >= n1156p_n_exhibit - 6:
            syms = [SYM_N1156P_CHT, SYM_N1156P_ROUGH, SYM_N1156P_OIL]
        else:
            syms = [SYM_N1156P_CHT, SYM_N1156P_OIL]
        for sym_id in syms:
            rels.append(Relationship(
                externalId=f"REL-{flight_ext_id}-EXHIBITED-{sym_id}",
                sourceExternalId=flight_ext_id,
                sourceType="event",
                targetExternalId=sym_id,
                targetType="event",
                relationshipType="EXHIBITED",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

    # Aircraft → symptom (Observation event) for knowledge graph and SDK traversal
    for s in get_symptom_nodes():
        tail = str(s.get("aircraft_id") or "")
        sym_ext = str(s.get("externalId") or "")
        if not tail or not sym_ext:
            continue
        rels.append(Relationship(
            externalId=f"REL-{tail}-OBSERVED_ON-{sym_ext}",
            sourceExternalId=tail,
            sourceType="asset",
            targetExternalId=sym_ext,
            targetType="event",
            relationshipType="OBSERVED_ON",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

    for tail in TAILS:
        eng = f"{tail}-ENGINE"
        rels.append(Relationship(
            externalId=f"REL-{eng}-IS_TYPE-{ENGINE_MODEL_EXT_ID}",
            sourceExternalId=eng,
            sourceType="asset",
            targetExternalId=ENGINE_MODEL_EXT_ID,
            targetType="asset",
            relationshipType="IS_TYPE",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

    component_suffixes = [
        "-ENGINE", "-ENGINE-CYLINDERS", "-ENGINE-OIL",
        "-PROPELLER", "-AIRFRAME", "-AVIONICS", "-FUEL-SYSTEM",
    ]
    for tail in TAILS:
        for suffix in component_suffixes:
            comp_id = f"{tail}{suffix}"
            parent_id = tail if suffix in ("-ENGINE", "-PROPELLER", "-AIRFRAME", "-AVIONICS", "-FUEL-SYSTEM") else f"{tail}-ENGINE"
            rels.append(Relationship(
                externalId=f"REL-{parent_id}-HAS_COMPONENT-{comp_id}",
                sourceExternalId=parent_id,
                sourceType="asset",
                targetExternalId=comp_id,
                targetType="asset",
                relationshipType="HAS_COMPONENT",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

    store.upsert_relationships(rels)
    print(f"  Upserted {len(rels)} fleet graph relationships")
    print(f"    GOVERNED_BY: {len(TAILS)} edges (aircraft → FleetOwner)")
    print(f"    HAS_POLICY: {len(OPERATIONAL_POLICIES)} edges (FleetOwner → policy)")
    exhibited_count = sum(1 for r in rels if r.relationshipType == "EXHIBITED")
    observed_on_count = sum(1 for r in rels if r.relationshipType == "OBSERVED_ON")
    print(f"    EXHIBITED: {exhibited_count} edges (flights → Observation events)")
    print(f"    OBSERVED_ON: {observed_on_count} edges (aircraft → Observation/Symptom)")
    print(f"    IS_TYPE: {len(TAILS)} edges (engine → {ENGINE_MODEL_EXT_ID})")
