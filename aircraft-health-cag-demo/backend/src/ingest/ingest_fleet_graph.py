"""
Fleet Graph Ingestion — extended knowledge graph nodes and relationships.

Creates the fleet-level knowledge graph structure from dataset constants:
  - FleetOwner node (Desert_Sky_Aviation)
  - OperationalPolicy nodes (oil change, grace period, annual, ferry)
  - SymptomNode for N8834Q and N1156P
  - GOVERNED_BY: each aircraft → FleetOwner
  - HAS_POLICY: FleetOwner → each policy
  - EXHIBITED: relevant flight events → symptoms
  - IS_TYPE: each {TAIL}-ENGINE → ENGINE_MODEL_LYC_O320_H2AD
  - HAS_COMPONENT: explicit hierarchy edges for graph traversal

Causal ordering is not encoded as graph edges — the agent uses
get_engine_type_history and chronological events instead.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from mock_cdf.store.store import (  # type: ignore[import]
    store,
    Relationship,
    SymptomNode,
    OperationalPolicy,
    FleetOwner,
)
from dataset import (  # type: ignore[import]
    TAILS,
    SYMPTOM_NODES,
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


def ingest_fleet_graph() -> None:
    """Ingest all fleet-level graph nodes and relationships."""

    fleet_owners = [FleetOwner(**FLEET_OWNER)]
    store.upsert_fleet_owners(fleet_owners)
    print(f"  Upserted {len(fleet_owners)} fleet owner nodes")

    policies = [OperationalPolicy(**p) for p in OPERATIONAL_POLICIES]
    store.upsert_policies(policies)
    print(f"  Upserted {len(policies)} operational policy nodes")

    symptoms = [SymptomNode(**s) for s in SYMPTOM_NODES]
    store.upsert_symptoms(symptoms)
    print(f"  Upserted {len(symptoms)} symptom nodes")

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
                targetType="asset",
                relationshipType="EXHIBITED",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

    n1156p_symptoms = [SYM_N1156P_CHT, SYM_N1156P_OIL, SYM_N1156P_ROUGH, SYM_N1156P_POWER]
    for fi in range(*N1156P_EXHIBITED_FLIGHT_RANGE):
        flight_ext_id = f"FLIGHT-N1156P-{fi:04d}"
        if fi >= 97:
            syms = [SYM_N1156P_ROUGH, SYM_N1156P_POWER, SYM_N1156P_CHT]
        elif fi >= 93:
            syms = [SYM_N1156P_CHT, SYM_N1156P_ROUGH, SYM_N1156P_OIL]
        else:
            syms = [SYM_N1156P_CHT, SYM_N1156P_OIL]
        for sym_id in syms:
            rels.append(Relationship(
                externalId=f"REL-{flight_ext_id}-EXHIBITED-{sym_id}",
                sourceExternalId=flight_ext_id,
                sourceType="event",
                targetExternalId=sym_id,
                targetType="asset",
                relationshipType="EXHIBITED",
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
    print(f"    EXHIBITED: {exhibited_count} edges (flights → symptoms)")
    print(f"    IS_TYPE: {len(TAILS)} edges (engine → {ENGINE_MODEL_EXT_ID})")
