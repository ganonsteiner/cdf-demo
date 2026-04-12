"""
Maintenance Log Ingestion — Desert Sky Aviation Fleet.

Parses data/maintenance_{TAIL}.csv (IT source) for each of the four
aircraft and creates:
  - CDF Events (type=MaintenanceRecord, Squawk, or Inspection)
  - CDF Relationships: PERFORMED_ON, REFERENCES_AD, IDENTIFIED_ON

Squawk events store description + severity + status in metadata, enabling
full-text search by the agent's searchFleetForSimilarEvents tool.
"""

from __future__ import annotations

import time
from typing import Optional
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
NOW_MS = int(time.time() * 1000)


def _ad_reference_token_to_doc_external_id(token: str) -> Optional[str]:
    """
    Map a maintenance ad_reference fragment (e.g. 'AD 80-04-03 R2') to a CDF File externalId.
    Returns None if no matching ET document is ingested.
    """
    raw = token.strip()
    if not raw:
        return None
    low = raw.lower()
    if low.startswith("ad "):
        raw = raw[3:].strip()
    key = raw.strip().lower()
    doc_by_key: dict[str, str] = {
        "80-04-03 r2": "DOC-AD-80-04-03-R2",
        "2001-23-03": "DOC-AD-2001-23-03",
        "2011-10-09": "DOC-AD-2011-10-09",
        "90-06-03 r1": "DOC-AD-90-06-03-R1",
    }
    return doc_by_key.get(key)


def _date_to_ms(date_str: str) -> int:
    """Parse YYYY-MM-DD date string to milliseconds."""
    try:
        dt = datetime.strptime(str(date_str), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return NOW_MS


def ingest_maintenance_for_tail(tail: str, event_id_offset: int) -> int:
    """Ingest maintenance records for one aircraft. Returns next available event ID."""
    from mock_cdf.store.store import store, CdfEvent, Relationship  # type: ignore[import]

    csv_path = DATA_DIR / f"maintenance_{tail}.csv"
    if not csv_path.exists():
        print(f"  [{tail}] ✗ {csv_path.name} not found — run 'npm run generate' first")
        return event_id_offset

    df = pd.read_csv(csv_path).fillna("")

    aircraft_asset = store.get_asset(tail)
    aircraft_db_id = aircraft_asset.id if aircraft_asset else None

    events: list[CdfEvent] = []
    relationships: list[Relationship] = []
    event_id = event_id_offset

    for idx, row in df.iterrows():
        maint_type = str(row.get("maintenance_type", "")).strip().lower()
        date_str = str(row.get("date", "")).strip()
        component_id = str(row.get("component_id", "")).strip() or tail
        description = str(row.get("description", "")).strip()
        squawk_id = str(row.get("squawk_id", "")).strip()

        ts_ms = _date_to_ms(date_str)

        # Determine CDF event type and external ID
        if maint_type == "squawk":
            event_type = "Squawk"
            ext_id = squawk_id if squawk_id else f"SQ-{tail}-{idx:03d}"
            subtype = "squawk"
        elif maint_type in ("annual", "100hr", "progressive"):
            event_type = "Inspection"
            subtype = maint_type
            ext_id = f"INSP-{tail}-{date_str}"
        elif maint_type == "post_accident_inspection":
            event_type = "MaintenanceRecord"
            subtype = "post_accident_inspection"
            ext_id = f"MAINT-{tail}-postaccident-{date_str}"
        else:
            event_type = "MaintenanceRecord"
            subtype = maint_type
            ext_id = f"MAINT-{tail}-{maint_type}-{date_str}"

        # Resolve component asset for assetIds linking
        comp_asset = store.get_asset(component_id)
        asset_ids = []
        if comp_asset:
            asset_ids.append(comp_asset.id)
        if aircraft_db_id and aircraft_db_id not in asset_ids:
            asset_ids.append(aircraft_db_id)

        meta: dict[str, str] = {
            "component_id": component_id,
            "maintenance_type": maint_type,
            "date": date_str,
            "mechanic": str(row.get("mechanic", "")),
            "inspector": str(row.get("inspector", "")),
            "ad_reference": str(row.get("ad_reference", "")),
            "sb_reference": str(row.get("sb_reference", "")),
            "parts_replaced": str(row.get("parts_replaced", "")),
            "labor_hours": str(row.get("labor_hours", "")),
            "signoff_type": str(row.get("signoff_type", "")),
            "hobbs_at_service": str(row.get("hobbs_at_service", "")),
            "tach_at_service": str(row.get("tach_at_service", "")),
            "next_due_hobbs": str(row.get("next_due_hobbs", "")),
            "next_due_tach": str(row.get("next_due_tach", "")),
            "next_due_date": str(row.get("next_due_date", "")),
            "tail": tail,
        }

        # Squawk-specific metadata
        if event_type == "Squawk":
            meta["severity"] = str(row.get("severity", "non-grounding"))
            meta["status"] = str(row.get("status", "open"))

        events.append(CdfEvent(
            id=event_id,
            externalId=ext_id,
            type=event_type,
            subtype=subtype,
            description=description,
            startTime=ts_ms,
            assetIds=asset_ids,
            metadata=meta,
            source="maintenance_log_it",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

        # PERFORMED_ON relationship: event → component
        if comp_asset:
            relationships.append(Relationship(
                externalId=f"REL-{ext_id}-PERFORMED_ON",
                sourceExternalId=ext_id,
                sourceType="event",
                targetExternalId=component_id,
                targetType="asset",
                relationshipType="PERFORMED_ON",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

        # REFERENCES_AD relationships → real DOC-AD-* File external IDs
        ad_refs = str(row.get("ad_reference", ""))
        for ad_num in ad_refs.split(";"):
            ad_num = ad_num.strip()
            if not ad_num:
                continue
            doc_ext = _ad_reference_token_to_doc_external_id(ad_num)
            if not doc_ext:
                continue
            safe = doc_ext.replace("-", "_")
            relationships.append(Relationship(
                externalId=f"REL-{ext_id}-REFERENCES_AD-{safe}",
                sourceExternalId=ext_id,
                sourceType="event",
                targetExternalId=doc_ext,
                targetType="file",
                relationshipType="REFERENCES_AD",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

        # IDENTIFIED_ON for squawks
        if event_type == "Squawk" and comp_asset:
            relationships.append(Relationship(
                externalId=f"REL-{ext_id}-IDENTIFIED_ON",
                sourceExternalId=ext_id,
                sourceType="event",
                targetExternalId=component_id,
                targetType="asset",
                relationshipType="IDENTIFIED_ON",
                createdTime=NOW_MS,
                lastUpdatedTime=NOW_MS,
            ))

        event_id += 1

    store.upsert_events(events)
    store.upsert_relationships(relationships)
    print(f"  [{tail}] {len(events)} maintenance events, {len(relationships)} relationships")
    return event_id


def ingest_maintenance() -> None:
    """Ingest maintenance for all four aircraft."""
    from dataset import TAILS  # type: ignore[import]
    from mock_cdf.store.store import store  # type: ignore[import]

    for tail in TAILS:
        store.delete_maintenance_ingest_for_tail(tail)

    existing = store.get_events()
    event_id = max((e.id for e in existing), default=0) + 1
    for tail in TAILS:
        event_id = ingest_maintenance_for_tail(tail, event_id)
