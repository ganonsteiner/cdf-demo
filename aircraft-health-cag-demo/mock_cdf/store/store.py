"""
CDF Store — single in-memory store for the Desert Sky Aviation fleet.

Mirrors the Cognite Data Fusion resource model with JSON file persistence.
Each resource type corresponds to a CDF resource: Assets, TimeSeries,
Datapoints, Events, Relationships, and Files.

Extended with fleet-specific resource types (OperationalPolicy, FleetOwner) that are served via custom
POST list routes using httpx in agent tools. Observed symptoms are standard CDF Events (type Observation, subtype Symptom).

Thread-safe via threading.Lock for concurrent FastAPI request handling.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field

STORE_DIR = Path(__file__).parent


# ---------------------------------------------------------------------------
# Pydantic models — standard CDF resource types
# ---------------------------------------------------------------------------

class Asset(BaseModel):
    """Mirrors CDF Asset resource type — node in the asset hierarchy."""
    id: int
    externalId: str
    name: str
    description: Optional[str] = None
    parentId: Optional[int] = None
    parentExternalId: Optional[str] = None
    metadata: dict[str, str] = Field(default_factory=dict)
    createdTime: int = 0
    lastUpdatedTime: int = 0


class TimeSeries(BaseModel):
    """Mirrors CDF TimeSeries resource — sensor/metric metadata."""
    id: int
    externalId: str
    name: str
    description: Optional[str] = None
    assetId: Optional[int] = None
    unit: Optional[str] = None
    isString: bool = False
    metadata: dict[str, str] = Field(default_factory=dict)
    createdTime: int = 0
    lastUpdatedTime: int = 0


class Datapoint(BaseModel):
    """Single time series data point — OT sensor reading."""
    timestamp: int
    value: float


class CdfEvent(BaseModel):
    """Mirrors CDF Event resource — maintenance records, squawks, inspections, flights."""
    id: int
    externalId: str
    type: str
    subtype: Optional[str] = None
    description: Optional[str] = None
    startTime: Optional[int] = None
    endTime: Optional[int] = None
    assetIds: list[int] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    source: Optional[str] = None
    createdTime: int = 0
    lastUpdatedTime: int = 0


class Relationship(BaseModel):
    """Mirrors CDF Relationship resource — directed graph edge between resources."""
    externalId: str
    sourceExternalId: str
    sourceType: str
    targetExternalId: str
    targetType: str
    relationshipType: Optional[str] = None
    confidence: float = 1.0
    createdTime: int = 0
    lastUpdatedTime: int = 0


class CdfFile(BaseModel):
    """Mirrors CDF File resource — linked documents (POH, ADs, SBs)."""
    id: int
    externalId: str
    name: str
    mimeType: Optional[str] = None
    assetIds: list[int] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    uploaded: bool = True
    createdTime: int = 0
    lastUpdatedTime: int = 0


# ---------------------------------------------------------------------------
# Extended fleet resource types — served via custom POST list routes
# ---------------------------------------------------------------------------


class OperationalPolicy(BaseModel):
    """Fleet policy governing maintenance intervals, ferry authorization, etc."""
    externalId: str
    title: str
    description: str
    rule: str = ""
    category: str = ""
    references: str = ""
    type: str = "OperationalPolicy"


class FleetOwner(BaseModel):
    """Fleet management entity — owns and governs all aircraft in the fleet."""
    externalId: str
    name: str
    description: str = ""
    location: str = ""
    contact: str = ""
    type: str = "FleetOwner"


# ---------------------------------------------------------------------------
# Store singleton
# ---------------------------------------------------------------------------

class CdfStore:
    """
    Thread-safe JSON persistence layer for all CDF resource types.

    Single active store — no multi-state routing. One events.json and one
    datapoints.json covering the entire Desert Sky Aviation fleet.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._assets: dict[str, Asset] = {}
        self._timeseries: dict[str, TimeSeries] = {}
        self._datapoints: dict[str, list[Datapoint]] = {}
        self._events: dict[str, CdfEvent] = {}
        self._relationships: dict[str, Relationship] = {}
        self._files: dict[str, CdfFile] = {}
        # Extended fleet resources
        self._policies: dict[str, OperationalPolicy] = {}
        self._fleet_owners: dict[str, FleetOwner] = {}
        self.init()

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _read_json(self, filename: str) -> list[dict[str, Any]]:
        path = STORE_DIR / filename
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError):
            return []

    def _write_json(self, filename: str, data: list[Any]) -> None:
        path = STORE_DIR / filename
        path.write_text(json.dumps(data, indent=2, default=str))

    def init(self) -> None:
        """Load all resource stores from disk into memory."""
        with self._lock:
            self._assets = {
                a["externalId"]: Asset(**a)
                for a in self._read_json("assets.json")
            }
            self._timeseries = {
                ts["externalId"]: TimeSeries(**ts)
                for ts in self._read_json("timeseries.json")
            }
            self._relationships = {
                r["externalId"]: Relationship(**r)
                for r in self._read_json("relationships.json")
            }
            self._files = {
                f["externalId"]: CdfFile(**f)
                for f in self._read_json("files.json")
            }

            # Events — single unified store
            self._events = {
                e["externalId"]: CdfEvent(**e)
                for e in self._read_json("events.json")
            }

            # Datapoints — single unified store
            dp_map: dict[str, list[Datapoint]] = {}
            for entry in self._read_json("datapoints.json"):
                ext_id = entry.get("externalId", "")
                dp_map[ext_id] = [Datapoint(**p) for p in entry.get("datapoints", [])]
            self._datapoints = dp_map

            # Extended fleet resources
            self._policies = {
                p["externalId"]: OperationalPolicy(**p)
                for p in self._read_json("policies.json")
            }
            self._fleet_owners = {
                fo["externalId"]: FleetOwner(**fo)
                for fo in self._read_json("fleet_owners.json")
            }

    # ------------------------------------------------------------------
    # Flush helpers
    # ------------------------------------------------------------------

    def _flush_assets(self) -> None:
        self._write_json("assets.json", [a.model_dump() for a in self._assets.values()])

    def _flush_timeseries(self) -> None:
        self._write_json("timeseries.json", [ts.model_dump() for ts in self._timeseries.values()])

    def _flush_datapoints(self) -> None:
        records = [
            {"externalId": ext_id, "datapoints": [dp.model_dump() for dp in dps]}
            for ext_id, dps in self._datapoints.items()
        ]
        self._write_json("datapoints.json", records)

    def _flush_events(self) -> None:
        self._write_json("events.json", [e.model_dump() for e in self._events.values()])

    def _flush_relationships(self) -> None:
        self._write_json("relationships.json", [r.model_dump() for r in self._relationships.values()])

    def _flush_files(self) -> None:
        self._write_json("files.json", [f.model_dump() for f in self._files.values()])

    def _flush_policies(self) -> None:
        self._write_json("policies.json", [p.model_dump() for p in self._policies.values()])

    def _flush_fleet_owners(self) -> None:
        self._write_json("fleet_owners.json", [fo.model_dump() for fo in self._fleet_owners.values()])

    # ------------------------------------------------------------------
    # Asset methods
    # ------------------------------------------------------------------

    def get_assets(self) -> list[Asset]:
        with self._lock:
            return list(self._assets.values())

    def get_asset(self, external_id: str) -> Optional[Asset]:
        with self._lock:
            return self._assets.get(external_id)

    def get_asset_by_id(self, asset_id: int) -> Optional[Asset]:
        with self._lock:
            return next((a for a in self._assets.values() if a.id == asset_id), None)

    def upsert_asset(self, asset: Asset) -> Asset:
        with self._lock:
            self._assets[asset.externalId] = asset
            self._flush_assets()
            return asset

    def upsert_assets(self, assets: list[Asset]) -> None:
        with self._lock:
            for asset in assets:
                self._assets[asset.externalId] = asset
            self._flush_assets()

    def get_asset_subtree(self, external_id: str) -> list[Asset]:
        """Breadth-first traversal of the asset hierarchy from the given root."""
        with self._lock:
            root = self._assets.get(external_id)
            if not root:
                return []
            result: list[Asset] = []
            queue = [root]
            while queue:
                current = queue.pop(0)
                result.append(current)
                children = [
                    a for a in self._assets.values()
                    if a.parentExternalId == current.externalId
                ]
                queue.extend(children)
            return result

    # ------------------------------------------------------------------
    # TimeSeries methods
    # ------------------------------------------------------------------

    def get_timeseries(self) -> list[TimeSeries]:
        with self._lock:
            return list(self._timeseries.values())

    def get_time_series_by_id(self, external_id: str) -> Optional[TimeSeries]:
        with self._lock:
            return self._timeseries.get(external_id)

    def upsert_time_series(self, ts: TimeSeries) -> TimeSeries:
        with self._lock:
            self._timeseries[ts.externalId] = ts
            self._flush_timeseries()
            return ts

    def upsert_timeseries(self, items: list[TimeSeries]) -> None:
        with self._lock:
            for ts in items:
                self._timeseries[ts.externalId] = ts
            self._flush_timeseries()

    # ------------------------------------------------------------------
    # Datapoint methods
    # ------------------------------------------------------------------

    def get_datapoints(
        self,
        external_id: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 1000,
    ) -> list[Datapoint]:
        with self._lock:
            points = self._datapoints.get(external_id, [])
            if start is not None:
                points = [p for p in points if p.timestamp >= start]
            if end is not None:
                points = [p for p in points if p.timestamp <= end]
            return points[:limit]

    def get_latest_datapoint(self, external_id: str) -> Optional[Datapoint]:
        with self._lock:
            points = self._datapoints.get(external_id, [])
            if not points:
                return None
            return max(points, key=lambda p: p.timestamp)

    def append_datapoints(self, external_id: str, points: list[Datapoint]) -> None:
        with self._lock:
            if external_id not in self._datapoints:
                self._datapoints[external_id] = []
            self._datapoints[external_id].extend(points)
            self._flush_datapoints()

    def set_datapoints(self, external_id: str, points: list[Datapoint]) -> None:
        with self._lock:
            self._datapoints[external_id] = points
            self._flush_datapoints()

    # ------------------------------------------------------------------
    # Event methods
    # ------------------------------------------------------------------

    def get_events(self) -> list[CdfEvent]:
        with self._lock:
            return list(self._events.values())

    def get_event(self, external_id: str) -> Optional[CdfEvent]:
        with self._lock:
            return self._events.get(external_id)

    def upsert_event(self, event: CdfEvent) -> CdfEvent:
        with self._lock:
            self._events[event.externalId] = event
            self._flush_events()
            return event

    def upsert_events(self, events: list[CdfEvent]) -> None:
        with self._lock:
            for event in events:
                self._events[event.externalId] = event
            self._flush_events()

    def delete_maintenance_ingest_for_tail(self, tail: str) -> None:
        """
        Remove maintenance CSV–sourced events for one aircraft and any relationships
        that reference those events.

        Maintenance externalIds embed the CSV ``date``; re-ingesting after a date change
        would otherwise leave stale events alongside new ones (duplicate history rows).
        """
        with self._lock:
            removed: set[str] = set()
            for ext_id, ev in list(self._events.items()):
                if ev.source != "maintenance_log_it":
                    continue
                if (ev.metadata or {}).get("tail") != tail:
                    continue
                del self._events[ext_id]
                removed.add(ext_id)
            if not removed:
                return
            for rel_id, rel in list(self._relationships.items()):
                if rel.sourceExternalId in removed or rel.targetExternalId in removed:
                    del self._relationships[rel_id]
            self._flush_events()
            self._flush_relationships()

    # ------------------------------------------------------------------
    # Relationship methods
    # ------------------------------------------------------------------

    def get_relationships(self) -> list[Relationship]:
        with self._lock:
            return list(self._relationships.values())

    def get_relationships_for_node(
        self,
        external_id: str,
        relationship_type: Optional[str] = None,
        direction: str = "both",
    ) -> list[Relationship]:
        """
        Return relationships where external_id is source, target, or both.

        direction='outbound' — edges where this node is the source
        direction='inbound'  — edges where this node is the target
        direction='both'     — union (default, required for fleet/policy traversal)
        """
        with self._lock:
            results = []
            for rel in self._relationships.values():
                is_source = rel.sourceExternalId == external_id
                is_target = rel.targetExternalId == external_id
                if direction == "outbound" and not is_source:
                    continue
                if direction == "inbound" and not is_target:
                    continue
                if direction == "both" and not (is_source or is_target):
                    continue
                if relationship_type and rel.relationshipType != relationship_type:
                    continue
                results.append(rel)
            return results

    def upsert_relationship(self, rel: Relationship) -> Relationship:
        with self._lock:
            self._relationships[rel.externalId] = rel
            self._flush_relationships()
            return rel

    def upsert_relationships(self, rels: list[Relationship]) -> None:
        with self._lock:
            for rel in rels:
                self._relationships[rel.externalId] = rel
            self._flush_relationships()

    # ------------------------------------------------------------------
    # File methods
    # ------------------------------------------------------------------

    def get_files(self) -> list[CdfFile]:
        with self._lock:
            return list(self._files.values())

    def get_file(self, external_id: str) -> Optional[CdfFile]:
        with self._lock:
            return self._files.get(external_id)

    def upsert_file(self, file: CdfFile) -> CdfFile:
        with self._lock:
            self._files[file.externalId] = file
            self._flush_files()
            return file

    def upsert_files(self, files: list[CdfFile]) -> None:
        with self._lock:
            for f in files:
                self._files[f.externalId] = f
            self._flush_files()

    # ------------------------------------------------------------------
    # Extended fleet resource methods
    # ------------------------------------------------------------------

    def get_policies(self) -> list[OperationalPolicy]:
        with self._lock:
            return list(self._policies.values())

    def upsert_policies(self, items: list[OperationalPolicy]) -> None:
        with self._lock:
            for p in items:
                self._policies[p.externalId] = p
            self._flush_policies()

    def get_fleet_owners(self) -> list[FleetOwner]:
        with self._lock:
            return list(self._fleet_owners.values())

    def upsert_fleet_owners(self, items: list[FleetOwner]) -> None:
        with self._lock:
            for fo in items:
                self._fleet_owners[fo.externalId] = fo
            self._flush_fleet_owners()

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Wipe all in-memory and on-disk data. Used by reset script."""
        with self._lock:
            self._assets = {}
            self._timeseries = {}
            self._datapoints = {}
            self._events = {}
            self._relationships = {}
            self._files = {}
            self._policies = {}
            self._fleet_owners = {}
            for filename in [
                "assets.json", "timeseries.json", "relationships.json", "files.json",
                "events.json", "datapoints.json",
                "policies.json", "fleet_owners.json",
            ]:
                self._write_json(filename, [])
            for legacy in ("findings.json", "conditions.json", "symptoms.json"):
                leg = STORE_DIR / legacy
                if leg.exists():
                    try:
                        leg.unlink()
                    except OSError:
                        pass

    def get_counts(self) -> dict[str, int]:
        """Return record counts for all resource types."""
        with self._lock:
            return {
                "assets": len(self._assets),
                "timeseries": len(self._timeseries),
                "datapoints": sum(len(v) for v in self._datapoints.values()),
                "events": len(self._events),
                "relationships": len(self._relationships),
                "files": len(self._files),
                "policies": len(self._policies),
                "fleet_owners": len(self._fleet_owners),
            }


# Module-level singleton — imported by all route handlers and the agent
store = CdfStore()
