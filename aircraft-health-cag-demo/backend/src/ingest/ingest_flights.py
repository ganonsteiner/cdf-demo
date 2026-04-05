"""
Flight Data Ingestion — Desert Sky Aviation Fleet.

Parses data/flight_data_{TAIL}.csv (OT source) for each of the four
aircraft and creates:
  - Per-tail TimeSeries nodes (e.g. N4798E.aircraft.hobbs)
  - Per-tail Datapoints for each sensor reading
  - Flight CDF Events with pilot_notes and route in metadata

Per-tail TimeSeries external IDs avoid collisions between aircraft.
Flight Events enable searchFleetForSimilarEvents full-text search over
pilot_notes field stored in event metadata.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"
NOW_MS = int(time.time() * 1000)

# Sensor columns mapped to per-tail TS suffix
SENSOR_COLUMNS: dict[str, tuple[str, str, str]] = {
    # csv_col: (ts_suffix, name, unit)
    "hobbs_end":        ("aircraft.hobbs",          "Hobbs Time",               "hours"),
    "tach_end":         ("aircraft.tach",            "Tach Time",                "hours"),
    "cycles":           ("aircraft.cycles",          "Landing Cycles",           "cycles"),
    "fuel_used_gal":    ("aircraft.fuel_used",       "Fuel Used Per Flight",     "gal"),
    "oil_pressure_min": ("engine.oil_pressure_min",  "Oil Pressure Min",         "psi"),
    "oil_pressure_max": ("engine.oil_pressure_max",  "Oil Pressure Max",         "psi"),
    "oil_temp_max":     ("engine.oil_temp_max",      "Oil Temp Max",             "°F"),
    "cht_max":          ("engine.cht_max",            "CHT Max",                  "°F"),
    "egt_max":          ("engine.egt_max",            "EGT Max",                  "°F"),
}

# TS asset ownership: suffix prefix → component suffix
TS_ASSET_OWNER: dict[str, str] = {
    "aircraft.": "",          # root aircraft asset
    "engine.":   "-ENGINE",   # engine sub-asset
}


def _ts_external_id(tail: str, suffix: str) -> str:
    return f"{tail}.{suffix}"


def _asset_external_id_for_ts(tail: str, ts_suffix: str) -> str:
    """Return the asset externalId that owns this time series."""
    if ts_suffix.startswith("engine."):
        return f"{tail}-ENGINE"
    return tail


def _ts_id_offset(tail: str) -> int:
    offsets = {"N4798E": 200, "N2251K": 300, "N8834Q": 400, "N1156P": 500}
    return offsets.get(tail, 200)


def ingest_flights_for_tail(tail: str) -> None:
    """Ingest flights for one aircraft tail."""
    from mock_cdf.store.store import store, TimeSeries, Datapoint, CdfEvent  # type: ignore[import]

    csv_path = DATA_DIR / f"flight_data_{tail}.csv"
    if not csv_path.exists():
        print(f"  [{tail}] ✗ {csv_path.name} not found — run 'npm run generate' first")
        return

    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # --- TimeSeries definitions ---
    id_base = _ts_id_offset(tail)
    ts_list = []
    for i, (col, (suffix, name, unit)) in enumerate(SENSOR_COLUMNS.items()):
        asset_ext_id = _asset_external_id_for_ts(tail, suffix)
        asset = store.get_asset(asset_ext_id)
        asset_id = asset.id if asset else None
        ts_list.append(TimeSeries(
            id=id_base + i,
            externalId=_ts_external_id(tail, suffix),
            name=f"{tail} {name}",
            description=f"{name} for {tail}",
            assetId=asset_id,
            unit=unit,
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))
    store.upsert_timeseries(ts_list)

    # --- Datapoints ---
    for col, (suffix, _, _) in SENSOR_COLUMNS.items():
        if col not in df.columns:
            continue
        ts_ext_id = _ts_external_id(tail, suffix)
        points = []
        for _, row in df.iterrows():
            val = row.get(col)
            if pd.isna(val):
                continue
            ts_ms = int(row["timestamp"].timestamp() * 1000)
            points.append(Datapoint(timestamp=ts_ms, value=float(val)))
        store.set_datapoints(ts_ext_id, points)

    # --- Flight Events (for full-text search by agent) ---
    aircraft_asset = store.get_asset(tail)
    aircraft_db_id = aircraft_asset.id if aircraft_asset else None

    event_id_base = id_base * 1000
    events: list[CdfEvent] = []
    for idx, row in df.iterrows():
        ts_ms = int(row["timestamp"].timestamp() * 1000)
        flight_idx = int(row.get("flight_index", idx))
        ext_id = f"FLIGHT-{tail}-{flight_idx:04d}"

        pilot_notes = str(row.get("pilot_notes", "") or "")
        route = str(row.get("route", "") or "")
        duration = float(row.get("duration", 0) or 0)
        cht_max = row.get("cht_max")
        oil_temp = row.get("oil_temp_max")

        meta: dict[str, str] = {
            "route": route,
            "pilot_notes": pilot_notes,
            "duration": str(round(duration, 2)),
            "hobbs_start": str(row.get("hobbs_start", "")),
            "hobbs_end": str(row.get("hobbs_end", "")),
            "tail": tail,
        }
        if not pd.isna(cht_max):
            meta["cht_max"] = str(round(float(cht_max), 1))
        if not pd.isna(oil_temp):
            meta["oil_temp_max"] = str(round(float(oil_temp), 1))

        # Mark anomalous flights for the agent
        is_anomalous = (
            (not pd.isna(cht_max) and float(cht_max) > 430) or
            (not pd.isna(oil_temp) and float(oil_temp) > 220)
        )
        if is_anomalous:
            meta["anomalous"] = "true"

        events.append(CdfEvent(
            id=event_id_base + flight_idx,
            externalId=ext_id,
            type="Flight",
            subtype=route,
            description=f"Flight {tail} — {route} — {duration:.1f}h" + (f" — {pilot_notes[:60]}" if pilot_notes else ""),
            startTime=ts_ms,
            endTime=ts_ms + int(duration * 3600 * 1000),
            assetIds=[aircraft_db_id] if aircraft_db_id else [],
            metadata=meta,
            source="flight_data_ot",
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        ))

    store.upsert_events(events)
    print(f"  [{tail}] {len(ts_list)} TS, {len(df)} datapoint rows, {len(events)} flight events")


def ingest_flights() -> None:
    """Ingest flights for all four aircraft."""
    from dataset import TAILS  # type: ignore[import]
    for tail in TAILS:
        ingest_flights_for_tail(tail)
