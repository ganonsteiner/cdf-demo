"""
Asset Ingestion — Desert Sky Aviation Fleet.

Seeds the asset hierarchy for all four Cessna 172N aircraft plus the
Desert_Sky_Aviation fleet owner node. Each aircraft has:
  {TAIL}                  — root aircraft asset
  {TAIL}-ENGINE           — Lycoming O-320-H2AD
  {TAIL}-ENGINE-CYLINDERS — cylinder assembly
  {TAIL}-ENGINE-OIL       — oil system
  {TAIL}-PROPELLER        — McCauley fixed-pitch prop
  {TAIL}-AIRFRAME         — fuselage, wings, empennage
  {TAIL}-AVIONICS         — avionics stack
  {TAIL}-FUEL-SYSTEM      — fuel system
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

# Project root on path for mock_cdf imports when running as a script
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from dataset import ENGINE_TACH_AT_OVERHAUL  # noqa: E402

from mock_cdf.store.store import store, Asset  # noqa: E402

NOW_MS = int(time.time() * 1000)

# Fleet aircraft definitions
FLEET: list[dict[str, Any]] = [
    {
        "tail": "N4798E",
        "name": "N4798E — 1978 Cessna 172N",
        "description": "1978 Cessna 172N, S/N 17270798. 380 SMOH. Based KPHX.",
        "serial": "17270798",
        "smoh": "380",
    },
    {
        "tail": "N2251K",
        "name": "N2251K — 1978 Cessna 172N",
        "description": "1978 Cessna 172N, S/N 17271243. 290 SMOH. Based KPHX.",
        "serial": "17271243",
        "smoh": "290",
    },
    {
        "tail": "N8834Q",
        "name": "N8834Q — 1978 Cessna 172N",
        "description": "1978 Cessna 172N, S/N 17273047. 198 SMOH. Based KPHX.",
        "serial": "17273047",
        "smoh": "198",
    },
    {
        "tail": "N1156P",
        "name": "N1156P — 1978 Cessna 172N",
        "description": "1978 Cessna 172N, S/N 17271891. 520 SMOH. Based KPHX.",
        "serial": "17271891",
        "smoh": "520",
    },
]


def _build_fleet_assets() -> list[dict[str, Any]]:
    """Build all asset records for the four-aircraft fleet."""
    assets: list[dict[str, Any]] = []
    asset_id = 1

    for i, aircraft in enumerate(FLEET):
        tail = aircraft["tail"]
        base_id = i * 10 + 1

        # Root aircraft asset
        assets.append({
            "id": base_id,
            "externalId": tail,
            "name": aircraft["name"],
            "description": aircraft["description"],
            "parentExternalId": None,
            "metadata": {
                "aircraft_type": "Cessna 172N",
                "year": "1978",
                "serial_number": aircraft["serial"],
                "tail_number": tail,
                "base_airport": "KPHX",
                "engine_type": "Lycoming O-320-H2AD",
                "engine_tbo_hours": "2000",
                "engine_smoh": aircraft["smoh"],
                "overhaul_tach": str(ENGINE_TACH_AT_OVERHAUL[tail]),
                "max_gross_weight_lbs": "2300",
                "operator": "Desert Sky Aviation",
            },
        })

        # Engine
        assets.append({
            "id": base_id + 1,
            "externalId": f"{tail}-ENGINE",
            "name": f"{tail} — Engine (Lycoming O-320-H2AD)",
            "description": f"Lycoming O-320-H2AD, 160 hp, 2000 hr TBOH, {aircraft['smoh']} SMOH",
            "parentExternalId": tail,
            "metadata": {
                "model": "O-320-H2AD",
                "hp": "160",
                "tbo_hours": "2000",
                "smoh": aircraft["smoh"],
                "fuel_type": "100LL",
            },
        })

        # Engine cylinders
        assets.append({
            "id": base_id + 2,
            "externalId": f"{tail}-ENGINE-CYLINDERS",
            "name": f"{tail} — Cylinders (4)",
            "description": "Four cylinders, Champion REM40E spark plugs, hydraulic barrel lifters (H2AD variant)",
            "parentExternalId": f"{tail}-ENGINE",
            "metadata": {"count": "4", "plug_model": "Champion REM40E"},
        })

        # Engine oil
        assets.append({
            "id": base_id + 3,
            "externalId": f"{tail}-ENGINE-OIL",
            "name": f"{tail} — Oil System",
            "description": "Wet sump, 8 qt capacity, 50-hr change interval per Lycoming SB 388C",
            "parentExternalId": f"{tail}-ENGINE",
            "metadata": {"capacity_qts": "8", "change_interval_hrs": "50"},
        })

        # Propeller
        assets.append({
            "id": base_id + 4,
            "externalId": f"{tail}-PROPELLER",
            "name": f"{tail} — Propeller (McCauley 1C235/DTM7557)",
            "description": "McCauley 1C235/DTM7557 fixed-pitch 2-blade, 2000 hr / 6 yr TBOH",
            "parentExternalId": tail,
            "metadata": {"model": "1C235/DTM7557", "tbo_hours": "2000", "tbo_years": "6"},
        })

        # Airframe
        assets.append({
            "id": base_id + 5,
            "externalId": f"{tail}-AIRFRAME",
            "name": f"{tail} — Airframe",
            "description": "Fuselage, wings, empennage, flight controls, landing gear",
            "parentExternalId": tail,
            "metadata": {"max_gross_weight_lbs": "2300", "vne_kias": "163"},
        })

        # Avionics
        assets.append({
            "id": base_id + 6,
            "externalId": f"{tail}-AVIONICS",
            "name": f"{tail} — Avionics",
            "description": "Comm radio, nav/VOR, transponder (24-mo inspection), ELT (12-mo battery)",
            "parentExternalId": tail,
            "metadata": {"transponder_inspection_months": "24", "elt_battery_months": "12"},
        })

        # Fuel system
        assets.append({
            "id": base_id + 7,
            "externalId": f"{tail}-FUEL-SYSTEM",
            "name": f"{tail} — Fuel System",
            "description": "Gravity-feed from two wing tanks, 43 gal usable, 100LL avgas",
            "parentExternalId": tail,
            "metadata": {"capacity_gal_usable": "43", "fuel_type": "100LL"},
        })

        asset_id = base_id + 8

    # Shared engine model — standalone asset (IS_TYPE from each {TAIL}-ENGINE)
    assets.append({
        "id": 101,
        "externalId": "ENGINE_MODEL_LYC_O320_H2AD",
        "name": "Lycoming O-320-H2AD",
        "description": "4-cylinder horizontally opposed aircraft engine, 160hp, 2000hr TBOH",
        "parentExternalId": None,
        "metadata": {
            "type": "EngineModel",
            "horsepower": "160",
            "tbo_hours": "2000",
        },
    })

    # Fleet owner node (id 100)
    assets.append({
        "id": 100,
        "externalId": "Desert_Sky_Aviation",
        "name": "Desert Sky Aviation",
        "description": "Flight school and aircraft rental operation at KPHX. Operates four 1978 Cessna 172N aircraft.",
        "parentExternalId": None,
        "metadata": {
            "location": "KPHX — Phoenix Sky Harbor International Airport",
            "contact": "ops@desertsky.aero",
            "type": "FleetOperator",
        },
    })

    return assets


def ingest_assets() -> None:
    """Seed all fleet asset nodes into the mock CDF store."""
    asset_defs = _build_fleet_assets()
    assets = [
        Asset(
            id=a["id"],
            externalId=a["externalId"],
            name=a["name"],
            description=a.get("description"),
            parentExternalId=a.get("parentExternalId"),
            metadata=a.get("metadata", {}),
            createdTime=NOW_MS,
            lastUpdatedTime=NOW_MS,
        )
        for a in asset_defs
    ]
    store.upsert_assets(assets)
    print(f"  Upserted {len(assets)} assets (4 aircraft × 8 nodes + engine model + fleet owner)")


if __name__ == "__main__":
    ingest_assets()
