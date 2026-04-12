"""
Transform flight data → per-tail CSVs in data/.

Imports from dataset.py and writes flight_data_{TAIL}.csv for each of the
four aircraft. The CSV columns mirror the CDF TimeSeries / Events ingestion
schema used by ingest_flights.py.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

# Resolve paths relative to this script's location
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

sys.path.insert(0, str(SCRIPT_DIR))
from dataset import TAILS, generate_flights, get_demo_anchor  # noqa: E402


def write_flight_csv(tail: str) -> Path:
    """Generate and write flight CSV for one aircraft."""
    flights = generate_flights(tail)
    output_path = DATA_DIR / f"flight_data_{tail}.csv"

    if not flights:
        print(f"  [{tail}] No flights generated — skipping")
        return output_path

    fieldnames = [
        "timestamp", "hobbs_start", "hobbs_end", "tach_start", "tach_end",
        "route", "duration", "oil_pressure_min", "oil_pressure_max",
        "oil_temp_max", "cht_max", "egt_max", "fuel_used_gal", "cycles",
        "pilot_notes", "tail", "flight_index",
    ]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flights)

    print(f"  [{tail}] {len(flights)} flights → {output_path.name}")
    return output_path


def main() -> None:
    anchor = get_demo_anchor()
    env_used = os.environ.get("DESERT_SKY_DEMO_DATE", "").strip()
    src = "DESERT_SKY_DEMO_DATE" if env_used else "UTC today"
    print(f"Demo anchor (UTC date): {anchor.date().isoformat()} (source: {src})")
    print("Generating per-tail flight CSVs...")
    for tail in TAILS:
        write_flight_csv(tail)
    print("Done.")


if __name__ == "__main__":
    main()
