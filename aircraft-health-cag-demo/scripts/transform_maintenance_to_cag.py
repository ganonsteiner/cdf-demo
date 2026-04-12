"""
Transform maintenance data → per-tail CSVs in data/.

Imports from dataset.py and writes maintenance_{TAIL}.csv for each of the
four aircraft. The CSV columns mirror the CDF Events ingestion schema used
by ingest_maintenance.py.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

sys.path.insert(0, str(SCRIPT_DIR))
from dataset import TAILS, get_all_maintenance, get_demo_anchor  # noqa: E402


FIELDNAMES = [
    "date", "component_id", "maintenance_type", "description",
    "hobbs_at_service", "tach_at_service", "next_due_hobbs", "next_due_tach", "next_due_date",
    "mechanic", "inspector", "ad_reference", "sb_reference",
    "squawk_id", "resolved_by", "parts_replaced", "labor_hours", "signoff_type",
    "severity", "status",
]


def write_maintenance_csv(tail: str) -> Path:
    """Write maintenance CSV for one aircraft."""
    records = get_all_maintenance(tail)
    output_path = DATA_DIR / f"maintenance_{tail}.csv"

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            # Fill missing optional columns with empty string
            row = {k: r.get(k, "") for k in FIELDNAMES}
            writer.writerow(row)

    print(f"  [{tail}] {len(records)} maintenance records → {output_path.name}")
    return output_path


def main() -> None:
    anchor = get_demo_anchor()
    env_used = os.environ.get("DESERT_SKY_DEMO_DATE", "").strip()
    src = "DESERT_SKY_DEMO_DATE" if env_used else "UTC today"
    print(f"Demo anchor (UTC date): {anchor.date().isoformat()} (source: {src})")
    print("Generating per-tail maintenance CSVs...")
    for tail in TAILS:
        write_maintenance_csv(tail)
    print("Done.")


if __name__ == "__main__":
    main()
