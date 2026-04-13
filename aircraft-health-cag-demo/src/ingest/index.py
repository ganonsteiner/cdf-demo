"""
Ingestion Orchestrator — Desert Sky Aviation Fleet.

Runs all ingestion stages in order:
  1. Assets      — 4-aircraft hierarchy + fleet owner node
  2. Documents   — ET documents (POH, ADs, SBs)
  3. Flights     — OT sensor data, per-tail CSVs, flight events
  4. Maintenance — IT maintenance records, per-tail CSVs
  5. Fleet graph — policies, fleet owner
                   + relationships (GOVERNED_BY, HAS_POLICY, IS_TYPE, HAS_COMPONENT)

Usage:
  python -m src.ingest.index
  (from project root, after pip install -e .)
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Ensure scripts/ directory is importable for dataset.py
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))


def run_ingestion() -> None:
    start = time.time()
    print("\n✈  Desert Sky Aviation — Fleet Ingestion Pipeline\n")

    print("Stage 1/5: Assets — 4-aircraft hierarchy + fleet owner")
    from .ingest_assets import ingest_assets
    ingest_assets()

    print("\nStage 2/5: Documents — ET layer (POH, ADs, SBs)")
    from .ingest_documents import ingest_documents
    ingest_documents()

    print("\nStage 3/5: Flight Data (OT) — per-tail CSVs → TimeSeries + Events")
    from .ingest_flights import ingest_flights
    ingest_flights()

    print("\nStage 4/5: Maintenance Log (IT) — per-tail CSVs → Events + Relationships")
    from .ingest_maintenance import ingest_maintenance
    ingest_maintenance()

    print("\nStage 5/5: Fleet Graph — Observation events, policies, relationships (incl. IS_TYPE)")
    from .ingest_fleet_graph import ingest_fleet_graph
    ingest_fleet_graph()

    elapsed = time.time() - start
    print(f"\n✓ Ingestion complete in {elapsed:.1f}s")

    # Signal the mock CDF server to reload its in-memory store from disk
    import urllib.request
    cdf = os.getenv("CDF_BASE_URL", "http://localhost:4001").rstrip("/")
    mock_cdf_url = f"{cdf}/admin/reload"
    try:
        req = urllib.request.Request(mock_cdf_url, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=3) as resp:
            print(f"  Mock CDF store reloaded: {resp.read().decode()[:120]}")
    except Exception:
        print("  (mock CDF server not running — start it first, then re-run ingest)")
    print()


if __name__ == "__main__":
    run_ingestion()
