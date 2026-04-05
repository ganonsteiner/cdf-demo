"""
Fleet Dataset — Desert Sky Aviation, KPHX.

Single source of truth for all four aircraft in the fleet:
  N4798E  — AIRWORTHY   (380 SMOH, 1 open non-grounding squawk, oil due in 18 hrs)
  N2251K  — FERRY ONLY  (290 SMOH, oil 1 hr overdue, one direct ferry flight permitted, 0 squawks)
  N8834Q  — CAUTION     (198 SMOH, elevated CHT #3 + rough mag check)
  N1156P  — NOT AIRWORTHY (catastrophic engine failure at ~520 SMOH, 6 months ago)

Uses numpy.random.default_rng(seed=42) for fully deterministic output.
Story-beat overrides are applied on top of the random baseline.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Fleet constants
# ---------------------------------------------------------------------------

TAILS: tuple[str, ...] = ("N4798E", "N2251K", "N8834Q", "N1156P")

# Generation parameters per tail
FLIGHT_COUNTS = {"N4798E": 70, "N2251K": 50, "N8834Q": 35, "N1156P": 100}

# Months of history per tail
HISTORY_MONTHS = {"N4798E": 14, "N2251K": 10, "N8834Q": 7, "N1156P": 20}

# Starting Hobbs per tail (reflects total time, engine overhaul history)
HOBBS_START = {"N4798E": 4730.0, "N2251K": 5090.0, "N8834Q": 4802.0, "N1156P": 5280.0}

# Engine overhaul Hobbs baseline per tail (Hobbs at last major overhaul)
OVERHAUL_HOBBS = {"N4798E": 4730.0 - 380.0, "N2251K": 5090.0 - 290.0, "N8834Q": 4802.0 - 198.0, "N1156P": 5280.0 - 520.0}

# Route weights: local, KSDL, KFLG, KPRC
ROUTE_CHOICES = ["KPHX-local", "KPHX-KSDL", "KPHX-KFLG", "KPHX-KPRC"]
ROUTE_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

# Route typical durations (hours)
ROUTE_DURATIONS = {
    "KPHX-local": (0.5, 1.2),
    "KPHX-KSDL": (0.4, 0.9),
    "KPHX-KFLG": (1.0, 2.0),
    "KPHX-KPRC": (0.8, 1.5),
}

ENGINE_TBO = 2000

# ---------------------------------------------------------------------------
# External ID constants for graph nodes (used by ingestion + agent)
# ---------------------------------------------------------------------------

# Symptom nodes
SYM_N8834Q_CHT = "Symptom_N8834Q_ElevatedCHT"
SYM_N8834Q_MAG = "Symptom_N8834Q_RoughMag"
SYM_N1156P_CHT = "Symptom_N1156P_ElevatedCHT"
SYM_N1156P_OIL = "Symptom_N1156P_OilConsumption"
SYM_N1156P_ROUGH = "Symptom_N1156P_RoughRunning"
SYM_N1156P_POWER = "Symptom_N1156P_PowerLoss"

# Policy nodes
POLICY_OIL_CHANGE = "Policy_OilChangeInterval"
POLICY_OIL_GRACE = "Policy_OilGracePeriod"
POLICY_ANNUAL = "Policy_AnnualInspection"
POLICY_FERRY = "Policy_FerryFlightOilOverdue"

# Fleet owner
FLEET_OWNER_ID = "Desert_Sky_Aviation"

# N1156P: flight event external IDs that exhibited symptoms (flights -10 to -2, no -1)
# These are computed at generation time; stored here as a reference pattern.
# Format: FLIGHT-{TAIL}-{INDEX:04d}
N1156P_EXHIBITED_FLIGHT_RANGE = (90, 99)  # indices 90..98 (0-based), flight -1 is index 99 (omitted)

# N8834Q: last 3 flights exhibit symptoms
N8834Q_EXHIBITED_FLIGHT_RANGE = (32, 35)  # indices 32, 33, 34 (0-based)

# ---------------------------------------------------------------------------
# Symptom / policy data
# ---------------------------------------------------------------------------

SYMPTOM_NODES: list[dict[str, Any]] = [
    {
        "externalId": SYM_N8834Q_CHT,
        "aircraft_id": "N8834Q",
        "title": "Elevated CHT #3",
        "description": "Cylinder head temperature on #3 cylinder running 40-60°F above the other cylinders during cruise. First observed three flights ago and trending upward.",
        "observation": "CHT #3 consistently reaching 430-450°F while CHT #1, #2, #4 remain 360-380°F at same power setting.",
        "severity": "caution",
        "first_observed": "2026-02-15",
    },
    {
        "externalId": SYM_N8834Q_MAG,
        "aircraft_id": "N8834Q",
        "title": "Rough Running on Left Mag",
        "description": "Engine runs noticeably rough during mag check on left magneto. Right mag check is smooth. RPM drop on left mag is 150 RPM versus 50 RPM on right.",
        "observation": "Mag check at runup: right mag 50 RPM drop (normal), left mag 150 RPM drop with roughness. Cleared after extended runup.",
        "severity": "caution",
        "first_observed": "2026-02-20",
    },
    {
        "externalId": SYM_N1156P_CHT,
        "aircraft_id": "N1156P",
        "title": "Persistently Elevated CHT",
        "description": "CHT readings trending upward over the previous 10 flights before failure. Reached 460-480°F on final flights.",
        "observation": "Pilot notes documented persistent high CHT during cruise, particularly on longer flights. Engine leaned aggressively to manage temperatures.",
        "severity": "warning",
        "first_observed": "2025-08-01",
    },
    {
        "externalId": SYM_N1156P_OIL,
        "aircraft_id": "N1156P",
        "title": "Increased Oil Consumption",
        "description": "Oil consumption increasing over the 3 months preceding failure. Required adding 1 qt oil every 8-10 flight hours versus normal 15-20 hours.",
        "observation": "Pre-flight checks documented oil level dropping faster than historical baseline. No visible external leaks found.",
        "severity": "warning",
        "first_observed": "2025-07-15",
    },
    {
        "externalId": SYM_N1156P_ROUGH,
        "aircraft_id": "N1156P",
        "title": "Intermittent Rough Running",
        "description": "Rough engine operation noted intermittently during cruise flight, particularly at lean mixture settings. Cleared when mixture enriched.",
        "observation": "Pilot notes on multiple flights mention roughness that cleared with mixture enrichment. Attributed to improper leaning technique at the time.",
        "severity": "warning",
        "first_observed": "2025-09-01",
    },
    {
        "externalId": SYM_N1156P_POWER,
        "aircraft_id": "N1156P",
        "title": "Reduced Power Output",
        "description": "Climb performance noticeably reduced on final flights before failure. Unable to maintain normal cruise RPM at full throttle.",
        "observation": "Pilot reported inability to reach normal cruise RPM. Takeoff roll longer than normal. Climb rate approximately 400 FPM versus normal 770 FPM.",
        "severity": "critical",
        "first_observed": "2025-09-20",
    },
]
OPERATIONAL_POLICIES: list[dict[str, Any]] = [
    {
        "externalId": POLICY_OIL_CHANGE,
        "title": "Oil Change Interval",
        "description": "All Desert Sky Aviation aircraft require oil and filter change every 50 Hobbs hours or 4 calendar months, whichever comes first. References Lycoming SB 388C.",
        "rule": "oil_change_hobbs_interval=50; oil_change_calendar_months=4",
        "category": "engine_maintenance",
        "references": "Lycoming SB 388C; FAA AC 43.13-1B",
    },
    {
        "externalId": POLICY_OIL_GRACE,
        "title": "Oil Change Grace Period — Ferry Flight Authorization",
        "description": "Aircraft with oil change overdue by 1-5 Hobbs hours may conduct a single direct ferry flight to the maintenance facility. The PIC must document the ferry flight in the aircraft journey log. Aircraft with oil overdue by more than 5 hours are NOT AIRWORTHY.",
        "rule": "ferry_authorized_if_oil_overdue_hours_between=1,5; ferry_not_authorized_if_oil_overdue_hours_gt=5",
        "category": "airworthiness",
        "references": "Desert Sky Aviation Operations Manual Rev 4.2",
    },
    {
        "externalId": POLICY_ANNUAL,
        "title": "Annual Inspection Currency",
        "description": "All aircraft must maintain current annual inspection per 14 CFR 91.409. Annual inspection must be completed by a certificated A&P/IA. Aircraft with expired annual are NOT AIRWORTHY.",
        "rule": "annual_inspection_required_calendar_months=12",
        "category": "airworthiness",
        "references": "14 CFR 91.409; Desert Sky Aviation Policy Manual",
    },
    {
        "externalId": POLICY_FERRY,
        "title": "Ferry Flight Procedure for Maintenance",
        "description": "Ferry flights to maintenance facility are permitted only when: (1) aircraft has a valid annual inspection, (2) oil is overdue by no more than 5 hours, (3) no grounding squawks exist, (4) PIC holds at least private certificate with appropriate ratings, (5) flight is direct to maintenance facility with no intermediate stops.",
        "rule": "ferry_requires_valid_annual=true; ferry_max_oil_overdue_hours=5; ferry_requires_no_grounding_squawks=true",
        "category": "operations",
        "references": "Desert Sky Aviation Operations Manual Rev 4.2",
    },
]

FLEET_OWNER: dict[str, Any] = {
    "externalId": FLEET_OWNER_ID,
    "name": "Desert Sky Aviation",
    "description": "Flight school and aircraft rental operation based at KPHX. Operates a fleet of four 1978 Cessna 172N Skyhawks.",
    "location": "KPHX — Phoenix Sky Harbor International Airport",
    "contact": "ops@desertsky.aero",
}

# ---------------------------------------------------------------------------
# Normal engine parameter ranges
# ---------------------------------------------------------------------------

NORMAL_PARAMS = {
    "oil_pressure_min": (55, 75),
    "oil_pressure_max": (65, 85),
    "oil_temp_max": (175, 215),
    "cht_max": (340, 400),
    "egt_max": (1250, 1420),
    "fuel_used_gal": (4.5, 8.5),
}

# Caution thresholds (for N8834Q and N1156P symptom flights)
CAUTION_PARAMS = {
    "cht_max": (420, 455),
    "oil_temp_max": (215, 235),
}

# Critical params (N1156P pre-failure flights)
CRITICAL_PARAMS = {
    "cht_max": (455, 490),
    "oil_temp_max": (225, 245),
    "oil_pressure_min": (35, 55),
    "oil_pressure_max": (50, 65),
}


# ---------------------------------------------------------------------------
# Flight generation
# ---------------------------------------------------------------------------

def _gen_flight_params(rng: np.random.Generator, route: str, is_caution: bool = False, is_critical: bool = False) -> dict[str, Any]:
    """Generate realistic engine parameters for a single flight."""
    dur_lo, dur_hi = ROUTE_DURATIONS[route]
    # Weighted toward shorter durations using beta distribution
    raw_dur = rng.beta(1.5, 3.0) * (dur_hi - dur_lo) + dur_lo
    duration = float(np.clip(raw_dur, 0.5, 4.0))

    fuel_lo, fuel_hi = NORMAL_PARAMS["fuel_used_gal"]
    fuel = float(rng.uniform(fuel_lo, fuel_hi) * duration)

    def _param(key: str) -> float:
        if is_critical and key in CRITICAL_PARAMS:
            lo, hi = CRITICAL_PARAMS[key]
        elif is_caution and key in CAUTION_PARAMS:
            lo, hi = CAUTION_PARAMS[key]
        else:
            lo, hi = NORMAL_PARAMS[key]
        return float(rng.uniform(lo, hi))

    return {
        "duration": round(duration, 2),
        "oil_pressure_min": round(_param("oil_pressure_min"), 1),
        "oil_pressure_max": round(_param("oil_pressure_max"), 1),
        "oil_temp_max": round(_param("oil_temp_max"), 1),
        "cht_max": round(_param("cht_max"), 1),
        "egt_max": round(_param("egt_max"), 1),
        "fuel_used_gal": round(fuel, 2),
        "cycles": 1,
    }


def generate_flights(tail: str) -> list[dict[str, Any]]:
    """Generate deterministic flight records for one aircraft."""
    rng = np.random.default_rng(seed=42)
    # Advance rng state to be tail-specific so each aircraft has independent sequences
    tail_seeds = {"N4798E": 42, "N2251K": 137, "N8834Q": 251, "N1156P": 389}
    rng = np.random.default_rng(seed=tail_seeds[tail])

    count = FLIGHT_COUNTS[tail]
    months = HISTORY_MONTHS[tail]
    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    history_start = now - timedelta(days=months * 30)

    routes = rng.choice(ROUTE_CHOICES, size=count, p=ROUTE_WEIGHTS)
    # Random flight times spread across the history period
    time_offsets = sorted(rng.uniform(0, 1, size=count))

    hobbs = HOBBS_START[tail]
    flights: list[dict[str, Any]] = []

    # Determine which flights are "symptomatic" for N8834Q and N1156P
    n8834q_exhibit = set(range(*N8834Q_EXHIBITED_FLIGHT_RANGE))
    n1156p_exhibit = set(range(*N1156P_EXHIBITED_FLIGHT_RANGE))

    for i in range(count):
        is_caution = (tail == "N8834Q" and i in n8834q_exhibit) or \
                     (tail == "N1156P" and i in n1156p_exhibit)
        is_critical = tail == "N1156P" and i >= N1156P_EXHIBITED_FLIGHT_RANGE[1]
        # N1156P flight -1 (index 99) is the failure flight — omit it from pilot flights
        if tail == "N1156P" and i == count - 1:
            continue

        route = str(routes[i])
        params = _gen_flight_params(rng, route, is_caution=is_caution, is_critical=is_critical)

        elapsed = time_offsets[i] * (months * 30)
        flight_dt = history_start + timedelta(days=elapsed)

        # For N1156P, stop flights 6 months before present (after the accident)
        if tail == "N1156P":
            accident_date = datetime(2025, 10, 3, tzinfo=timezone.utc)
            if flight_dt > accident_date:
                continue

        tach_start = round(hobbs * 0.92, 1)

        pilot_notes = _gen_pilot_notes(rng, route, params, tail, i, is_caution, is_critical)

        flights.append({
            "timestamp": flight_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "hobbs_start": round(hobbs, 1),
            "hobbs_end": round(hobbs + params["duration"], 1),
            "tach_start": tach_start,
            "tach_end": round(tach_start + params["duration"] * 0.92, 1),
            "route": route,
            "duration": params["duration"],
            "oil_pressure_min": params["oil_pressure_min"],
            "oil_pressure_max": params["oil_pressure_max"],
            "oil_temp_max": params["oil_temp_max"],
            "cht_max": params["cht_max"],
            "egt_max": params["egt_max"],
            "fuel_used_gal": params["fuel_used_gal"],
            "cycles": 1,
            "pilot_notes": pilot_notes,
            "tail": tail,
            "flight_index": i,
        })
        hobbs = round(hobbs + params["duration"], 1)

    # Apply N4798E story overrides
    if tail == "N4798E":
        flights = _apply_n4798e_overrides(flights, rng)

    return flights


def _gen_pilot_notes(
    rng: np.random.Generator,
    route: str,
    params: dict[str, Any],
    tail: str,
    idx: int,
    is_caution: bool,
    is_critical: bool,
) -> str:
    """Generate realistic pilot notes for a flight."""
    notes = ""

    if is_caution and tail == "N8834Q":
        if params["cht_max"] > 430:
            notes = f"CHT #3 running high at {params['cht_max']:.0f}°F, other cylinders normal. Enriched mixture. Will schedule A&P inspection."
        else:
            notes = "Mag check showed slight roughness on left mag. Cleared after extended runup. CHT slightly elevated on #3."
    elif is_critical and tail == "N1156P":
        if idx >= 97:
            notes = f"Rough running throughout flight, couldn't maintain altitude at cruise power. CHT very high {params['cht_max']:.0f}°F, oil pressure low. Declared precautionary and returned to KPHX."
        elif idx >= 95:
            notes = f"Engine running rough intermittently. CHT #2 spiked to {params['cht_max']:.0f}°F. Reduced power and enriched mixture. Oil seems to be burning faster than normal."
        else:
            notes = f"High CHT again {params['cht_max']:.0f}°F. Leaned aggressively to bring it down. Engine feels down on power."
    elif tail == "N1156P" and not is_critical and not is_caution:
        # Some early pre-symptom notes
        if rng.random() < 0.15:
            note_options = [
                "Good flight, all normal.",
                "Slight hesitation on startup, cleared after warmup.",
                "Oil temp a little warm today, ambient hot.",
                f"CHT running {params['cht_max']:.0f}°F at cruise, leaned as per POH.",
            ]
            notes = str(rng.choice(note_options))
    elif rng.random() < 0.12:
        # Occasional normal notes for other aircraft
        note_options = [
            "Smooth flight, all instruments normal.",
            "Slight turbulence below 3000 ft, otherwise uneventful.",
            "Pattern work, 6 T&Gs.",
            "Crosswind practice.",
            "Good flight.",
        ]
        notes = str(rng.choice(note_options))

    # N1156P pre-failure symptom flights (exhibit range)
    if tail == "N1156P" and idx in range(*N1156P_EXHIBITED_FLIGHT_RANGE):
        symptom_notes = [
            f"CHT elevated at {params['cht_max']:.0f}°F during cruise. Enriched mixture helped slightly.",
            "Engine feels slightly rough at cruise power. Oil consumption seems higher than normal.",
            f"Rough running on climbout, smoothed out in cruise. CHT {params['cht_max']:.0f}°F.",
            "Added quart of oil before flight — burning more than usual.",
            "Intermittent roughness when leaned. Kept mixture rich.",
        ]
        notes = str(rng.choice(symptom_notes))

    return notes


def _apply_n4798e_overrides(flights: list[dict[str, Any]], rng: np.random.Generator) -> list[dict[str, Any]]:
    """Apply N4798E story overrides: Flagstaff trips in summer months."""
    for f in flights:
        dt = datetime.strptime(f["timestamp"], "%Y-%m-%d %H:%M:%S")
        if dt.month in (6, 7, 8) and rng.random() < 0.3:
            f["route"] = "KPHX-KFLG"
            new_dur = float(np.clip(rng.uniform(1.2, 2.0), 0.5, 4.0))
            f["duration"] = round(new_dur, 2)
            f["hobbs_end"] = round(f["hobbs_start"] + new_dur, 1)
    return flights


# ---------------------------------------------------------------------------
# Maintenance records
# ---------------------------------------------------------------------------

# Component IDs used in maintenance records (shared prefix per tail)
def _comp(tail: str, component: str) -> str:
    return f"{tail}-{component}"


MAINTENANCE_RECORDS: dict[str, list[dict[str, Any]]] = {
    "N4798E": [
        {
            "date": "2025-06-10",
            "component_id": "N4798E",
            "maintenance_type": "annual",
            "description": "Annual inspection completed. All systems checked per FAR 43 Appendix D. Minor discrepancies noted and corrected. Aircraft returned to service.",
            "hobbs_at_service": 4742.0,
            "tach_at_service": 4362.0,
            "next_due_hobbs": "",
            # Calendar due 12 months after sign-off — keep ~2 months ahead of “demo now” (spring 2026)
            "next_due_date": "2026-06-10",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "Mike Torres, IA #3847291",
            "ad_reference": "AD 80-04-03 R2; AD 2001-23-03; AD 2011-10-09; AD 90-06-03 R1",
            "sb_reference": "SB 480F",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Spark plugs rotated and gapped; oil and filter changed",
            "labor_hours": 8.5,
            "signoff_type": "inspection_approval",
        },
        {
            "date": "2025-07-15",
            "component_id": "N4798E-ENGINE",
            "maintenance_type": "oil_change",
            "description": "Oil and filter change. 50-hour interval. Drained 7 qts 15W-50. Cut filter — no metal found. Oil analysis sent to Blackstone Labs.",
            "hobbs_at_service": 4780.2,
            "tach_at_service": 4398.6,
            "next_due_hobbs": 4830.2,
            "next_due_date": "2025-11-15",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Lycoming LW-16702 oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
        {
            "date": "2025-11-20",
            "component_id": "N4798E-ENGINE",
            "maintenance_type": "oil_change",
            "description": "Oil and filter change at 50-hour interval. Cut filter — trace ferrous particles, within normal limits for this engine variant. Blackstone results from last change were normal.",
            "hobbs_at_service": 4836.5,
            "tach_at_service": 4449.4,
            "next_due_hobbs": 4886.5,
            "next_due_date": "2026-03-20",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Lycoming LW-16702 oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
    ],
    "N2251K": [
        {
            "date": "2025-06-05",
            "component_id": "N2251K",
            "maintenance_type": "annual",
            "description": "Annual inspection completed. Seat tracks inspected per AD 2011-10-09. Exhaust inspected per AD 90-06-03 R1. Aircraft returned to service.",
            "hobbs_at_service": 5092.1,
            "tach_at_service": 4685.1,
            "next_due_hobbs": "",
            "next_due_date": "2026-06-05",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "James Wheeler, IA #2918473",
            "ad_reference": "AD 80-04-03 R2; AD 2011-10-09; AD 90-06-03 R1",
            "sb_reference": "SB 480F",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Exhaust gaskets; spark plugs inspected and rotated",
            "labor_hours": 7.5,
            "signoff_type": "inspection_approval",
        },
        {
            "date": "2025-07-01",
            "component_id": "N2251K-ENGINE",
            "maintenance_type": "oil_change",
            "description": "50-hour oil and filter change. No anomalies. Cut filter clean.",
            "hobbs_at_service": 5098.4,
            "tach_at_service": 4690.5,
            "next_due_hobbs": 5148.4,
            "next_due_date": "2025-11-01",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
        {
            "date": "2025-11-10",
            "component_id": "N2251K-ENGINE",
            "maintenance_type": "oil_change",
            "description": "50-hour oil and filter change. Cut filter — no metal. Last oil analysis normal.",
            "hobbs_at_service": 5154.2,
            "tach_at_service": 4741.9,
            "next_due_hobbs": 5204.2,
            "next_due_date": "2026-03-10",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
    ],
    "N8834Q": [
        {
            "date": "2025-09-12",
            "component_id": "N8834Q",
            "maintenance_type": "annual",
            "description": "Annual inspection completed. All systems airworthy. Magnetos timed and tested. No discrepancies. Aircraft returned to service.",
            "hobbs_at_service": 4803.8,
            "tach_at_service": 4419.5,
            "next_due_hobbs": "",
            "next_due_date": "2026-09-12",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "Mike Torres, IA #3847291",
            "ad_reference": "AD 80-04-03 R2; AD 2011-10-09; AD 90-06-03 R1",
            "sb_reference": "SB 480F",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Spark plugs replaced; magneto points inspected",
            "labor_hours": 9.0,
            "signoff_type": "inspection_approval",
        },
        {
            "date": "2025-09-20",
            "component_id": "N8834Q-ENGINE",
            "maintenance_type": "oil_change",
            "description": "50-hour oil change concurrent with annual. No anomalies.",
            "hobbs_at_service": 4803.8,
            "tach_at_service": 4419.5,
            "next_due_hobbs": 4853.8,
            "next_due_date": "2026-01-20",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
        {
            "date": "2026-01-28",
            "component_id": "N8834Q-ENGINE",
            "maintenance_type": "oil_change",
            "description": "50-hour oil and filter change. Cut filter — very fine metallic particles, borderline. Noted for follow-up. Sent to Blackstone.",
            "hobbs_at_service": 4857.1,
            "tach_at_service": 4468.5,
            "next_due_hobbs": 4907.1,
            "next_due_date": "2026-05-28",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
    ],
    "N1156P": [
        {
            "date": "2025-04-15",
            "component_id": "N1156P",
            "maintenance_type": "annual",
            "description": "Annual inspection completed. Minor squawks noted. ELT battery replaced. Aircraft returned to service.",
            "hobbs_at_service": 5282.3,
            "tach_at_service": 4859.7,
            "next_due_hobbs": "",
            "next_due_date": "2026-04-15",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "James Wheeler, IA #2918473",
            "ad_reference": "AD 80-04-03 R2; AD 2011-10-09; AD 90-06-03 R1",
            "sb_reference": "SB 480F",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "ELT battery; spark plugs rotated; oil and filter changed",
            "labor_hours": 10.0,
            "signoff_type": "inspection_approval",
        },
        {
            "date": "2025-04-20",
            "component_id": "N1156P-ENGINE",
            "maintenance_type": "oil_change",
            "description": "Oil change concurrent with annual. Cut filter clean. No anomalies.",
            "hobbs_at_service": 5282.3,
            "tach_at_service": 4859.7,
            "next_due_hobbs": 5332.3,
            "next_due_date": "2025-08-20",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.0,
            "signoff_type": "return_to_service",
        },
        {
            "date": "2025-09-05",
            "component_id": "N1156P-ENGINE",
            "maintenance_type": "oil_change",
            "description": "50-hour oil change. OVERDUE — 53.2 hours since last change. Pilot reported oil consumption has increased. Cut filter shows elevated fine metallic particles. RECOMMEND BORESCOPE AND OIL ANALYSIS BEFORE NEXT FLIGHT. Blackstone sample sent.",
            "hobbs_at_service": 5385.5,
            "tach_at_service": 4954.7,
            "next_due_hobbs": 5435.5,
            "next_due_date": "2026-01-05",
            "mechanic": "Desert Aero Maintenance — James Wheeler, A&P/IA #2918473",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "SB 388C",
            "squawk_id": "",
            "resolved_by": "",
            "parts_replaced": "Oil filter; 7 qts Aeroshell 15W-50",
            "labor_hours": 1.5,
            "signoff_type": "return_to_service",
        },
        {
            "date": "2025-11-15",
            "component_id": "N1156P-ENGINE",
            "maintenance_type": "post_accident_inspection",
            "description": "Post-accident teardown inspection following catastrophic engine failure on 2025-10-03. Cylinder #2 connecting rod failed and exited through case. Evidence of chronic lean detonation found: piston crown erosion, cylinder head erosion consistent with detonation, valve face pitting. Engine is NOT REPAIRABLE. Replacement required.",
            "hobbs_at_service": 5435.5,
            "tach_at_service": 4995.5,
            "next_due_hobbs": "",
            "next_due_date": "",
            "mechanic": "AZ Aviation Overhaul — Linda Chen, A&P/IA #3561028",
            "inspector": "Linda Chen, IA #3561028",
            "ad_reference": "AD 2011-10-09",
            "sb_reference": "",
            "squawk_id": "SQ-N1156P-001",
            "resolved_by": "",
            "parts_replaced": "",
            "labor_hours": 24.0,
            "signoff_type": "conformity_statement",
        },
    ],
}

SQUAWK_RECORDS: dict[str, list[dict[str, Any]]] = {
    "N4798E": [
        {
            "date": "2026-01-15",
            "component_id": "N4798E-ENGINE",
            "maintenance_type": "squawk",
            "description": "Minor oil seep at rocker cover gasket — right rear cylinder. Not affecting oil level measurably. Deferred to next scheduled maintenance.",
            "hobbs_at_service": 4858.2,
            "tach_at_service": 4469.5,
            "next_due_hobbs": "",
            "next_due_date": "",
            "mechanic": "Cactus Aviation Services — Mike Torres, A&P/IA #3847291",
            "inspector": "",
            "ad_reference": "",
            "sb_reference": "",
            "squawk_id": "SQ-N4798E-001",
            "resolved_by": "",
            "parts_replaced": "",
            "labor_hours": 0.25,
            "signoff_type": "",
            "severity": "non-grounding",
            "status": "open",
        },
    ],
    "N2251K": [],
    "N8834Q": [],
    "N1156P": [
        {
            "date": "2025-10-03",
            "component_id": "N1156P-ENGINE",
            "maintenance_type": "squawk",
            "description": "CATASTROPHIC ENGINE FAILURE. Connecting rod failure, rod exited through case. Aircraft made emergency off-airport landing. Aircraft NOT AIRWORTHY. Engine requires replacement.",
            "hobbs_at_service": 5435.5,
            "tach_at_service": 4995.5,
            "next_due_hobbs": "",
            "next_due_date": "",
            "mechanic": "AZ Aviation Overhaul — Linda Chen, A&P/IA #3561028",
            "inspector": "Linda Chen, IA #3561028",
            "ad_reference": "AD 2011-10-09",
            "sb_reference": "",
            "squawk_id": "SQ-N1156P-001",
            "resolved_by": "",
            "parts_replaced": "",
            "labor_hours": 2.0,
            "signoff_type": "",
            "severity": "grounding",
            "status": "open",
        },
    ],
}


def get_all_maintenance(tail: str) -> list[dict[str, Any]]:
    """Return maintenance records + squawks for a given tail."""
    records = list(MAINTENANCE_RECORDS.get(tail, []))
    squawks = list(SQUAWK_RECORDS.get(tail, []))
    all_records = records + squawks
    # Normalize component IDs to use the tail prefix consistently
    for r in all_records:
        comp = r.get("component_id", "")
        # Replace bare component names with tail-prefixed versions
        replacements = {
            "AIRCRAFT": tail,
            "ENGINE-1": f"{tail}-ENGINE",
            "PROP-1": f"{tail}-PROPELLER",
            "AIRFRAME-1": f"{tail}-AIRFRAME",
            "AVIONICS-1": f"{tail}-AVIONICS",
        }
        for old, new in replacements.items():
            if comp == old:
                r["component_id"] = new
                break
    return all_records
