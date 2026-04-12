"""
Fleet Dataset — Desert Sky Aviation, KPHX.

Single source of truth for all four aircraft in the fleet:
  N4798E  — AIRWORTHY   (380 SMOH tach, 1 open non-grounding squawk, oil due in ~18 tach hr; calendar leg current)
  N2251K  — FERRY ONLY  (290 SMOH tach, oil ~1.2 tach hr overdue on tach leg, calendar leg current; ferry authorized per policy)
  N8834Q  — CAUTION     (198 SMOH, elevated CHT #3 + rough mag check, oil due in ~11 tach hr)
  N1156P  — NOT AIRWORTHY (catastrophic engine failure at ~520 SMOH, ~30 days before demo anchor)

Calendar fields and ISO timestamps are offsets from get_demo_anchor() (UTC). Tach/Hobbs story
numbers stay fixed. Optional env DESERT_SKY_DEMO_DATE=YYYY-MM-DD makes transforms reproducible.

Uses numpy.random.default_rng(seed=42) for fully deterministic output.
Story-beat overrides are applied on top of the random baseline.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Fleet constants
# ---------------------------------------------------------------------------

TAILS: tuple[str, ...] = ("N4798E", "N2251K", "N8834Q", "N1156P")

# Generation parameters per tail (N1156P: stored pilot-log rows; see generate_flights)
FLIGHT_COUNTS = {"N4798E": 70, "N2251K": 53, "N8834Q": 36, "N1156P": 78}

# Months of history per tail
HISTORY_MONTHS = {"N4798E": 14, "N2251K": 10, "N8834Q": 7, "N1156P": 20}

# Non-N1156P: last N flights fall in the final R days before the demo anchor (active-rental look).
RECENT_FLIGHT_COUNT = 6
RECENT_FLIGHT_DAYS_BEFORE_ANCHOR = 7

# Starting Hobbs per tail (first chronological flight hobbs_start for non–N1156P tails)
HOBBS_START = {"N4798E": 4730.0, "N2251K": 5090.0, "N8834Q": 4802.0, "N1156P": 5268.0}

# N1156P: last pilot-log hobbs before failure (aligned with squawk / maintenance teardown)
N1156P_LAST_HOBBS = 5435.5
# First hobbs in CSV (oldest flight); chosen so annual at ~5282 falls inside the log
N1156P_FIRST_HOBBS = 5268.0

# Engine overhaul Hobbs baseline per tail (Hobbs at last major overhaul = ENGINE_TACH_AT_OVERHAUL / 0.92)
OVERHAUL_HOBBS = {
    "N4798E": 4413.4,   # 4060.3 / 0.92
    "N2251K": 4842.3,   # 4464.9 / 0.92
    "N8834Q": 4638.3,   # 4267.2 / 0.92
    "N1156P": N1156P_LAST_HOBBS - 520.0,
}

# Route weights: local, KSDL, KFLG, KPRC
ROUTE_CHOICES = ["KPHX-local", "KPHX-KSDL", "KPHX-KFLG", "KPHX-KPRC"]
ROUTE_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

# Route typical durations (hours) — local includes pattern work, maneuvers, and instrument training
ROUTE_DURATIONS = {
    "KPHX-local": (1.0, 3.0),
    "KPHX-KSDL": (0.5, 1.2),
    "KPHX-KFLG": (1.5, 3.0),
    "KPHX-KPRC": (1.2, 2.5),
}

ENGINE_TBO = 2000

# Tach reading at last engine overhaul — SMOH = current_tach − this (maintenance clock is tach).
ENGINE_TACH_AT_OVERHAUL: dict[str, float] = {
    "N4798E": 4060.3,   # SMOH = 4440.3 - 4060.3 = 380
    "N2251K": 4464.9,   # SMOH = 4754.9 - 4464.9 = 290
    "N8834Q": 4267.2,   # SMOH = 4465.2 - 4267.2 = 198
    "N1156P": 4475.5,   # SMOH = 4995.5 - 4475.5 = 520
}

# End-of-flight-log tach (demo as-of). Matches natural generate_flights() chain so the last
# flight is not artificially lengthened; _snap_last_flight_to_target_tach only applies tiny
# rounding corrections (≤0.05 hr duration delta).
CURRENT_TACH_SNAPSHOT: dict[str, float] = {
    "N4798E": 4440.3,   # 18 tach hr before next oil (4458.3)
    "N2251K": 4754.9,   # 1.2 tach hr past due (next_due 4753.7)
    "N8834Q": 4465.2,   # ~11 tach hr before next oil (4476.2)
    "N1156P": 4995.5,   # last log tach before/through failure timeline
}

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

# N1156P: flight_index (chronological 0..77) for EXHIBITED symptom links — last 9 flights before failure
N1156P_STORED_FLIGHT_COUNT = 78
N1156P_EXHIBITED_FLIGHT_RANGE = (69, 78)
# Last 4 stored flights: elevated multi-metric telemetry (Flights page yellows)
N1156P_SEVERE_FLIGHT_RANGE = (74, 78)

# N8834Q: last 3 flights exhibit symptoms (0-based flight_index)
N8834Q_EXHIBITED_FLIGHT_RANGE = (33, 36)

# ---------------------------------------------------------------------------
# Demo anchor (UTC midnight). All calendar dates in this module derive from it.
# ---------------------------------------------------------------------------

N1156P_FAILURE_DAYS_BEFORE_ANCHOR = 30


def get_demo_anchor() -> datetime:
    """
    Demo as-of instant: UTC midnight for the resolved calendar day.

    Set DESERT_SKY_DEMO_DATE=YYYY-MM-DD for reproducible CSVs and CI; otherwise today (UTC).
    """
    raw = os.environ.get("DESERT_SKY_DEMO_DATE", "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d = datetime.now(timezone.utc).date()
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _d(anchor: datetime, day_offset: int) -> str:
    """Calendar date YYYY-MM-DD for anchor + day_offset (anchor is UTC midnight)."""
    return (anchor + timedelta(days=day_offset)).strftime("%Y-%m-%d")


def n1156p_accident_datetime(anchor: datetime | None = None) -> datetime:
    """Catastrophic engine failure instant (UTC date boundary) for story ordering."""
    a = anchor or get_demo_anchor()
    return a - timedelta(days=N1156P_FAILURE_DAYS_BEFORE_ANCHOR)


def format_n1156p_accident_iso(anchor: datetime | None = None) -> str:
    """ISO date of N1156P failure (for maintenance narrative text)."""
    return n1156p_accident_datetime(anchor).strftime("%Y-%m-%d")


def format_n1156p_accident_month_year(anchor: datetime | None = None) -> str:
    """Human label e.g. 'March 2026' for asset descriptions and prompts."""
    return n1156p_accident_datetime(anchor).strftime("%B %Y")


# ---------------------------------------------------------------------------
# Symptom / policy data
# ---------------------------------------------------------------------------


def get_symptom_nodes(anchor: datetime | None = None) -> list[dict[str, Any]]:
    """
    Observation/symptom metadata for fleet graph ingest.

    N8834Q first_observed dates sit in a short window before the anchor.
    N1156P symptoms are compressed into the weeks before the accident date.
    """
    a = anchor or get_demo_anchor()
    accident = n1156p_accident_datetime(a)

    def iso(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    q_cht = iso(a - timedelta(days=17))
    q_mag = iso(a - timedelta(days=10))

    return [
        {
            "externalId": SYM_N8834Q_CHT,
            "aircraft_id": "N8834Q",
            "title": "Elevated CHT #3",
            "description": "Cylinder head temperature on #3 cylinder running 40-60°F above the other cylinders during cruise. First observed three flights ago and trending upward.",
            "observation": "CHT #3 consistently reaching 430-450°F while CHT #1, #2, #4 remain 360-380°F at same power setting.",
            "severity": "caution",
            "first_observed": q_cht,
        },
        {
            "externalId": SYM_N8834Q_MAG,
            "aircraft_id": "N8834Q",
            "title": "Rough Running on Left Mag",
            "description": "Engine runs noticeably rough during mag check on left magneto. Right mag check is smooth. RPM drop on left mag is 150 RPM versus 50 RPM on right.",
            "observation": "Mag check at runup: right mag 50 RPM drop (normal), left mag 150 RPM drop with roughness. Cleared after extended runup.",
            "severity": "caution",
            "first_observed": q_mag,
        },
        {
            "externalId": SYM_N1156P_CHT,
            "aircraft_id": "N1156P",
            "title": "Persistently Elevated CHT",
            "description": "CHT readings trending upward over the previous nine flights before failure. Reached 460-480°F on final flights.",
            "observation": "Pilot notes documented persistent high CHT during cruise, particularly on longer flights. Engine leaned aggressively to manage temperatures.",
            "severity": "warning",
            "first_observed": iso(accident - timedelta(days=21)),
        },
        {
            "externalId": SYM_N1156P_OIL,
            "aircraft_id": "N1156P",
            "title": "Increased Oil Consumption",
            "description": "Oil consumption increased in the weeks preceding failure. Required adding 1 qt oil every 8-10 flight hours versus normal 15-20 hours.",
            "observation": "Pre-flight checks documented oil level dropping faster than historical baseline. No visible external leaks found.",
            "severity": "warning",
            "first_observed": iso(accident - timedelta(days=24)),
        },
        {
            "externalId": SYM_N1156P_ROUGH,
            "aircraft_id": "N1156P",
            "title": "Intermittent Rough Running",
            "description": "Rough engine operation noted intermittently during cruise flight, particularly at lean mixture settings. Cleared when mixture enriched.",
            "observation": "Pilot notes on multiple flights mention roughness that cleared with mixture enrichment. Attributed to improper leaning technique at the time.",
            "severity": "warning",
            "first_observed": iso(accident - timedelta(days=14)),
        },
        {
            "externalId": SYM_N1156P_POWER,
            "aircraft_id": "N1156P",
            "title": "Reduced Power Output",
            "description": "Climb performance noticeably reduced on final flights before failure. Unable to maintain normal cruise RPM at full throttle.",
            "observation": "Pilot reported inability to reach normal cruise RPM. Takeoff roll longer than normal. Climb rate approximately 400 FPM versus normal 770 FPM.",
            "severity": "critical",
            "first_observed": iso(accident - timedelta(days=7)),
        },
    ]
OPERATIONAL_POLICIES: list[dict[str, Any]] = [
    {
        "externalId": POLICY_OIL_CHANGE,
        "title": "Oil Change Interval",
        "description": "All Desert Sky Aviation aircraft require oil and filter change every 50 tach (engine) hours or 4 calendar months, whichever comes first. References Lycoming SB 388C.",
        "rule": "oil_change_tach_interval=50; oil_change_calendar_months=4",
        "category": "engine_maintenance",
        "references": "Lycoming SB 388C; FAA AC 43.13-1B",
    },
    {
        "externalId": POLICY_OIL_GRACE,
        "title": "Oil Change Grace Period — Ferry Flight Authorization",
        "description": "Aircraft may conduct a single direct ferry to the maintenance facility when oil is overdue on tach by >0.0–5.0 hours, or on the calendar leg by 1–13 days (per 50 tach hr / 4 month policy). PIC must document the ferry in the journey log. Oil overdue by more than 5 tach hours, or 14+ calendar days past the due date, is NOT AIRWORTHY.",
        "rule": "ferry_authorized_if_oil_overdue_tach_hours_between=0.1,5; ferry_authorized_if_oil_overdue_calendar_days_between=1,13; ferry_not_authorized_if_oil_overdue_tach_hours_gt=5; ferry_not_authorized_if_oil_overdue_calendar_days_gte=14",
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
        "description": "Ferry flights to maintenance facility are permitted only when: (1) aircraft has a valid annual inspection, (2) oil is overdue by no more than 5 tach hours and no more than 13 calendar days (4-month leg), (3) no grounding squawks exist, (4) PIC holds at least private certificate with appropriate ratings, (5) flight is direct to maintenance facility with no intermediate stops.",
        "rule": "ferry_requires_valid_annual=true; ferry_max_oil_overdue_tach_hours=5; ferry_max_oil_overdue_calendar_days=13; ferry_requires_no_grounding_squawks=true",
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

# Critical params (legacy / non–N1156P-severe)
CRITICAL_PARAMS = {
    "cht_max": (455, 490),
    "oil_temp_max": (225, 245),
    "oil_pressure_min": (35, 55),
    "oil_pressure_max": (50, 65),
}

# N1156P final stored flights — bands aligned with client/src/lib/flightThresholds.ts
N1156P_SEVERE_PARAMS = {
    "cht_max": (455, 490),
    "oil_temp_max": (225, 245),
    "oil_pressure_min": (38, 54),
    "oil_pressure_max": (86, 96),
    "egt_max": (1360, 1480),
}


# ---------------------------------------------------------------------------
# Flight generation
# ---------------------------------------------------------------------------

def _gen_flight_params(
    rng: np.random.Generator,
    route: str,
    is_caution: bool = False,
    is_critical: bool = False,
    *,
    is_n1156p_caution_exhibit: bool = False,
    is_n1156p_severe: bool = False,
) -> dict[str, Any]:
    """Generate realistic engine parameters for a single flight."""
    dur_lo, dur_hi = ROUTE_DURATIONS[route]
    # Weighted toward shorter durations using beta distribution
    raw_dur = rng.beta(1.5, 3.0) * (dur_hi - dur_lo) + dur_lo
    duration = float(np.clip(raw_dur, 0.5, 4.0))

    fuel_lo, fuel_hi = NORMAL_PARAMS["fuel_used_gal"]
    fuel = float(rng.uniform(fuel_lo, fuel_hi) * duration)

    def _param(key: str) -> float:
        if is_n1156p_severe and key in N1156P_SEVERE_PARAMS:
            lo, hi = N1156P_SEVERE_PARAMS[key]
        elif is_n1156p_caution_exhibit and key in CAUTION_PARAMS:
            lo, hi = CAUTION_PARAMS[key]
        elif is_critical and key in CRITICAL_PARAMS:
            lo, hi = CRITICAL_PARAMS[key]
        elif is_caution and key in CAUTION_PARAMS:
            lo, hi = CAUTION_PARAMS[key]
        else:
            lo, hi = NORMAL_PARAMS[key]
        return float(rng.uniform(lo, hi))

    if is_n1156p_severe:
        oil_psi_lo = round(float(rng.uniform(*N1156P_SEVERE_PARAMS["oil_pressure_min"])), 1)
        oil_psi_hi = round(float(rng.uniform(*N1156P_SEVERE_PARAMS["oil_pressure_max"])), 1)
    else:
        oil_psi_lo = round(_param("oil_pressure_min"), 1)
        oil_psi_hi = round(_param("oil_pressure_max"), 1)
    if oil_psi_lo > oil_psi_hi:
        oil_psi_lo, oil_psi_hi = oil_psi_hi, oil_psi_lo
    if oil_psi_lo == oil_psi_hi:
        oil_psi_hi = round(min(oil_psi_hi + 0.1, 120.0), 1)

    return {
        "duration": round(duration, 2),
        "oil_pressure_min": oil_psi_lo,
        "oil_pressure_max": oil_psi_hi,
        "oil_temp_max": round(_param("oil_temp_max"), 1),
        "cht_max": round(_param("cht_max"), 1),
        "egt_max": round(_param("egt_max"), 1),
        "fuel_used_gal": round(fuel, 2),
        "cycles": 1,
    }


def _generate_flights_n1156p() -> list[dict[str, Any]]:
    """Exactly N1156P_STORED_FLIGHT_COUNT flights, chronological flight_index 0..77, hobbs to N1156P_LAST_HOBBS."""
    rng = np.random.default_rng(seed=389)
    tail = "N1156P"
    count = N1156P_STORED_FLIGHT_COUNT
    months = HISTORY_MONTHS[tail]
    now = get_demo_anchor()
    history_start = now - timedelta(days=months * 30)
    accident_date = n1156p_accident_datetime(now)
    span_days = max(1.0, (accident_date - history_start).total_seconds() / 86400.0)

    ex_lo, ex_hi = N1156P_EXHIBITED_FLIGHT_RANGE
    sev_lo, sev_hi = N1156P_SEVERE_FLIGHT_RANGE
    routes = rng.choice(ROUTE_CHOICES, size=count, p=ROUTE_WEIGHTS)
    time_offsets = sorted(rng.uniform(0.0, 1.0, size=count))

    scheduled: list[tuple[datetime, str]] = []
    for i in range(count):
        elapsed_days = time_offsets[i] * span_days
        flight_dt = history_start + timedelta(days=elapsed_days)
        if flight_dt > accident_date:
            flight_dt = accident_date - timedelta(minutes=30)
        scheduled.append((flight_dt, str(routes[i])))
    scheduled.sort(key=lambda x: x[0])

    draft: list[dict[str, Any]] = []
    for idx, (flight_dt, route) in enumerate(scheduled):
        in_exhibit = ex_lo <= idx < ex_hi
        in_severe = sev_lo <= idx < sev_hi
        is_n1156p_caution_exhibit = in_exhibit and not in_severe
        params = _gen_flight_params(
            rng,
            route,
            is_caution=False,
            is_critical=False,
            is_n1156p_caution_exhibit=is_n1156p_caution_exhibit,
            is_n1156p_severe=in_severe,
        )
        pilot_notes = _gen_pilot_notes(
            rng,
            route,
            params,
            tail,
            idx,
            is_n1156p_exhibit=in_exhibit,
            is_n1156p_severe=in_severe,
        )
        draft.append({
            "timestamp": flight_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "route": route,
            "params": params,
            "pilot_notes": pilot_notes,
            "flight_index": idx,
        })

    target_span = float(N1156P_LAST_HOBBS - N1156P_FIRST_HOBBS)
    raw_total = sum(float(row["params"]["duration"]) for row in draft)
    scale = target_span / raw_total if raw_total > 0 else 1.0
    hobbs = float(N1156P_FIRST_HOBBS)
    flights: list[dict[str, Any]] = []
    for idx, row in enumerate(draft):
        p = row["params"]
        dur = round(float(p["duration"]) * scale, 2)
        if idx == len(draft) - 1:
            dur = round(float(N1156P_LAST_HOBBS) - hobbs, 2)
        dur = max(0.5, dur)
        tach_start = round(hobbs * 0.92, 1)
        flights.append({
            "timestamp": row["timestamp"],
            "hobbs_start": round(hobbs, 1),
            "hobbs_end": round(hobbs + dur, 1),
            "tach_start": tach_start,
            "tach_end": round(tach_start + dur * 0.92, 1),
            "route": row["route"],
            "duration": dur,
            "oil_pressure_min": p["oil_pressure_min"],
            "oil_pressure_max": p["oil_pressure_max"],
            "oil_temp_max": p["oil_temp_max"],
            "cht_max": p["cht_max"],
            "egt_max": p["egt_max"],
            "fuel_used_gal": round(float(p["fuel_used_gal"]) * scale, 2),
            "cycles": 1,
            "pilot_notes": row["pilot_notes"],
            "tail": tail,
            "flight_index": row["flight_index"],
        })
        hobbs = round(hobbs + dur, 1)

    return flights


def generate_flights(tail: str) -> list[dict[str, Any]]:
    """Generate deterministic flight records for one aircraft."""
    if tail == "N1156P":
        return _generate_flights_n1156p()

    rng = np.random.default_rng(seed=42)
    tail_seeds = {"N4798E": 42, "N2251K": 137, "N8834Q": 251, "N1156P": 389}
    rng = np.random.default_rng(seed=tail_seeds[tail])

    count = FLIGHT_COUNTS[tail]
    months = HISTORY_MONTHS[tail]
    now = get_demo_anchor()
    history_start = now - timedelta(days=months * 30)

    routes = rng.choice(ROUTE_CHOICES, size=count, p=ROUTE_WEIGHTS)
    total_span_days = float(months * 30)
    day_offsets = _chronological_day_offsets_into_history(rng, count, total_span_days)

    hobbs = HOBBS_START[tail]
    flights: list[dict[str, Any]] = []

    n8834q_exhibit = set(range(*N8834Q_EXHIBITED_FLIGHT_RANGE))

    for i in range(count):
        is_caution = tail == "N8834Q" and i in n8834q_exhibit
        route = str(routes[i])
        params = _gen_flight_params(rng, route, is_caution=is_caution, is_critical=False)

        elapsed_days = float(day_offsets[i])
        flight_dt = history_start + timedelta(days=elapsed_days)

        tach_start = round(hobbs * 0.92, 1)

        pilot_notes = _gen_pilot_notes(
            rng, route, params, tail, i, is_n8834q_caution=is_caution
        )

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

    if tail == "N4798E":
        flights = _apply_n4798e_overrides(flights, rng)

    target = CURRENT_TACH_SNAPSHOT.get(tail)
    if target is not None:
        _snap_last_flight_to_target_tach(flights, target)

    return flights


def _snap_last_flight_to_target_tach(flights: list[dict[str, Any]], target_tach: float) -> None:
    """
    Optionally nudge the last flight so tach_end matches the fleet story snapshot.

    If the snapshot would require stretching the last flight (typical when target hobbs is
    far above the natural chain), we keep the natural duration so the final leg is not an
    outlier. Only sub-0.05 hr duration adjustments are applied (rounding shim).
    """
    if not flights:
        return
    last = flights[-1]
    hobbs_start = float(last["hobbs_start"])
    natural_dur = float(last["duration"])
    target_hobbs_end = target_tach / 0.92
    snap_dur = max(0.5, round(target_hobbs_end - hobbs_start, 2))
    if abs(snap_dur - natural_dur) > 0.05:
        return
    dur = snap_dur
    tach_start = round(hobbs_start * 0.92, 1)
    tach_end = round(tach_start + dur * 0.92, 1)
    hobbs_end = round(hobbs_start + dur, 1)
    last["duration"] = dur
    last["hobbs_end"] = hobbs_end
    last["tach_start"] = tach_start
    last["tach_end"] = tach_end


def _gen_pilot_notes(
    rng: np.random.Generator,
    route: str,
    params: dict[str, Any],
    tail: str,
    idx: int,
    *,
    is_n8834q_caution: bool = False,
    is_n1156p_exhibit: bool = False,
    is_n1156p_severe: bool = False,
) -> str:
    """Generate realistic pilot notes for a flight."""
    notes = ""
    sev_lo, sev_hi = N1156P_SEVERE_FLIGHT_RANGE

    if is_n8834q_caution and tail == "N8834Q":
        if params["cht_max"] > 430:
            notes = f"CHT #3 running high at {params['cht_max']:.0f}°F, other cylinders normal. Enriched mixture. Will schedule A&P inspection."
        else:
            notes = "Mag check showed slight roughness on left mag. Cleared after extended runup. CHT slightly elevated on #3."
    elif tail == "N1156P" and is_n1156p_severe:
        if idx >= sev_hi - 1:
            notes = (
                f"Rough running throughout flight, couldn't maintain altitude at cruise power. CHT very high "
                f"{params['cht_max']:.0f}°F, oil pressure low. Declared precautionary and returned to KPHX."
            )
        elif idx >= sev_hi - 3:
            notes = (
                f"Engine running rough intermittently. CHT spiked to {params['cht_max']:.0f}°F. Reduced power and enriched mixture. "
                "Oil seems to be burning faster than normal."
            )
        else:
            notes = (
                f"High CHT again {params['cht_max']:.0f}°F. Leaned aggressively to bring it down. Engine feels down on power."
            )
    elif tail == "N1156P" and not is_n1156p_exhibit:
        if rng.random() < 0.15:
            note_options = [
                "Good flight, all normal.",
                "Slight hesitation on startup, cleared after warmup.",
                "Oil temp a little warm today, ambient hot.",
                f"CHT running {params['cht_max']:.0f}°F at cruise, leaned as per POH.",
            ]
            notes = str(rng.choice(note_options))
    elif tail != "N1156P" and rng.random() < 0.12:
        note_options = [
            "Smooth flight, all instruments normal.",
            "Slight turbulence below 3000 ft, otherwise uneventful.",
            "Pattern work, 6 T&Gs.",
            "Crosswind practice.",
            "Good flight.",
        ]
        notes = str(rng.choice(note_options))

    if tail == "N1156P" and is_n1156p_exhibit and not is_n1156p_severe:
        symptom_notes = [
            f"CHT elevated at {params['cht_max']:.0f}°F during cruise. Enriched mixture helped slightly.",
            "Engine feels slightly rough at cruise power. Oil consumption seems higher than normal.",
            f"Rough running on climbout, smoothed out in cruise. CHT {params['cht_max']:.0f}°F.",
            "Added quart of oil before flight — burning more than usual.",
            "Intermittent roughness when leaned. Kept mixture rich.",
        ]
        notes = str(rng.choice(symptom_notes))

    return notes


def _chronological_day_offsets_into_history(
    rng: np.random.Generator,
    count: int,
    total_span_days: float,
) -> np.ndarray:
    """
    Strictly increasing offsets in [0, total_span_days] from history_start toward the anchor.

    The last RECENT_FLIGHT_COUNT points are sampled in the final RECENT_FLIGHT_DAYS_BEFORE_ANCHOR
    days so N4798E / N2251K / N8834Q always show recent activity on the Flights page.
    """
    if count <= 0:
        return np.array([], dtype=float)
    k = min(RECENT_FLIGHT_COUNT, count)
    early_n = count - k
    t = float(total_span_days)
    r = float(RECENT_FLIGHT_DAYS_BEFORE_ANCHOR)
    if t <= r + 1e-9:
        return np.sort(rng.uniform(0.0, t, size=count))
    early_hi = t - r
    early = np.sort(rng.uniform(0.0, early_hi, size=early_n)) if early_n > 0 else np.array([], dtype=float)
    recent_lo = t - r
    recent = np.sort(rng.uniform(recent_lo, t, size=k))
    if early_n > 0:
        return np.concatenate([early, recent])
    return recent


def _apply_n4798e_overrides(flights: list[dict[str, Any]], rng: np.random.Generator) -> list[dict[str, Any]]:
    """Apply N4798E story overrides: Flagstaff day-trips in summer months.

    After changing individual flight durations, rebuild the full hobbs/tach chain
    so every flight's hobbs_start, hobbs_end, tach_start, and tach_end are
    consistent and contiguous.
    """
    for f in flights:
        dt = datetime.strptime(f["timestamp"], "%Y-%m-%d %H:%M:%S")
        if dt.month in (6, 7, 8) and rng.random() < 0.3:
            orig_dur = float(f["duration"])
            new_dur = float(np.clip(rng.uniform(1.2, 2.0), 0.5, 4.0))
            if orig_dur > 0:
                f["fuel_used_gal"] = round(float(f["fuel_used_gal"]) * (new_dur / orig_dur), 2)
            f["route"] = "KPHX-KFLG"
            f["duration"] = round(new_dur, 2)

    # Rebuild hobbs/tach chain so all flights are contiguous after duration changes
    hobbs = float(flights[0]["hobbs_start"])
    for f in flights:
        f["hobbs_start"] = round(hobbs, 1)
        dur = float(f["duration"])
        f["hobbs_end"] = round(hobbs + dur, 1)
        f["tach_start"] = round(hobbs * 0.92, 1)
        f["tach_end"] = round((hobbs + dur) * 0.92, 1)
        hobbs = f["hobbs_end"]

    return flights


# ---------------------------------------------------------------------------
# Maintenance records
# ---------------------------------------------------------------------------

def build_all_maintenance_by_tail(anchor: datetime | None = None) -> dict[str, list[dict[str, Any]]]:
    """
    Per-tail maintenance CSV rows (scheduled work + squawks), ordered for export.

    Calendar date fields are offsets from the demo anchor; tach/hobbs match the fixed story.
    """
    a = anchor or get_demo_anchor()
    accident = n1156p_accident_datetime(a)
    fail_iso = format_n1156p_accident_iso(a)

    n1156p_annual = accident - timedelta(days=171)
    n1156p_next_annual = (n1156p_annual + timedelta(days=365)).strftime("%Y-%m-%d")
    n1156p_post = accident + timedelta(days=43)

    maint: dict[str, list[dict[str, Any]]] = {
        "N4798E": [
            {
                "date": _d(a, -295),
                "component_id": "N4798E",
                "maintenance_type": "annual",
                "description": "Annual inspection completed. All systems checked per FAR 43 Appendix D. Minor discrepancies noted and corrected. Aircraft returned to service.",
                "hobbs_at_service": 4739.0,
                "tach_at_service": 4360.9,
                "next_due_hobbs": "",
                "next_due_date": _d(a, 70),
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
                "date": _d(a, -95),
                "component_id": "N4798E-ENGINE",
                "maintenance_type": "oil_change",
                "description": "Oil and filter change. 50-hour interval. Drained 7 qts 15W-50. Cut filter — trace ferrous particles, within normal limits for this engine variant. Oil analysis sent to Blackstone Labs.",
                "hobbs_at_service": 4791.6,
                "tach_at_service": 4408.3,
                "next_due_hobbs": "",
                "next_due_tach": 4458.3,
                "next_due_date": "",
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
                "date": _d(a, -300),
                "component_id": "N2251K",
                "maintenance_type": "annual",
                "description": "Annual inspection completed. Seat tracks inspected per AD 2011-10-09. Exhaust inspected per AD 90-06-03 R1. Aircraft returned to service.",
                "hobbs_at_service": 5092.0,
                "tach_at_service": 4684.6,
                "next_due_hobbs": "",
                "next_due_date": _d(a, 65),
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
                "date": _d(a, -326),
                "component_id": "N2251K-ENGINE",
                "maintenance_type": "oil_change",
                "description": "50-hour oil and filter change. No anomalies. Cut filter clean.",
                "hobbs_at_service": 5058.4,
                "tach_at_service": 4653.7,
                "next_due_hobbs": "",
                "next_due_tach": 4703.7,
                "next_due_date": "",
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
                "date": _d(a, -66),
                "component_id": "N2251K-ENGINE",
                "maintenance_type": "oil_change",
                "description": "50-hour oil and filter change. Cut filter — no metal. Last oil analysis normal.",
                "hobbs_at_service": 5112.7,
                "tach_at_service": 4703.7,
                "next_due_hobbs": "",
                "next_due_tach": 4753.7,
                # Calendar leg still before due (tach leg is the only overdue leg for this tail).
                "next_due_date": _d(a, 42),
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
                "date": _d(a, -201),
                "component_id": "N8834Q",
                "maintenance_type": "annual",
                "description": "Annual inspection completed. All systems airworthy. Magnetos timed and tested. No discrepancies. Aircraft returned to service.",
                "hobbs_at_service": 4758.2,
                "tach_at_service": 4377.5,
                "next_due_hobbs": "",
                "next_due_date": _d(a, 164),
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
                "date": _d(a, -193),
                "component_id": "N8834Q-ENGINE",
                "maintenance_type": "oil_change",
                "description": "50-hour oil change concurrent with annual. No anomalies.",
                "hobbs_at_service": 4758.2,
                "tach_at_service": 4377.5,
                "next_due_hobbs": "",
                "next_due_tach": 4426.2,
                "next_due_date": "",
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
                "date": _d(a, -63),
                "component_id": "N8834Q-ENGINE",
                "maintenance_type": "oil_change",
                "description": "50-hour oil and filter change. Cut filter — very fine metallic particles, borderline. Noted for follow-up. Sent to Blackstone.",
                "hobbs_at_service": 4811.1,
                "tach_at_service": 4426.2,
                "next_due_hobbs": "",
                "next_due_tach": 4476.2,
                "next_due_date": "",
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
                "date": n1156p_annual.strftime("%Y-%m-%d"),
                "component_id": "N1156P",
                "maintenance_type": "annual",
                "description": "Annual inspection completed. Minor squawks noted. ELT battery replaced. Aircraft returned to service.",
                "hobbs_at_service": 5282.3,
                "tach_at_service": 4859.7,
                "next_due_hobbs": "",
                "next_due_date": n1156p_next_annual,
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
                "date": (accident - timedelta(days=166)).strftime("%Y-%m-%d"),
                "component_id": "N1156P-ENGINE",
                "maintenance_type": "oil_change",
                "description": "Oil change concurrent with annual. Cut filter clean. No anomalies.",
                "hobbs_at_service": 5282.3,
                "tach_at_service": 4859.7,
                "next_due_hobbs": "",
                "next_due_tach": 4909.7,
                "next_due_date": "",
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
                "date": (accident - timedelta(days=28)).strftime("%Y-%m-%d"),
                "component_id": "N1156P-ENGINE",
                "maintenance_type": "oil_change",
                "description": "50-hour oil change. OVERDUE — 53.2 hours since last change. Pilot reported oil consumption has increased. Cut filter shows elevated fine metallic particles. RECOMMEND BORESCOPE AND OIL ANALYSIS BEFORE NEXT FLIGHT. Blackstone sample sent.",
                "hobbs_at_service": 5385.5,
                "tach_at_service": 4954.7,
                "next_due_hobbs": "",
                "next_due_tach": 5004.7,
                "next_due_date": "",
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
                "date": n1156p_post.strftime("%Y-%m-%d"),
                "component_id": "N1156P-ENGINE",
                "maintenance_type": "post_accident_inspection",
                "description": (
                    f"Post-accident teardown inspection following catastrophic engine failure on {fail_iso}. "
                    "Cylinder #2 connecting rod failed and exited through case. Evidence of chronic lean detonation found: "
                    "piston crown erosion, cylinder head erosion consistent with detonation, valve face pitting. "
                    "Engine is NOT REPAIRABLE. Replacement required."
                ),
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

    squawks: dict[str, list[dict[str, Any]]] = {
        "N4798E": [
            {
                "date": _d(a, -58),
                "component_id": "N4798E-ENGINE",
                "maintenance_type": "squawk",
                "description": "Minor oil seep at rocker cover gasket — right rear cylinder. Not affecting oil level measurably. Deferred to next scheduled maintenance.",
                "hobbs_at_service": 4810.1,
                "tach_at_service": 4425.3,
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
                "date": fail_iso,
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

    return {t: maint[t] + squawks[t] for t in TAILS}


def get_all_maintenance(tail: str, *, anchor: datetime | None = None) -> list[dict[str, Any]]:
    """Return maintenance records + squawks for a given tail (calendar fields from anchor)."""
    by_tail = build_all_maintenance_by_tail(anchor)
    all_records = list(by_tail.get(tail, []))
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
