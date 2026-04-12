"""
Calendar math for YYYY-MM-DD strings — matches client parseDateInput + calendarDaysUntil
(local civil dates, whole days from today to target).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional


def calendar_days_until_iso(date_str: str) -> Optional[int]:
    """
    Whole calendar days from today's local date to a date-only ISO string.

    Empty or invalid input returns None. Aligns with the React calendarDaysUntil helper
    when the API and browser share the same local timezone (typical local dev).
    """
    raw = (date_str or "").strip()
    if not raw:
        return None
    try:
        target_d = date.fromisoformat(raw)
    except ValueError:
        try:
            target_d = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None
    today_d = datetime.now().astimezone().date()
    return (target_d - today_d).days
