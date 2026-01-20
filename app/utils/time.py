"""Time period utilities."""

from datetime import date, timedelta
from typing import Literal, Tuple, Optional


def get_week_start(d: date) -> date:
    """Get the Sunday start of the week for a given date."""
    days_since_sunday = (d.weekday() + 1) % 7
    return d - timedelta(days=days_since_sunday)


def get_month_start(d: date) -> date:
    """Get the first day of the month for a given date."""
    return d.replace(day=1)


def get_last_n_periods(
    n: int,
    granularity: Literal["weekly", "monthly"] = "weekly",
    end_date: Optional[date] = None,
) -> Tuple[date, date]:
    """Get date range for last N periods.

    Args:
        n: Number of periods
        granularity: 'weekly' or 'monthly'
        end_date: End date (default: today)

    Returns:
        Tuple of (start_date, end_date)
    """
    if end_date is None:
        end_date = date.today()

    if granularity == "weekly":
        # End at the most recent complete week (last Saturday)
        end_week_start = get_week_start(end_date)
        if end_week_start == get_week_start(date.today()):
            # Current week is incomplete, go back one week
            end_week_start = end_week_start - timedelta(days=7)

        end = end_week_start + timedelta(days=6)  # Saturday
        start = end_week_start - timedelta(days=7 * (n - 1))
    else:
        # End at the last day of the previous month
        this_month_start = get_month_start(end_date)
        end = this_month_start - timedelta(days=1)

        # Start at the first day of n months ago
        start_month = this_month_start
        for _ in range(n):
            start_month = (start_month - timedelta(days=1)).replace(day=1)
        start = start_month

    return start, end


def get_yoy_comparison_dates(
    month: date,
) -> Tuple[Tuple[date, date], Tuple[date, date]]:
    """Get date ranges for year-over-year comparison.

    Args:
        month: The month to compare (any date in that month)

    Returns:
        Tuple of ((current_start, current_end), (previous_start, previous_end))
    """
    current_start = get_month_start(month)

    # End of current month
    if current_start.month == 12:
        current_end = current_start.replace(year=current_start.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        current_end = current_start.replace(month=current_start.month + 1, day=1) - timedelta(days=1)

    # Previous year same month
    previous_start = current_start.replace(year=current_start.year - 1)
    previous_end = current_end.replace(year=current_end.year - 1)

    return (current_start, current_end), (previous_start, previous_end)
