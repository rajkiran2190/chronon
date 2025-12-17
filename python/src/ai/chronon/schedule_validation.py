"""
Schedule validation utilities for Chronon Python API.

This module provides validation for schedule expressions to ensure they run at most once per day.
Uses croniter library for proper cron parsing and validation.
"""

import datetime
from typing import Union

from croniter import croniter


def validate_at_most_daily_schedule(schedule_expression: str) -> Union[str, None]:
    """
    Validates that a schedule expression runs at most once per day.

    Args:
        schedule_expression: The schedule expression to validate

    Returns:
        None if valid, error message string if invalid

    Examples of valid expressions:
        - "@daily" (legacy format)
        - "0 2 * * *" (daily at 2am)
        - "30 14 * * MON-FRI" (weekdays at 2:30pm)
        - "0 9 * * 1" (Mondays at 9am)
        - "15 23 * * SUN" (Sundays at 11:15pm)

    Examples of invalid expressions:
        - "0 */2 * * *" (every 2 hours - too frequent)
        - "*/15 * * * *" (every 15 minutes - too frequent)
        - "0 9,14 * * *" (twice daily - too frequent)
    """
    if not schedule_expression or schedule_expression in ("None", ""):
        return None

    schedule_expression = schedule_expression.strip()

    if not schedule_expression:
        return None

    # Allow legacy @daily format
    if schedule_expression.lower() == "@daily":
        return None

    # Allow None/none for disabling schedules
    if schedule_expression.lower() in ("none", "null"):
        return None

    # Validate cron expression syntax
    try:
        # Test if croniter can parse the expression
        base_time = datetime.datetime(2024, 1, 1, 0, 0)  # Start from a Monday
        cron = croniter(schedule_expression, base_time)
    except (ValueError, TypeError) as e:
        return f"Invalid cron expression syntax: {e}"

    # Check frequency by counting executions over multiple 24-hour periods
    try:
        # Test over a week to catch various day-of-week patterns
        test_start = datetime.datetime(2024, 1, 1, 0, 0)  # Monday

        for day_offset in range(7):  # Test each day of the week
            day_start = test_start + datetime.timedelta(days=day_offset)
            day_end = day_start + datetime.timedelta(days=1)

            # Start croniter just before the day starts to catch all executions in the day
            cron_start = day_start - datetime.timedelta(seconds=1)
            cron = croniter(schedule_expression, cron_start)
            executions_in_day = 0

            # Count executions within this 24-hour period
            for _ in range(200):  # Safety limit to prevent infinite loops
                next_run = cron.get_next(datetime.datetime)
                if next_run >= day_end:
                    break
                executions_in_day += 1

                # If more than 1 execution in a day, it's too frequent
                if executions_in_day > 1:
                    return f"Schedule runs {executions_in_day} times on {day_start.strftime('%A')} ({day_start.strftime('%Y-%m-%d')}). Only at-most-daily schedules are allowed."

    except Exception as e:
        return f"Error validating schedule frequency: {e}"

    return None