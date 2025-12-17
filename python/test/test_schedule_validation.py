"""
Tests for schedule validation functionality.
"""

import pytest
from ai.chronon.schedule_validation import validate_at_most_daily_schedule


class TestScheduleValidation:
    """Test schedule validation for at-most-daily schedules."""

    def test_valid_daily_schedules(self):
        """Test that valid daily schedules pass validation."""
        valid_schedules = [
            "@daily",
            "0 0 * * *",  # midnight daily
            "0 2 * * *",  # 2am daily
            "30 14 * * *",  # 2:30pm daily
            "15 23 * * SUN",  # Sunday at 11:15pm
            "0 9 * * MON-FRI",  # weekdays at 9am
            "0 8 * * 1-5",  # weekdays at 8am (numeric)
            "45 16 * * 6",  # Saturday at 4:45pm
            None,  # no schedule
            "none",  # disabled schedule
            "None",  # disabled schedule
        ]

        for schedule in valid_schedules:
            result = validate_at_most_daily_schedule(schedule)
            assert result is None, f"Expected valid schedule '{schedule}' to pass validation, but got error: {result}"

    def test_invalid_schedules_too_frequent(self):
        """Test that schedules running more than once per day are rejected."""
        invalid_schedules = [
            "0 */2 * * *",  # every 2 hours
            "*/15 * * * *",  # every 15 minutes
            "0 9,14 * * *",  # twice daily at 9am and 2pm
            "0 * * * *",  # every hour
            "0 0,12 * * *",  # twice daily at midnight and noon
            "30 8-17 * * *",  # every hour from 8am to 5pm
        ]

        for schedule in invalid_schedules:
            result = validate_at_most_daily_schedule(schedule)
            assert result is not None, f"Expected invalid schedule '{schedule}' to fail validation, but it passed"
            assert "times" in result.lower() or "frequent" in result.lower(), f"Error message should mention frequency: {result}"

    def test_invalid_syntax(self):
        """Test that invalid cron syntax is rejected."""
        invalid_syntax = [
            "0 25 * * *",  # invalid hour
            "60 0 * * *",  # invalid minute
            "0 0 32 * *",  # invalid day of month
            "0 0 * 13 *",  # invalid month
            "0 0 * * 8",  # invalid day of week
            "invalid",  # not a cron expression
            "too many fields here * * *",  # wrong field count
        ]

        for schedule in invalid_syntax:
            result = validate_at_most_daily_schedule(schedule)
            assert result is not None, f"Expected invalid syntax '{schedule}' to fail validation, but it passed"

    def test_empty_strings_are_valid(self):
        """Test that empty strings are treated as valid (no schedule)."""
        empty_schedules = ["", "   ", None]
        for schedule in empty_schedules:
            result = validate_at_most_daily_schedule(schedule)
            assert result is None, f"Expected empty schedule '{schedule}' to be valid, but got error: {result}"

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Test different day representations
        assert validate_at_most_daily_schedule("0 9 * * 0") is None  # Sunday (0)
        assert validate_at_most_daily_schedule("0 9 * * 7") is None  # Sunday (7)

        # Test month boundaries
        assert validate_at_most_daily_schedule("0 9 1 * *") is None  # 1st of month
        assert validate_at_most_daily_schedule("0 9 31 * *") is None  # 31st of month

        # Test time boundaries
        assert validate_at_most_daily_schedule("0 0 * * *") is None  # midnight
        assert validate_at_most_daily_schedule("59 23 * * *") is None  # 23:59