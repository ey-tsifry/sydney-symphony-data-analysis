#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Common constants and utility functions."""

from typing import List


# earliest and latest years for which is there are full SSO season data sets
EARLIEST_SSO_SEASON_YEAR: int = 2018
LATEST_SSO_SEASON_YEAR: int = 2024

# year when SSO season calendar data switched to JSON
JSON_CALENDAR_START_YEAR: int = 2021

# default SQLite DB filename (may or may not already exist)
DEFAULT_SQLITE_DB: str = f"sso_html_{EARLIEST_SSO_SEASON_YEAR}_{LATEST_SSO_SEASON_YEAR}.db"

# SSO seasons are from February-December
SSO_SEASON_MONTHS: List[str] = [
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def validate_sso_input_years(input_years: List[int]) -> None:
    """
    Return whether SSO input years are within the valid accepted range.

    :param input_years: List of SSO input years
    :return: None if input years are within the accepted range
    """
    if not input_years:
        raise ValueError("List of input years is empty or invalid")
    min_year: int = min(input_years)
    max_year: int = max(input_years)
    if min_year < EARLIEST_SSO_SEASON_YEAR:
        raise ValueError(f"Input years cannot be earlier than {EARLIEST_SSO_SEASON_YEAR}")
    if max_year > LATEST_SSO_SEASON_YEAR:
        raise ValueError(f"Input years cannot be later than {LATEST_SSO_SEASON_YEAR}")
    return None


def validate_sso_json_calendar_year(
    season_year: int, latest_year: int = LATEST_SSO_SEASON_YEAR
) -> None:
    """
    Validate that JSON calendar input year is >= 2021 and <= the latest SSO season.

    :param season_year: Input season year
    :param latest_year: Latest SSO season (year)
    :return: None if input year is within a valid range
    """
    if season_year < JSON_CALENDAR_START_YEAR or season_year > latest_year:
        raise ValueError(f"Input JSON calendar year must be between 2021 and {latest_year}")
    return None
