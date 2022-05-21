#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List

# earliest and latest years for which is there are full SSO season data sets
EARLIEST_SSO_SEASON_YEAR: int = 2018
LATEST_SSO_SEASON_YEAR: int = 2022

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
