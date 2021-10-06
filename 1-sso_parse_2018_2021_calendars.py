#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse pre-fetched 2018-2022 SSO calendar files and output CSVs with concert URL "keys".

(keys = unique string identifiers that the SSO website assigns to each concert)

Note that string identifiers are apparently only unique within the same year, not necessarily
across different years.

Prereq: Calendar HTML (2018-2020) and JSON (2021 and later) files have been downloaded to local disk
Purpose: Allows programmatic retrieval of individual concert files from the SSO website based on
concert key naming scheme
"""


# %%
import os
from collections import namedtuple
from typing import List, NamedTuple

import pandas as pd

from sso_utilities import file_utils


# %%
class SSOCalendar:
    """Methods for extracting unique concert identifiers from SSO season calendar files."""

    def __init__(self, year: int) -> None:
        """
        Methods for extracting unique concert identifiers from SSO season calendar files.

        :param year: A year between 2018 and 2022 (latest season)
        """
        self.year = year
        self.calendar_record = namedtuple("calendar_record", "year keys")  # type: ignore

    def parse_html_calendar(self, month: str) -> NamedTuple:
        """
        Load and parse 2018-2020 SSO season calendar from local HTML file.

        Month is used to construct the file name and path: e.g. 2018/sso_2018_February.html

        :param month: Example: February
        :return: Named tuple with year and a list of related concert keys
        """
        if self.year < 2018 or self.year > 2020:
            raise ValueError("Error: Year must be between 2018 and 2020")

        year_str = str(self.year)
        html_file = os.path.join(year_str, f"sso_{year_str}_{month}.html")
        print(f"loading {html_file}...")

        try:
            calendar_obj = file_utils.ProcessHTML()
            concerts = calendar_obj.load_html(html_file).find_all(
                class_="reveal calendar-perf-modal"
            )
        except OSError:
            print("Calendar HTML didn't load")
            raise
        else:
            # initialise calendar_record object with pre-filled year
            concert_key_record = self.calendar_record(year=self.year, keys=[])

            # add concert keys to the calendar_record.key list
            for row in concerts:
                key = row.find("a", attrs={"alt": "Read More"})["href"].strip().split("/")[-1]
                if key not in concert_key_record.keys:
                    concert_key_record.keys.append(key)
            return concert_key_record

    def parse_json_calendar(self, latest_year: int = 2022) -> NamedTuple:
        """
        Load and parse post-2020 SSO season calendar from local JSON file.

        Post-2020 SSO season calendars are available in JSON format (so far), whereas previous
        season calendars were only available in HTML format.

        Example file: 2021/sso-concerts-2021.json

        :param latest_year: Latest SSO season (year)
        :return: Named tuple with year and a list of related concert keys
        """
        if self.year < 2021 or self.year > latest_year:
            raise ValueError(f"Error: Year must be between 2021 and {latest_year}")

        year_str = str(self.year)
        json_file = os.path.join(year_str, f"sso-concerts-{year_str}.json")
        print(f"loading {json_file}...")

        try:
            calendar_obj = file_utils.ProcessJSON()
            concerts = calendar_obj.load_json(json_file)["data"]
        except (KeyError, OSError):
            print("Calendar JSON didn't load or is missing a 'data' key")
            raise
        else:
            # initialise calendar_record object with pre-filled year
            concert_key_record = self.calendar_record(year=self.year, keys=[])

            # add concert keys to the calendar_record.key list
            for row in concerts:
                if year_str in row["concertSeason"]:
                    key = row["url"].split("/")[-1].strip()
                    if key not in concert_key_record.keys:
                        concert_key_record.keys.append(key)
            return concert_key_record


# %%
def sso_create_calendar_key_df(calendar_obj: SSOCalendar, month_list: List[str]) -> pd.DataFrame:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_obj: SSOCalendar instance for a particular year
    :param month_list: List of months in the SSO season
    :return: Dataframe with concert year and keys
    """
    if not calendar_obj.year:
        raise ValueError("Calendar object is missing a year value.")

    concert_list: List[NamedTuple] = []
    if calendar_obj.year >= 2021:
        concert_list.append(calendar_obj.parse_json_calendar())
    else:
        concert_list.extend([calendar_obj.parse_html_calendar(month) for month in month_list])

    calendar_df: pd.DataFrame = pd.DataFrame(concert_list).explode("keys").drop_duplicates().dropna(
        how="any"
    ).reset_index(drop=True)
    return calendar_df


def sso_export_calendar_key_df(calendar_df: pd.DataFrame, export_csv_name: str) -> None:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_df: Dataframe with concert years and keys
    :param export_csv_name: Name of export CSV file
    """
    try:
        calendar_df["keys"].to_csv(path_or_buf=export_csv_name, index=False, header=False)
    except (KeyError, OSError):
        print(
            f"Failed to write to disk: {os.path.abspath(export_csv_name)} ({calendar_df.shape[0]} keys)"
        )
        raise
    else:
        return


# %%
def main():
    """."""
    # SSO seasons are from February-December
    MONTHS: List[str] = [
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
    # SSO season year(s) for which to extract concert keys
    YEAR_LIST: List[int] = [2022]

    # export concert keys for each SSO season to a separate CSV
    for year in YEAR_LIST:
        # create dataframe with concert keys for the specified season (year)
        calendar = SSOCalendar(year)
        year_str = str(year)
        print(f"\n{year_str}:")
        calendar_df = sso_create_calendar_key_df(calendar, MONTHS)
        # write season concert keys to disk
        try:
            out_file = os.path.join(year_str, f"sso_{year_str}_keys.csv")
            sso_export_calendar_key_df(calendar_df, out_file)
        except OSError:
            raise
        else:
            print(
                f"Successfully wrote to disk: {os.path.abspath(out_file)} ({calendar_df.shape[0]} keys)"
            )


# %%
if __name__ == "__main__":
    main()
