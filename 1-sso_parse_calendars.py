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
import argparse
import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional

import pandas as pd
from bs4.element import ResultSet

from sso_utilities import common, file_utils

# %%
logger = logging.getLogger(__name__)


# %%
class SSOCalendar:
    """Methods for extracting unique concert identifiers from SSO season calendar files."""

    def __init__(self, year: int) -> None:
        """
        Initialise with year and template calendar record.

        :param year: A year between 2018 and the year of the latest published season
        """
        self.year: int = year
        self.calendar_record = namedtuple("calendar_record", "year keys")  # type: ignore

    @property
    def html_calendar(self) -> List[NamedTuple]:
        """Return concert keys extracted from calendar HTML files."""
        return self._parse_html_calendar()

    @property
    def json_calendar(self) -> NamedTuple:
        """Return concert keys extracted from calendar JSON file."""
        return self._parse_json_calendar()

    def _parse_single_html_calendar_file(
        self, file_processor: file_utils.ProcessHTML, month: str
    ) -> Optional[NamedTuple]:
        """
        Load and parse 2018-2020 SSO season calendar data for a single month from a local HTML file.

        Month is used to construct the file name and path: e.g. 2018/sso_2018_February.html

        :param file_processor: ProcessHTML object for help with loading HTML files
        :param month: Calendar month
        :return: Named tuple with year and a list of related concert keys
        """
        year_str: str = str(self.year)
        html_file: str = os.path.join(year_str, f"sso_{year_str}_{month}.html")
        logger.info(f"loading {html_file}...")

        try:
            concerts: ResultSet = file_processor.load_html(html_file).find_all(
                class_="reveal calendar-perf-modal"
            )
        except OSError as e:
            logger.error("Calendar HTML file failed to load")
            raise e

        if not concerts:
            logger.info(f"Skipping {html_file} ... no concerts found to extract")
            return None

        # initialise calendar_record object with pre-filled year
        concert_key_record = self.calendar_record(year=self.year, keys=[])

        # add concert keys to the calendar_record.key list
        for row in concerts:
            key: str = row.find("a", attrs={"alt": "Read More"})["href"].strip().split("/")[-1]
            if key not in concert_key_record.keys:
                concert_key_record.keys.append(key)
        return concert_key_record

    def _parse_html_calendar(
        self, month_list: List[str] = common.SSO_SEASON_MONTHS
    ) -> List[NamedTuple]:
        """
        Load and parse 2018-2020 SSO season calendar data for all months in a particular year.

        :param month_list: List of months in the SSO season
        :return: List of named tuples with all concert keys for the specified year
        """
        if self.year < common.EARLIEST_SSO_SEASON_YEAR or self.year > 2020:
            logger.error(
                f"Input year must be between {str(common.EARLIEST_SSO_SEASON_YEAR)} and 2020"
            )
            raise ValueError

        html_concert_list: List[NamedTuple] = []
        file_processor: file_utils.ProcessHTML = file_utils.ProcessHTML()

        for month in month_list:
            try:
                html_concert = self._parse_single_html_calendar_file(file_processor, month)
            except (OSError, ValueError) as e:
                raise e
            else:
                if html_concert:
                    html_concert_list.append(html_concert)
        return html_concert_list

    def _parse_json_calendar(self, latest_year: int = common.LATEST_SSO_SEASON_YEAR) -> NamedTuple:
        """
        Load and parse post-2020 SSO season calendar from local JSON file.

        Post-2020 SSO season calendars are available in JSON format (so far), whereas previous
        season calendars were only available in HTML format.

        Example file: 2021/sso-concerts-2021.json

        :param latest_year: Latest SSO season (year)
        :return: Named tuple with year and a list of related concert keys
        """
        if self.year < 2021 or self.year > latest_year:
            logger.error(f"Input year must be between 2021 and {latest_year}")
            raise ValueError

        year_str: str = str(self.year)
        json_file: str = os.path.join(year_str, f"sso-concerts-{year_str}.json")
        logger.info(f"loading {json_file}...")

        try:
            file_processor: file_utils.ProcessJSON = file_utils.ProcessJSON()
            concerts: Dict = file_processor.load_json(json_file)["data"]
        except (KeyError, OSError) as e:
            logger.error("Calendar JSON failed to load or is missing a 'data' key")
            raise e

        if not concerts:
            logger.error(f"No concerts extracted from {json_file}")
            raise ValueError

        # initialise calendar_record object with pre-filled year
        concert_key_record = self.calendar_record(year=self.year, keys=[])

        # add concert keys to the calendar_record.key list
        for row in concerts:
            if year_str in row["concertSeason"]:
                key: str = row["url"].split("/")[-1].strip()
                if key not in concert_key_record.keys:
                    concert_key_record.keys.append(key)
        return concert_key_record


# %%
def _create_sso_calendar_key_df(calendar_obj: SSOCalendar) -> pd.DataFrame:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_obj: SSOCalendar instance for a particular year
    :param month_list: List of months in the SSO season
    :return: Dataframe with concert year and keys
    """
    if not calendar_obj.year:
        logger.error("Calendar object is missing a year value")
        raise ValueError

    concert_list: List[NamedTuple] = []
    if calendar_obj.year >= 2021:
        try:
            json_calendar = calendar_obj.json_calendar
        except (KeyError, OSError, ValueError) as e:
            raise e
        else:
            concert_list.append(json_calendar)
    else:
        try:
            html_calendar = calendar_obj.html_calendar
        except (OSError, ValueError) as e:
            raise e
        else:
            concert_list.extend(html_calendar)

    calendar_df: pd.DataFrame = pd.DataFrame(concert_list).explode("keys").drop_duplicates().dropna(
        how="any"
    ).reset_index(drop=True)
    return calendar_df


def _export_sso_calendar_key_df(calendar_df: pd.DataFrame, export_csv_name: str) -> None:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_df: Dataframe with concert years and keys
    :param export_csv_name: Name of export CSV file
    """
    try:
        if os.path.exists(export_csv_name):
            old_file: str = (
                os.path.splitext(export_csv_name)[0]
                + f".{datetime.strftime(datetime.now(), '%Y%m%d')}.csv"
            )
            try:
                os.rename(export_csv_name, old_file)
            except OSError:
                os.remove(old_file)
                os.rename(export_csv_name, old_file)
            logger.info(f"Found existing '{export_csv_name}'. Renamed to: {old_file}")
        calendar_df["keys"].to_csv(path_or_buf=export_csv_name, index=False, header=False)
    except (KeyError, OSError) as e:
        logger.error(
            " ".join(
                [
                    "Failed to write to disk:",
                    f"{os.path.abspath(export_csv_name)} ({len(calendar_df)} keys)",
                ]
            )
        )
        raise e
    else:
        return


# %%
def _get_cli_args() -> argparse.ArgumentParser:
    """
    Get command line arguments.

    :return: ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description=" ".join(
            [
                "Extract unique concert identifier strings from",
                "pre-fetched SSO calendar files and output them to CSVs",
            ]
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-y",
        "--year",
        required=True,
        type=int,
        metavar="YEAR",
        dest="input_years",
        action="append",
        help="\n".join(
            [
                "Specify the SSO season calendar year(s) for which to extract concert identifiers.",
                "Example #1: '-y 2021 -y 2022'",
                "Example #2: '--year 2022'",
            ]
        ),
    )
    parser.add_argument(
        "--csv-prefix",
        type=str,
        default="sso",
        help="(optional) Specify an alternative CSV file name prefix. Default prefix: sso",
    )
    return parser


# %%
def main():
    """."""
    # set log level
    logging.basicConfig(level=logging.INFO)
    # get CLI args
    args: argparse.Namespace = _get_cli_args().parse_args()
    # check that input years are valid
    try:
        common.validate_sso_input_years(args.input_years)
    except ValueError as e:
        logger.error(e)
        raise e

    # export concert keys for each requested SSO season to a separate CSV
    for year in args.input_years:
        # create dataframe with concert keys for the specified season (year)
        calendar: SSOCalendar = SSOCalendar(year)
        year_str: str = str(year)
        logger.info(f"Processing calendar files for {year_str}:")
        calendar_df: pd.DataFrame = _create_sso_calendar_key_df(calendar)
        if calendar_df.empty:
            error_msg = "Cannot export empty calendar dataframe"
            logger.error(error_msg)
            raise ValueError(error_msg)
        # write season concert keys to disk
        try:
            out_file: str = os.path.join(year_str, f"{args.csv_prefix}_{year_str}_keys.csv")
            _export_sso_calendar_key_df(calendar_df, out_file)
        except OSError as e:
            raise e
        else:
            logger.info(
                " ".join(
                    [
                        "Successfully wrote to disk:",
                        f"{os.path.abspath(out_file)} ({len(calendar_df)} keys)",
                    ]
                )
            )


# %%
if __name__ == "__main__":
    main()
