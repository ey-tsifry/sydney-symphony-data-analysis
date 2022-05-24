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
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from bs4.element import ResultSet

from sso_utilities import common, file_utils

# %%
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# %%
@dataclass
class CalendarRecord:
    """
    Data class to store all calendar concert keys for a given year.
    """

    year: int
    keys: Set[str]


# %%
class CalendarJsonProcessor:
    """
    Class for processing and merging SSO calendar JSON files for a specified year.
    """

    def __init__(self, season_year: int, latest_year: int = common.LATEST_SSO_SEASON_YEAR) -> None:
        """
        :param season_year: Input season year
        :param latest_year: Latest SSO season (year)
        """
        try:
            common.validate_sso_json_calendar_year(season_year, latest_year)
        except ValueError as e:
            logger.error(e)
            raise e
        self.season_year = season_year

    @staticmethod
    def load_calendar_json(json_file: str) -> Dict[str, Any]:
        """
        Load content from a season calendar JSON file.

        :param json_file: JSON file
        :return: JSON file content
        """
        json_content: Dict[str, Any] = {}
        try:
            file_processor: file_utils.ProcessJSON = file_utils.ProcessJSON()
            json_content = file_processor.load_json(json_file)
        except OSError as e:
            raise e
        return json_content

    def filter_season_data(self, json_content: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter JSON content to just the concerts in the specified season year.

        :param json_content: Calendar JSON content
        :return: Filtered Calendar JSON content
        """
        if "data" not in json_content:
            error_msg: str = f"[{str(self.season_year)}] Calendar JSON is missing a 'data' key"
            logger.error(error_msg)
            raise KeyError(error_msg)
        json_content["data"] = [
            concert
            for concert in json_content["data"]
            if str(self.season_year) in concert.get("concertSeason", "")
        ]
        return json_content

    def extract_unique_concert_urls(self, concert_list: List[Dict[str, Any]]) -> Set[str]:
        """
        Extract unique list of concert URLs from JSON calendar.

        :param concert_list: List of concert items in the JSON calendar
        :return: Set of concert URLs
        """
        concert_urls: Set[str] = set(
            filter(lambda url: url, sorted(set(concert.get("url", "") for concert in concert_list)))
        ) if concert_list else set()
        return concert_urls

    def _concert_url_diff(self, old_json_urls: Set[str], new_json_urls: Set[str]):
        """
        Return all concert URLs in the new JSON file that are not in the old JSON file.
        (These are new concerts that have been added since the old JSON was published).

        :param old_json_urls: Concert URLs in old JSON calendar
        :param new_json_urls: Concert URLs in new JSON calendar
        :return: New concert URLs that are not in the old JSON calendar
        """
        return new_json_urls.difference(old_json_urls)

    def merge_json_calendars(self, old_json_file: str, new_json_file: str) -> Dict[str, Any]:
        """
        Merge content from the old and new JSON calendars.

        :param old_json_file: Old JSON calendar file
        :param new_json_file: New JSON calendar file
        :return: Merged JSON calendar
        """
        old_json_content: Dict[str, Any] = {}
        new_json_content: Dict[str, Any] = {}
        try:
            old_json_content = self.load_calendar_json(old_json_file)
            new_json_content = self.load_calendar_json(new_json_file)
        except OSError as e:
            logger.error(f"[{str(self.season_year)}] Calendar JSON failed to load: {e}")
            raise e

        logger.info(
            f"Old JSON: {len(old_json_content['data'])} concerts, "
            f"New JSON: {len(new_json_content['data'])} concerts"
        )

        merged_json_content: Dict[str, Any] = old_json_content.copy()

        # extract URLs for concerts that are in new_json_content but not in old_json_content
        concert_url_diff: Set[str] = self._concert_url_diff(
            self.extract_unique_concert_urls(old_json_content["data"]),
            self.extract_unique_concert_urls(new_json_content["data"]),
        )
        # extract metadata for the new concert URLs
        new_concert_data: List[Dict[str, Any]] = [
            concert for concert in new_json_content["data"] if concert["url"] in concert_url_diff
        ]
        # merge new and old concert data
        merged_json_content["data"].extend(new_concert_data)
        # copy newest metadata to merged json
        merged_json_content["meta"] = new_json_content["meta"]
        return merged_json_content

    def export_merged_json_calendar(
        self, merged_json_content: Dict[str, Any], export_json: str
    ) -> None:
        """
        Export merged JSON calendar to a JSON file.

        :param merged_json_content: Merged JSON calendar
        :param export_json: Name of export JSON file
        """
        with open(export_json, "w") as out_file:
            json.dump(merged_json_content, out_file)
        return None


# %%
class SSOCalendar:
    """Methods for extracting unique concert identifiers from SSO season calendar files."""

    def __init__(self, year: int) -> None:
        """
        :param year: A year between 2018 and the year of the latest published season
        """
        self.year = year

    @property
    def html_calendar(self) -> List[CalendarRecord]:
        """Return concert keys extracted from calendar HTML files."""
        return self._parse_html_calendar()

    @property
    def json_calendar(self) -> CalendarRecord:
        """Return concert keys extracted from calendar JSON file."""
        return self._parse_json_calendar()

    def _parse_single_html_calendar_file(
        self, file_processor: file_utils.ProcessHTML, month: str
    ) -> Optional[CalendarRecord]:
        """
        Load and parse 2018-2020 SSO season calendar data for a single month from a local HTML file.
        Each HTML calendar contains multiple concerts.

        Month is used to construct the file name and path: e.g. 2018/sso_2018_February.html

        :param file_processor: ProcessHTML object for help with loading HTML files
        :param month: Calendar month
        :return: CalendarRecord with year and a list of related concert keys
        """
        year_str: str = str(self.year)
        html_file: str = os.path.join(year_str, f"sso_{year_str}_{month}.html")
        logger.info(f"[{year_str}] loading {html_file}...")

        concerts: Optional[ResultSet] = None
        try:
            html_content = file_processor.load_html(html_file)
        except OSError as e:
            logger.error(f"[{year_str}] Calendar HTML file failed to load: {e}")
            raise e
        if html_content:
            concerts = html_content.find_all(class_="reveal calendar-perf-modal")

        if not concerts:
            logger.info(f"[{year_str}] Skipping {html_file} ... no concerts found to extract")
            return None

        # initialise calendar record with pre-filled year
        calendar_record: CalendarRecord = CalendarRecord(year=self.year, keys=set())

        # add concert keys to the calendar_record.keys set
        for row in concerts:
            key: str = row.find("a", attrs={"alt": "Read More"})["href"].strip().split("/")[-1]
            calendar_record.keys.add(key)
        return calendar_record

    def _parse_html_calendar(
        self, month_list: List[str] = common.SSO_SEASON_MONTHS
    ) -> List[CalendarRecord]:
        """
        Load and parse 2018-2020 SSO season calendar data for all months in a particular year.

        :param month_list: List of months in the SSO season
        :return: List of CalendarRecords with all concert keys for the specified year
        """
        if (
            self.year < common.EARLIEST_SSO_SEASON_YEAR
            or self.year >= common.JSON_CALENDAR_START_YEAR
        ):
            error_msg: str = (
                f"Input year must be between {str(common.EARLIEST_SSO_SEASON_YEAR)} "
                f"and {str(common.JSON_CALENDAR_START_YEAR - 1)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        html_concert_list: List[CalendarRecord] = []
        file_processor: file_utils.ProcessHTML = file_utils.ProcessHTML()

        for month in month_list:
            html_concert: Optional[CalendarRecord] = None
            try:
                html_concert = self._parse_single_html_calendar_file(file_processor, month)
            # fail entire process if a file fails to load or parse
            # (one month could have a large # of concerts)
            except OSError as os_error:
                raise os_error
            except ValueError as value_error:
                logger.error(
                    f"[{str(self.year)}] Error while parsing calendar HTML file: {value_error}"
                )
                raise value_error
            # but don't fail if there were no actual concerts to extract from a file
            if html_concert:
                html_concert_list.append(html_concert)
        return html_concert_list

    def _parse_json_calendar(self) -> CalendarRecord:
        """
        Load and parse post-2020 SSO season calendar from local JSON file.

        Post-2020 SSO season calendars are available in JSON format (so far), whereas previous
        season calendars were only available in HTML format.

        Example file: 2021/sso-concerts-2021.json

        :return: CalendarRecord with year and a list of related concert keys
        """
        try:
            common.validate_sso_json_calendar_year(self.year)
        except ValueError as e:
            logger.error(e)
            raise e

        year_str: str = str(self.year)
        json_file: str = os.path.join(year_str, f"sso-concerts-{year_str}.json")
        logger.info(f"[{year_str}] loading {json_file}...")

        concerts: Dict[str, Any] = {}
        calendar_processor = CalendarJsonProcessor(self.year)
        try:
            concerts = calendar_processor.filter_season_data(
                calendar_processor.load_calendar_json(json_file)
            )
        except OSError as os_error:
            logger.error(f"[{year_str}] Calendar JSON failed to load: {os_error}")
            raise os_error
        except KeyError as key_error:
            logger.error(f"[{year_str}] Calendar JSON is missing a 'data' key: {key_error}")
            raise key_error

        # initialise calendar record with pre-filled year and no keys
        calendar_record: CalendarRecord = CalendarRecord(year=self.year, keys=set())
        if not concerts.get("data", []):
            logger.info(f"[{year_str}] Skipping {json_file} ... no concerts found to extract")
            return calendar_record

        # add concert keys to the calendar_record.keys set
        concert_urls: Set[str] = calendar_processor.extract_unique_concert_urls(concerts["data"])
        concert_keys: Set[str] = set(
            url.split("/")[-1].strip() for url in concert_urls if url
        ) if concert_urls else set()
        if concert_keys:
            calendar_record.keys = concert_keys
        return calendar_record


# %%
def _create_sso_calendar_key_df(calendar_obj: SSOCalendar) -> pd.DataFrame:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_obj: SSOCalendar instance for a particular year
    :return: Dataframe with concert year and keys
    """
    if not calendar_obj.year:
        error_msg = "Calendar object is missing a year value"
        logger.error(error_msg)
        raise ValueError(error_msg)

    concert_list: List[CalendarRecord] = []
    if calendar_obj.year >= common.JSON_CALENDAR_START_YEAR:
        json_calendar: Optional[CalendarRecord] = None
        try:
            json_calendar = calendar_obj.json_calendar
        except (KeyError, OSError, ValueError) as e:
            raise e
        if json_calendar and json_calendar.keys:
            concert_list.append(json_calendar)
    else:
        html_calendar: List[CalendarRecord] = []
        try:
            html_calendar = calendar_obj.html_calendar
        except (OSError, ValueError) as e:
            raise e
        if html_calendar:
            concert_list.extend(html_calendar)

    calendar_df: pd.DataFrame = pd.DataFrame(concert_list) if concert_list else pd.DataFrame()
    if not calendar_df.empty:
        calendar_df = (
            calendar_df.explode("keys").drop_duplicates().dropna(how="any").reset_index(drop=True)
        )
    return calendar_df


def _export_sso_calendar_key_df(calendar_df: pd.DataFrame, export_csv: str) -> None:
    """
    Return dataframe with SSO season concert keys.

    :param calendar_df: Dataframe with concert years and keys
    :param export_csv: Name of export CSV file
    """
    # write concert keys to a temp file first
    tempfile.tempdir = os.path.curdir
    csv_tempfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        calendar_df["keys"].sort_values().to_csv(
            path_or_buf=csv_tempfile, index=False, header=False
        )
    except (KeyError, OSError) as e:
        logger.error(
            "Failed to write dataframe to temp file: "
            f"{os.path.abspath(csv_tempfile.name)} ({len(calendar_df)} keys)"
        )
        raise e
    csv_tempfile.close()

    # rename existing CSV if it exists
    if os.path.exists(export_csv):
        old_file: str = (
            os.path.splitext(export_csv)[0] + f".{datetime.strftime(datetime.now(), '%Y%m%d')}.csv"
        )
        try:
            os.rename(export_csv, old_file)
        except OSError:
            os.remove(old_file)
            os.rename(export_csv, old_file)
        logger.info(f"Found existing '{export_csv}'. Renamed to: {old_file}")

    # move temp file to CSV
    try:
        shutil.move(csv_tempfile.name, export_csv)
    except OSError as e:
        logger.error(
            f"Failed to copy {os.path.abspath(csv_tempfile.name)} to: "
            f"{os.path.abspath(export_csv)} ({len(calendar_df)} keys)"
        )
        raise e
    return None


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
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    # subparser for extracting unique concert ID strings
    parser_one = subparsers.add_parser(
        "extract_concert_ids",
        help="Extract unique concert identifier strings from pre-fetched SSO calendar files",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser_one.add_argument(
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
    parser_one.add_argument(
        "--csv-prefix",
        type=str,
        default="sso",
        help="(optional) Specify an alternative CSV file name prefix. Default prefix: sso",
    )
    # subparser for updating concert season JSON calendars for SSO seasons >= 2021
    parser_two = subparsers.add_parser(
        "update_calendar_json",
        help="Merge old and new concert season JSON calendars into a new file",
    )
    parser_two.add_argument(
        "-y",
        "--year",
        required=True,
        type=int,
        choices=range(common.JSON_CALENDAR_START_YEAR, common.LATEST_SSO_SEASON_YEAR + 1, 1),
        help=" ".join(
            [
                "SSO season calendar year between",
                f"{common.JSON_CALENDAR_START_YEAR} and {common.LATEST_SSO_SEASON_YEAR}"
            ]
        ),
    )
    parser_two.add_argument(
        "--old-json-file", required=True, type=str, help="Old concert season JSON file"
    )
    parser_two.add_argument(
        "--new-json-file", required=True, type=str, help="New concert season JSON file"
    )
    return parser


# %%
def main():
    """."""
    # get CLI args
    args: argparse.Namespace = _get_cli_args().parse_args()

    if args.command == "update_calendar_json":
        calendar_processor = CalendarJsonProcessor(args.year)
        merged_json: Dict[str, Any] = calendar_processor.merge_json_calendars(
            os.path.join(str(args.year), args.old_json_file),
            os.path.join(str(args.year), args.new_json_file),
        )
        try:
            export_json: str = os.path.join(str(args.year), f"sso-concerts-merged-{args.year}.json")
            calendar_processor.export_merged_json_calendar(merged_json, export_json)
        except OSError as e:
            logger.error(f"Error while saving the merged JSON calendar file: {e}")
            raise e
        logger.info(
            f"Successfully saved merged JSON calendar file: {os.path.abspath(export_json)} "
            f"[{len(merged_json['data'])} concerts]"
        )

    if args.command == "extract_concert_ids":
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
            calendar_df: pd.DataFrame = pd.DataFrame()
            try:
                calendar_df = _create_sso_calendar_key_df(calendar)
            except (KeyError, OSError, ValueError) as e:
                logger.warning(
                    f"[{year}] Skipping year because there was an error generating "
                    f"the calendar dataframe: {e}"
                )
                continue
            if calendar_df.empty:
                logger.info(f"[{year}] Skipping year because calendar dataframe is empty...")
                continue

            # write season concert keys to disk
            try:
                out_file: str = os.path.join(year_str, f"{args.csv_prefix}_{year_str}_keys.csv")
                _export_sso_calendar_key_df(calendar_df, out_file)
            except OSError as e:
                logger.error(f"[{year}] Error while trying to save concert key CSV: {e}")
            else:
                logger.info(
                    f"[{year}] Successfully saved CSV: "
                    f"{os.path.abspath(out_file)} [{len(calendar_df)} keys]"
                )


# %%
if __name__ == "__main__":
    main()
