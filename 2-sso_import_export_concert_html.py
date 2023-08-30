#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combine individual SSO concert HTML file content into a DataFrame and export to a local
SQLite database

Prereq: HTML files have been downloaded to local disk
Purpose: Aggregate all concert information into an external database for more convenient future
processing
"""

# %%
import argparse
import logging
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import exc

from data_models.sqlite_db import DBRecord
from sso_utilities import common, file_utils

# %%
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# %%
def import_concerts_by_year(year: int, concert_key_list: List[str] = []) -> List[DBRecord]:
    """
    Return list of concert HTML file content for a specified year.
    Can specify individual concerts by concert key name, or all concerts (default).

    :param year: Year
    :param concert_key_list: (optional) List of concert keys (=names of concert HTML files)
    :return: List with concert HTML file content records
    """
    concert_record_list: List[DBRecord] = []

    # import existing HTML content from valid paths only
    year_str: str = str(year)
    event_path: Path = Path(os.path.join(year_str, "events"))
    if not event_path.exists():
        raise OSError(f"[{year_str}] Events directory does not exist: {event_path.absolute()}")

    # check whether to import specific HTML files or all HTML files in the event path
    file_processor: file_utils.ProcessHTML = file_utils.ProcessHTML()
    if concert_key_list:
        html_file_list = [
            event_path.joinpath(f"{concert_key}.html") for concert_key in concert_key_list
        ]
    else:
        html_file_list = sorted(event_path.glob("*.html"))
    logger.info(f"{year_str}\n\n" f"Requested HTML file count: {len(html_file_list)}")

    for idx, html_file in enumerate(html_file_list):
        # Input: {year}/events/some-key.html
        logger.info(f"[{year_str}][{idx + 1}] loading {html_file}...")
        concert_record: Optional[DBRecord] = None
        try:
            concert_record = _import_single_concert(file_processor, year, html_file)
        # we currently fail the entire process for if any file fails to load or has no content
        # this prevents importing an imcomplete set of files for a particular year into the DB
        except (OSError, ValidationError) as e:
            raise e
        if not concert_record:
            raise ValueError(f"[{year_str}] No concert data loaded from {html_file}")

        concert_record_list.append(concert_record)
    return concert_record_list


def _import_single_concert(
    file_processor: file_utils.ProcessHTML, year: int, concert_html: Path
) -> Optional[DBRecord]:
    """
    Import content from a single concert HTML file.

    :param file_processor: ProcessHTML object for help with loading HTML files
    :param year: Year
    :param concert_html: Path object representing a concert HTML file
    :return: DBRecord with single concert metadata and HTML content, None if no content
    """
    concert_record: Optional[DBRecord] = None
    html_content = None
    try:
        html_content = file_processor.load_html(str(concert_html))
    except OSError as e:
        raise e

    if html_content:
        concert_record = DBRecord(
            year=year,
            key=concert_html.name.split(".")[0],
            html_content=str(html_content.contents[-1]),
        )
    return concert_record


def _create_concert_detail_df(concert_record_list: List[DBRecord]) -> pd.DataFrame:
    """
    Return DataFrame of HTML file content for a list of SSO concerts.

    :param year: List with concert HTML file content DB records
    :return: Dataframe with concert HTML file content records
    """
    concert_df: pd.DataFrame = pd.DataFrame(
        [concert.dict() for concert in concert_record_list]
    ) if concert_record_list else pd.DataFrame()
    return concert_df


# %%
def _sqlite_db_file_name(input_years: List[int], db_file_prefix: str) -> str:
    """
    Return SSO SQLite DB file name, depending on inputs.

    :param input_years: List of years for which to import concert HTML file content
    :param db_file_prefix: SQLite DB file name prefix
    :return: SQLite DB file name
    """
    # first year for which to import concert files
    start_year: int = min(input_years)
    # last year for which to import concert files
    end_year: int = max(input_years)

    db_name: str = ""
    if (
        db_file_prefix == "sso"
        and start_year == common.EARLIEST_SSO_SEASON_YEAR
        and end_year == common.LATEST_SSO_SEASON_YEAR
    ):
        db_name = common.DEFAULT_SQLITE_DB
    else:
        db_file_suffix: str = (
            f"html_{end_year}" if start_year == end_year else f"html_{start_year}_{end_year}"
        )
        db_name = f"{db_file_prefix}_{db_file_suffix}.db"
    return db_name


def _validate_sqlite_db_filepath(db_file_name: str, append_flag: bool = False) -> None:
    """
    Validate SQLite DB file path, depending on the value of append_flag.

    :param db_file_name: SQLite DB file name
    :param append_flag: Specify whether or not new records are being appended to an existing DB
                        table. If append_flag=False, then a new DB file will be created.
    :return: None if path validation checks passed
    """
    # if append_flag=True, verify that the DB file path exists
    if append_flag:
        if not os.path.exists(db_file_name):
            raise OSError(f"{db_file_name} does not exist, cannot append records to anything")
    # if append_flag=False, raise an error if the DB file path exists
    else:
        if os.path.exists(db_file_name):
            raise OSError(f"{db_file_name} already exists, will not overwrite")
    return None


def _export_sqlite_html_db(
    db_name: str, concert_df: pd.DataFrame, append_flag: bool = False
) -> None:
    """
    Export Dataframe of SSO HTML file content to the specified SQLite DB.

    :param db_name: SQLite DB file name
    :param concert_df: Dataframe with concert HTML file content records
    :param append_flag: For an existing table, specify whether the SQLite engine should
                    append new rows or fail outright
    """
    try:
        sqlite_processor = file_utils.ProcessSQLite(db_name)
        sqlite_processor.export_html_to_sqlite_db(concert_df, append_flag)
    except (KeyError, OSError, ValueError, exc.SQLAlchemyError) as e:
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
            ["Load pre-fetched SSO concert HTML files", "and export them to a SQLite database"]
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
                "Specify the SSO season calendar year(s) for which to import concert HTML files.",
                "Example #1: '-y 2021 -y 2022'",
                "Example #2: '--year 2022'",
            ]
        ),
    )
    parser.add_argument(
        "-c",
        "--concert-key",
        type=str,
        metavar="CONCERT_KEY",
        dest="concert_keys",
        action="append",
        help="\n".join(
            [
                "(optional) Specify keys for individual concert(s) to import.",
                "Example #1: '-c some-concert-key -c another-key'",
                "Example #2: '--concert-key some-concert-key'",
            ]
        ),
    )
    parser.add_argument(
        "--db-prefix",
        type=str,
        default="sso",
        help="(optional) Specify custom SQLite DB file name prefix (default: 'sso'). Example: test",
    )
    parser.add_argument(
        "-a",
        "--append",
        default=False,
        action="store_true",
        dest="append_records",
        help=" ".join(
            [
                "(optional) Specify whether to append records to an existing SQLite DB.",
                "Default: False",
            ]
        ),
    )
    parser.add_argument(
        "--db-file-name",
        type=str,
        help="(required if append=True) Specify existing SQLite DB file name to append records to",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        default=False,
        action="store_true",
        dest="dry_run",
        help=" ".join(
            [
                "(optional) When set to True, concert record keys are returned,",
                "but no actual records are written to disk",
            ]
        ),
    )
    return parser


# %%
def main():
    """."""
    # get CLI args
    args: argparse.Namespace = _get_cli_args().parse_args()
    # check that input years are valid
    try:
        common.validate_sso_input_years(args.input_years)
    except ValueError as e:
        logger.error(e)
        raise e

    # validate that args.db_file_name exists if args.append_records=True
    if args.append_records and not args.db_file_name:
        raise ValueError("Must provide a SQLite DB file name when append=True")

    # set SQLite DB file name
    db_name: str = ""
    try:
        db_name = (
            args.db_file_name
            if args.append_records
            else _sqlite_db_file_name(args.input_years, args.db_prefix)
        )
        _validate_sqlite_db_filepath(db_name, args.append_records)
    except OSError as e:
        logger.error(f"There was an error while trying to set the DB name: {e}")
        raise e

    if not db_name:
        error_msg = "Invalid SQLite DB file name"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # import concert HTML file content
    master_concert_list: List[DBRecord] = []
    for year in args.input_years:
        concert_year_list: List[DBRecord] = []
        try:
            if args.concert_keys:
                concert_year_list = import_concerts_by_year(year, args.concert_keys)
            else:
                concert_year_list = import_concerts_by_year(year)
        except (OSError, ValidationError, ValueError) as e:
            logger.error(f"Error while trying to import concert HTML files: {e}")
            raise e
        if concert_year_list:
            master_concert_list.extend(concert_year_list)

    # extract concert keys and HTML content to a dataframe
    concert_df: pd.DataFrame = pd.DataFrame()
    try:
        concert_df = _create_concert_detail_df(master_concert_list)
    except (KeyError, ValueError) as e:
        logger.error(f"Error while creating the concert HTML dataframe: {e}")
        raise e

    if concert_df.empty:
        logger.warn(f"Concert HTML dataframe is empty. Not exporting anything")
        return None

    # if dry_run=True, just output keys for the concert records that would have been exported
    if args.dry_run:
        db_year: str = DBRecord.__fields__["year"].name
        db_key: str = DBRecord.__fields__["key"].name
        year_key_pairs: List[str] = concert_df[[db_year, db_key]].apply(
            lambda row: f"{row[db_year]}|{row[db_key]}", axis=1
        ).to_list()
        logger.info(
            f"[DRY RUN] Would have exported {len(year_key_pairs)} concert HTML records. "
            f"Concert year-key pairs:\n"
            f"{', '.join(year_key_pairs)}"
        )
    # if dry_run=False, export dataframe to designated SQLite DB
    else:
        try:
            _export_sqlite_html_db(db_name, concert_df, args.append_records)
        except (KeyError, OSError, ValueError, exc.SQLAlchemyError) as e:
            logger.error(
                "Error while trying to export the concert HTML content " f"to SQLite DB: {e}"
            )
            raise e
        logger.info(f"Successfully exported HTML content to {os.path.abspath(db_name)}")


# %%
if __name__ == "__main__":
    main()
