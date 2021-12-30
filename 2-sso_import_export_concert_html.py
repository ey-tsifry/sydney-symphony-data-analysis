#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combine individual 2018-2022 SSO concert HTML file content into a DataFrame and export to a local
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
from typing import Dict, List

import pandas as pd
from sqlalchemy import exc

from data_models.sqlite_db import DBRecord
from sso_utilities import common, file_utils

# %%
logger = logging.getLogger(__name__)


# %%
def sso_import_concerts_by_year(year: int, concert_key_list: List[str] = []) -> List[DBRecord]:
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
        raise OSError(f"Events directory does not exist: {event_path.absolute()}")

    # check whether to import specific HTML files or all HTML files in the event path
    file_processor: file_utils.ProcessHTML = file_utils.ProcessHTML()
    if concert_key_list:
        html_file_list = [
            event_path.joinpath(f"{concert_key}.html") for concert_key in concert_key_list
        ]
    else:
        html_file_list = sorted(event_path.glob("*.html"))
    logger.info("\n\n".join([year_str, f"Requested HTML file count: {len(html_file_list)}"]))

    for idx, html_file in enumerate(html_file_list):
        # Input: {year}/events/some-key.html
        logger.info(f"[{idx + 1}] loading {html_file}...")
        try:
            concert_record: DBRecord = DBRecord(
                year,  # year
                html_file.name.split(".")[0],  # key
                str(file_processor.load_html(str(html_file)).contents[-1]),  # html_content
            )
        # we currently fail the entire process if any file fails to load
        # this prevents importing an imcomplete set of files into the DB
        except OSError as e:
            raise e
        else:
            concert_record_list.append(concert_record)
    return concert_record_list


def _sso_create_concert_detail_df(concert_record_list: List[DBRecord]) -> pd.DataFrame:
    """
    Return DataFrame of HTML file content for a list of SSO concerts.

    :param year: List with concert HTML file content records
    :return: Dataframe with concert HTML file content records
    """
    if not concert_record_list:
        raise ValueError("Concert record list is empty")
    concert_df: pd.DataFrame = pd.DataFrame(concert_record_list)
    return concert_df


# %%
def _sso_set_sqlite_db_filename(
    start_year: int, end_year: int, db_file_prefix: str, append_flag: bool = False
) -> str:
    """
    Return SSO SQLite DB filename, depending on inputs.

    :param start_year: First year for which to import concert HTML file content
    :param end_year: Last year for which to import concert HTML file content
    :param db_file_prefix: SQLite DB filename prefix
    :param append_flag: Specify whether or not new records are being appended to an existing DB
                        table. If append_flag=False, then a new DB file will be created.
    """
    db_name: str = ""
    if (
        db_file_prefix == "sso"
        and start_year == common.EARLIEST_SSO_SEASON_YEAR
        and end_year == common.LATEST_SSO_SEASON_YEAR
    ):
        db_name = common.DEFAULT_SQLITE_DB
    else:
        db_file_suffix: str = f"_html_{end_year}" if start_year == end_year else f"_html_{start_year}_{end_year}"
        db_name = f"{db_file_prefix}{db_file_suffix}.db"

    # if append_flag=True, verify that the DB file path exists
    if append_flag:
        if not os.path.exists(db_name):
            raise OSError(f"{db_name} does not exist, cannot append records to anything")
    # if append_flag=False, raise an error if the DB file path exists
    else:
        if os.path.exists(db_name):
            raise OSError(f"{db_name} already exists, will not overwrite")
    return db_name


def _sso_export_sqlite_html_db(
    db_name: str, concert_df: pd.DataFrame, append_flag: bool = False
) -> None:
    """
    Export Dataframe of SSO HTML file content to the specified SQLite DB.

    :param db_name: SQLite DB filename
    :param concert_df: Dataframe with concert HTML file content records
    :param append_flag: For an existing table, specify whether the SQLite engine should
                    append new rows or fail outright
    """
    if concert_df.empty:
        raise ValueError("No data to export")
    try:
        sqlite_processor = file_utils.ProcessSQLite(db_name)
        sqlite_processor.export_html_to_sqlite_db(concert_df, append_flag)
    except (KeyError, exc.SQLAlchemyError) as e:
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
        help="(optional) Specify custom SQLite DB filename prefix (default: 'sso'). Example: test",
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

    # set SQLite DB file name
    start_year: int = min(args.input_years)
    end_year: int = max(args.input_years)
    try:
        db_name: str = _sso_set_sqlite_db_filename(
            start_year, end_year, args.db_prefix, args.append_records
        )
    except OSError as e:
        logger.error(f"There was an error while trying to set the DB name: {e}")
        raise e

    if not db_name:
        error_msg = "Invalid SQLite DB filename"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # import concert HTML file content
    master_concert_list: List[DBRecord] = []
    for year in args.input_years:
        try:
            if args.concert_keys:
                concert_year_list = sso_import_concerts_by_year(year, args.concert_keys)
            else:
                concert_year_list = sso_import_concerts_by_year(year)
        except OSError as e:
            logger.error(f"There was an error while trying to import concert HTML files: {e}")
            raise e
        else:
            master_concert_list.extend(concert_year_list)

    # extract concert keys and HTML content to a dataframe
    try:
        concert_df: pd.DataFrame = _sso_create_concert_detail_df(master_concert_list)
    except (KeyError, ValueError) as e:
        logger.error(f"There was an error while creating the concert HTML dataframe: {e}")
        raise e

    # if dry_run=True, just output keys for the concert records that would have been exported
    if args.dry_run:
        db_field_map: Dict = DBRecord.fields()
        db_year: str = db_field_map["year"]
        db_key: str = db_field_map["key"]
        year_key_pairs = [f"{row[db_year]}|{row[db_key]}" for _, row in concert_df.iterrows()]
        logger.info(
            " ".join(
                [
                    f"[DRY RUN] Would have exported {len(year_key_pairs)} concert HTML records.",
                    "Concert year-key pairs:\n",
                    ", ".join(year_key_pairs),
                ]
            )
        )
    # if dry_run=False, export dataframe to designated SQLite DB
    else:
        try:
            _sso_export_sqlite_html_db(db_name, concert_df, args.append_records)
        except (KeyError, ValueError, exc.SQLAlchemyError) as e:
            logger.error(
                " ".join(
                    [
                        "There was an error while trying to export the concert HTML content",
                        f"to SQLite DB: {e}",
                    ]
                )
            )
            raise e
        else:
            logger.info(f"Successfully exported HTML content to {os.path.abspath(db_name)}")


# %%
if __name__ == "__main__":
    main()
