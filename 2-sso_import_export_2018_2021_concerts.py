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
import os
from collections import namedtuple
from pathlib import Path
from typing import List, NamedTuple

import pandas as pd
from sqlalchemy import exc

from sso_utilities import file_utils


# %%
def sso_import_concerts_by_year(year: int) -> List[NamedTuple]:
    """
    Return list of concert HTML file content for a specified year.

    :param year: Year
    :return: List with concert HTML file content records
    """
    concert_record_list: List[NamedTuple] = []
    db_record = namedtuple("db_record", "year key html_content")  # type: ignore

    # import existing HTML content from valid paths only
    year_str = str(year)
    path = Path(os.path.join(year_str, "events"))
    if not path.exists():
        raise OSError(f"Path does not exist: {path.absolute()}")

    concert_obj = file_utils.ProcessHTML()
    html_file_list = sorted(path.glob("*.html"))
    print(f"\n{year_str}\n\nHTML file count: {len(html_file_list)}\n")
    for idx, html_file in enumerate(html_file_list):
        # Input: {year}/events/some-key.html
        print(f"[{idx+1}] loading {html_file}...")
        try:
            concert_record = db_record(
                year=year,
                key=html_file.name.split(".")[0],
                html_content=concert_obj.load_html(str(html_file)).contents[-1],
            )
        # we currently fail the entire process if any file fails to load
        except OSError:
            raise
        else:
            concert_record_list.append(concert_record)
    return concert_record_list


def sso_create_concert_detail_df(concert_record_list: List[NamedTuple]) -> pd.DataFrame:
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
def sso_set_sqlite_db_filename(
    start_year: int, end_year: int, default_db_file: str, append_flag: bool = False
) -> str:
    """
    Return SSO SQLite DB filename, depending on inputs.

    :param start_year: First year for which to import concert HTML file content
    :param end_year: Last year for which to import concert HTML file content
    :param default_db_file: Default SQLite DB file name (file may or may not actually exist)
    :param append_flag: Specify whether or not new records are being appended to an existing DB
                        table. If append_flag=False, then a new DB file will be created.
    """
    db_name: str = ""
    # if append_flag=True, then use the default DB filename
    if append_flag:
        db_name = default_db_file
        if not os.path.exists(db_name):
            raise OSError(f"{db_name} does not exist, cannot append records to anything")
    # else set a new DB filename
    else:
        db_name = (
            f"sso_html_{end_year}.db"
            if start_year == end_year
            else f"sso_html_{start_year}_{end_year}.db"
        )
        if os.path.exists(db_name):
            raise OSError(f"{db_name} already exists")
    return db_name


def sso_export_sqlite_html_db(
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
        sqlite_obj = file_utils.ProcessSQLite(db_name)
        sqlite_obj.export_html_to_sqlite_db(concert_df, append_flag)
    except (KeyError, exc.SQLAlchemyError):
        raise
    else:
        return


# %%
def main():
    """."""
    # define a time period between 2018 and 2022
    YEAR_LIST: List[int] = [2022]
    # specify whether to append records to an existing DB, or create a new DB
    APPEND_RECORDS = True
    # name of default SSO SQLite DB file (may or may not already exist)
    DEFAULT_SQLITE_DB = "sso_html_2018_2022.db"

    # validate start and end years - currently they must be between 2018-2022
    START_YEAR, END_YEAR = (min(YEAR_LIST), max(YEAR_LIST))
    if START_YEAR < 2018 or END_YEAR > 2022:
        raise ValueError("Start and end years must be between 2018 and 2022")

    # set SQLite DB file name
    DB_NAME = sso_set_sqlite_db_filename(START_YEAR, END_YEAR, DEFAULT_SQLITE_DB, APPEND_RECORDS)
    if not DB_NAME:
        raise ValueError("Invalid SQLite DB filename")

    # create DataFrame of imported concert HTML files
    master_concert_list: List[NamedTuple] = []
    for year in YEAR_LIST:
        try:
            concert_year_list = sso_import_concerts_by_year(year)
        except OSError:
            raise
        else:
            master_concert_list.extend(concert_year_list)

    # export DataFrame to designated SQLite DB
    try:
        concert_df = sso_create_concert_detail_df(master_concert_list)
        sso_export_sqlite_html_db(DB_NAME, concert_df, APPEND_RECORDS)
    except (KeyError, ValueError, exc.SQLAlchemyError):
        raise
    else:
        print(f"\nSuccessfully exported HTML content to {os.path.abspath(DB_NAME)}")


# %%
if __name__ == "__main__":
    main()
