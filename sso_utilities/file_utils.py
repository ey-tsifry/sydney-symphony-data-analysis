#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility methods for processing different file types.
"""
import json
import os
import shutil
from functools import reduce
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, exc, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.types import Integer


class ProcessCSV:
    """Methods for processing CSV files."""

    def __init__(self, file_params: str = "r", from_encoding: str = "utf-8") -> None:
        """
        Methods for processing CSV files.

        :param file_params: File IO parameters, default: "r"
        :param from_encoding: Encoding, default: "utf-8"
        """
        self.file_params: str = file_params
        self.from_encoding: str = from_encoding

    def load_csv(self, filename: str) -> pd.DataFrame:
        """
        Load and process a CSV file.

        :param filename: CSV file
        :return: Dataframe with CSV file content
        """
        csv_df: pd.DataFrame = pd.DataFrame()
        with open(filename, self.file_params, encoding=self.from_encoding) as file:
            csv_df = pd.read_csv(file, encoding=self.from_encoding)
        return csv_df


class ProcessHTML:
    """Class with methods for processing HTML files."""

    def __init__(
        self, file_params: str = "rb", bsoup_params: str = "html5lib", from_encoding: str = "utf-8"
    ) -> None:
        """
        :param file_params: File IO parameters, default: "rb"
        :param bsoup_params: BeautifulSoup parameters, default: "html5lib"
        :param from_encoding: Encoding, default: "utf-8"
        """
        self.file_params: str = file_params
        self.bsoup_params: str = bsoup_params
        self.from_encoding: str = from_encoding

    def load_html(self, filename: str) -> Optional[BeautifulSoup]:
        """
        Load and process a local SSO HTML file.

        :param filename: HTML file
        :return: BeautifulSoup object with HTML file content, None if no content
        """
        html_soup: Optional[BeautifulSoup] = None
        with open(filename, self.file_params) as file:
            html_soup = BeautifulSoup(file, self.bsoup_params, from_encoding=self.from_encoding)
        return html_soup


class ProcessJSON:
    """Class with methods for processing JSON files."""

    def __init__(self, file_params: str = "r", file_encoding: str = "utf-8") -> None:
        """
        :param file_params: File IO parameters, default: "r"
        :param file_encoding: Encoding, default: "utf-8"
        """
        self.file_params: str = file_params
        self.encoding: str = file_encoding

    def load_json(self, filename: str) -> Dict[str, Any]:
        """
        Load and process a local SSO JSON file.

        :param filename: JSON file
        :return: Dictionary with JSON file content, empty if no content
        """
        json_content: Dict[str, Any] = {}
        with open(filename, self.file_params, encoding=self.encoding) as file:
            json_content = json.load(file)
        return json_content


class ProcessPickle:
    """Class with methods for processing Pickle files."""

    def __init__(self, file_params: str = "rb") -> None:
        """
        :param file_params: File IO parameters, default: "rb"
        """
        self.file_params: str = file_params

    def load_pickle(self, filename: str) -> pd.DataFrame:
        """
        Load and process a Pickle file.

        :param filename: Pickle file
        :return: Dataframe with Pickle file content, empty if no content
        """
        pickle_df: pd.DataFrame = pd.DataFrame()
        with open(filename, self.file_params) as file:
            pickle_df = pd.read_pickle(file)
        return pickle_df


class ProcessSQLite:
    """Class with methods for processing SQLite databases."""

    def __init__(self, db_name: str) -> None:
        """
        Database file will be created in local path if it doesn't yet exist.

        :param db_name: SQLite database file name
        """
        self.db_name: str = db_name
        self.engine: Engine = create_engine(f"sqlite:///{db_name}", echo=False)

    def export_html_to_sqlite_db(self, sqlite_df: pd.DataFrame, append_flag: bool = False) -> None:
        """
        Export consolidated BeautifulSoup HTML content to a SQLite DB.

        :param sqlite_df: DataFrame with BeautifulSoup HTML content
        :param append_flag: For an existing table, specify whether the SQLite engine should
                    append new rows or fail outright
        """
        if sqlite_df.empty:
            raise ValueError("HTML content dataframe is empty")
        if not any(column in sqlite_df.columns for column in ["html_content", "year"]):
            raise KeyError("HTML content dataframe is missing 'html_content' and/or 'year' columns")

        # convert HTML content from a BeautifulSoup object into a string since
        # SQLAlchemy doesn't accept BeautifulSoup types by default
        sqlite_df["html_content"] = sqlite_df["html_content"].astype(str)
        # extract list of year(s)
        year_list: List[str] = sorted(set(sqlite_df["year"]))
        try:
            # create backup copy of any existing DB file
            if os.path.exists(self.db_name):
                shutil.copy2(self.db_name, f"{self.db_name.split('.')[0]}.orig.db")
            # create one table for each year
            # if append_flag=True, records will be appended to any existing tables
            for year in year_list:
                sqlite_df.loc[sqlite_df["year"] == year].to_sql(
                    str(year),
                    con=self.engine,
                    dtype={"year": Integer()},
                    index=False,
                    if_exists="append" if append_flag else "fail",
                )
        except (KeyError, OSError, ValueError, exc.SQLAlchemyError) as e:
            raise e
        return None

    def _sqlite_table_names(self) -> List[str]:
        """
        Return list of SQLite DB table names.

        :return: List of table names
        """
        inspector = inspect(self.engine)
        table_names: List[str] = inspector.get_table_names()
        return table_names

    def load_sqlite_db(self) -> pd.DataFrame:
        """
        Load and process all exported SQLite DB tables.

        Converts HTML content from a string back into a BeautifulSoup object.

        :return: Dataframe with all SQLite DB data, empty if no results
        """
        master_df_list: List[pd.DataFrame] = []
        reduced_df: pd.DataFrame = pd.DataFrame()
        try:
            for table_name in self._sqlite_table_names():
                sql_df: pd.DataFrame = pd.read_sql_table(table_name, con=self.engine)
                sql_df["html_content"] = sql_df["html_content"].apply(
                    lambda html_string: BeautifulSoup(html_string, "html5lib")
                )
                master_df_list.append(sql_df)
        except (KeyError, OSError, ValueError) as e:
            raise e
        if master_df_list:
            reduced_df = reduce(lambda df1, df2: pd.concat([df1, df2]), master_df_list)
        return reduced_df

    def load_sqlite_db_by_year(self, year: int) -> pd.DataFrame:
        """
        Load and process exported SQLite DB table, filtered by year.

        Converts HTML content from a string back into a BeautifulSoup object.

        :param year: Year on which to filter
        :return: Dataframe with SQLite DB query results for the specified year, empty if no results
        """
        sql_df: pd.DataFrame = pd.DataFrame()
        try:
            sql_df = pd.read_sql_table(str(year), con=self.engine)
            sql_df["html_content"] = sql_df["html_content"].apply(
                lambda html_string: BeautifulSoup(html_string, "html5lib")
            )
        except (KeyError, OSError, ValueError) as e:
            raise e
        return sql_df


class ProcessWikiXML:
    """Class with methods for processing Wikipedia XML files."""

    def __init__(
        self, file_params: str = "rb", bsoup_params: str = "lxml", from_encoding: str = "utf-8"
    ) -> None:
        """
        :param file_params: File IO parameters, defaults to "rb"
        :param bsoup_params: BeautifulSoup parameters, defaults to "lxml"
        :param from_encoding: Encoding, defaults to "utf-8"
        """
        self.file_params: str = file_params
        self.bsoup_params: str = bsoup_params
        self.from_encoding: str = from_encoding

    def load_xml(self, filename: str) -> Optional[BeautifulSoup]:
        """
        Load and process a local Wikipedia XML file.

        :param filename: XML file
        :return: BeautifulSoup object with XML file content, None if no content
        """
        xml_soup: Optional[BeautifulSoup] = None
        with open(filename, self.file_params) as file:
            xml_soup = BeautifulSoup(file, self.bsoup_params, from_encoding=self.from_encoding)
        return xml_soup
