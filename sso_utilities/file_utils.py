#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import shutil
from functools import reduce
from typing import Dict, List

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

        :param file_params: File IO parameters, defaults to "r"
        :param from_encoding: Encoding, defaults to "utf-8"
        """
        self.file_params: str = file_params
        self.from_encoding: str = from_encoding

    def load_csv(self, filename: str) -> pd.DataFrame:
        """
        Load and process a CSV file.

        :param filename: CSV file
        :return: Dataframe with CSV file content
        """
        try:
            with open(filename, self.file_params, encoding=self.from_encoding) as file:
                csv_df: pd.DataFrame = pd.read_csv(file, encoding=self.from_encoding)
        except OSError as e:
            raise e
        else:
            return csv_df


class ProcessHTML:
    """Methods for processing HTML files."""

    def __init__(
        self, file_params: str = "rb", bsoup_params: str = "html5lib", from_encoding: str = "utf-8"
    ) -> None:
        """
        Methods for processing HTML files.

        :param file_params: File IO parameters, defaults to "rb"
        :param bsoup_params: BeautifulSoup parameters, defaults to "html5lib"
        :param from_encoding: Encoding, defaults to "utf-8"
        """
        self.file_params: str = file_params
        self.bsoup_params: str = bsoup_params
        self.from_encoding: str = from_encoding

    def load_html(self, filename: str) -> BeautifulSoup:
        """
        Load and process a local SSO HTML file.

        :param filename: HTML file
        :return: BeautifulSoup object with HTML file content
        """
        try:
            with open(filename, self.file_params) as file:
                html_soup: BeautifulSoup = BeautifulSoup(
                    file, self.bsoup_params, from_encoding=self.from_encoding
                )
        except OSError as e:
            raise e
        else:
            return html_soup


class ProcessJSON:
    """Methods for processing JSON files."""

    def __init__(self, file_params: str = "r", file_encoding: str = "utf-8") -> None:
        """Methods for processing JSON files.

        :param file_params: File IO parameters, defaults to "r"
        :param file_encoding: Encoding, defaults to "utf-8"
        """
        self.file_params: str = file_params
        self.encoding: str = file_encoding

    def load_json(self, filename: str) -> Dict:
        """
        Load and process a local SSO JSON file.

        :param filename: JSON file
        :return: Dictionary with JSON file content
        """
        try:
            with open(filename, self.file_params, encoding=self.encoding) as file:
                json_content: Dict = json.load(file)
        except OSError as e:
            raise e
        else:
            return json_content


class ProcessPickle:
    """Methods for processing Pickle files."""

    def __init__(self, file_params: str = "rb") -> None:
        """Methods for processing Pickle files.

        :param file_params: File IO parameters, defaults to "rb"
        """
        self.file_params: str = file_params

    def load_pickle(self, filename: str) -> pd.DataFrame:
        """
        Load and process a Pickle file.

        :param filename: Pickle file
        :return: Dataframe with Pickle file content
        """
        try:
            with open(filename, self.file_params) as file:
                pickle_df: pd.DataFrame = pd.read_pickle(file)
        except OSError as e:
            raise e
        else:
            return pickle_df


class ProcessSQLite:
    """Methods for processing SQLite databases."""

    def __init__(self, db_name: str) -> None:
        """
        Methods for processing SQLite databases.

        Database file will be created in local path if it doesn"t yet exist.

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
        else:
            return

    def _get_sqlite_table_names(self) -> List[str]:
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

        :return: Dataframe with all SQLite DB data
        """
        master_df_list: List[pd.DataFrame] = []
        try:
            for table_name in self._get_sqlite_table_names():
                sql_df: pd.DataFrame = pd.read_sql_table(table_name, con=self.engine)
                sql_df["html_content"] = sql_df["html_content"].apply(
                    lambda html_string: BeautifulSoup(html_string, "html5lib")
                )
                master_df_list.append(sql_df)
        except (KeyError, OSError, ValueError) as e:
            raise e
        else:
            return reduce(lambda df1, df2: pd.concat([df1, df2]), master_df_list)

    def load_sqlite_db_by_year(self, year: int) -> pd.DataFrame:
        """
        Load and process exported SQLite DB table, filtered by year.

        Converts HTML content from a string back into a BeautifulSoup object.

        :param year: Year on which to filter
        :return: Dataframe with SQLite DB query results for the specified year
        """
        try:
            sql_df: pd.DataFrame = pd.read_sql_table(str(year), con=self.engine)
            sql_df["html_content"] = sql_df["html_content"].apply(
                lambda html_string: BeautifulSoup(html_string, "html5lib")
            )
        except (KeyError, OSError, ValueError) as e:
            raise e
        else:
            return sql_df


class ProcessWikiXML:
    """Methods for processing Wikipedia XML files."""

    def __init__(
        self, file_params: str = "rb", bsoup_params: str = "lxml", from_encoding: str = "utf-8"
    ) -> None:
        """Methods for processing Wikipedia XML files.

        :param file_params: File IO parameters, defaults to "rb"
        :param bsoup_params: BeautifulSoup parameters, defaults to "lxml"
        :param from_encoding: Encoding, defaults to "utf-8"
        """
        self.file_params: str = file_params
        self.bsoup_params: str = bsoup_params
        self.from_encoding: str = from_encoding

    def load_xml(self, filename: str) -> BeautifulSoup:
        """
        Load and process a local Wikipedia XML file.

        :param filename: XML file
        :return: BeautifulSoup object with XML file content
        """
        try:
            with open(filename, self.file_params) as file:
                xml_soup: BeautifulSoup = BeautifulSoup(
                    file, self.bsoup_params, from_encoding=self.from_encoding
                )
        except OSError as e:
            raise e
        else:
            return xml_soup
