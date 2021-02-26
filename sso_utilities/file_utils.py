from bs4 import BeautifulSoup
from sqlalchemy import create_engine, exc
from sqlalchemy.types import Integer
import json
import pandas as pd
import sys

class ProcessCSV:
    """Functions for processing CSV files."""

    def __init__(self, file_params: str = 'r', from_encoding: str = 'utf-8') -> None:
        """Functions for processing CSV files.
        
        :param file_params: File IO parametres, defaults to 'r'
        :param from_encoding: Encoding, defaults to 'utf-8'
        """
        self.file_params = file_params
        self.from_encoding = from_encoding
    
    def load_csv(self, filename: str) -> pd.DataFrame:
        """Load and process a CSV file.

        :param filename: CSV file
        """
        try:
            with open(filename, self.file_params, encoding=self.from_encoding) as file:
                return pd.read_csv(file, encoding=self.from_encoding)
        except OSError as e:
            print(e)
            sys.exit(1)

class ProcessHTML:
    """Functions for processing HTML files."""

    def __init__(self, file_params: str = 'rb', bsoup_params: str = 'html5lib', from_encoding: str = 'utf-8') -> None:
        """Functions for processing HTML files.
        
        :param file_params: File IO parametres, defaults to 'rb'
        :param bsoup_params: BeautifulSoup parametres, defaults to 'html5lib'
        :param from_encoding: Encoding, defaults to 'utf-8'
        """
        self.file_params = file_params
        self.bsoup_params = bsoup_params
        self.from_encoding = from_encoding

    def load_html(self, filename: str) -> BeautifulSoup:
        """Load and process a local SSO HTML file.

        :param filename: HTML file
        """
        try:
            with open(filename, self.file_params) as file:
                return BeautifulSoup(file, self.bsoup_params, from_encoding=self.from_encoding)
        except OSError as e:
            print(f"Failed to load {filename}\nError: {e}")
            sys.exit(1)

class ProcessJSON:
    """Functions for processing JSON files."""

    def __init__(self, file_params: str = 'r', file_encoding: str = 'utf-8') -> None:
        """Functions for processing JSON files.

        :param file_params: File IO parametres, defaults to 'r'
        :param file_encoding: Encoding, defaults to 'utf-8'
        """
        self.file_params = file_params
        self.encoding = file_encoding

    def load_json(self, filename: str) -> dict:
        """Load and process a local SSO JSON file.

        :param filename: JSON file
        """
        try:
            with open(filename, self.file_params, encoding=self.encoding) as file:
                return json.load(file)
        except OSError as e:
            print(f"Failed to load {filename}\nError: {e}")
            sys.exit(1)

class ProcessPickle:
    """Functions for processing Pickle files."""

    def __init__(self, file_params: str = 'rb') -> None:
        """Functions for processing Pickle files.

        :param file_params: File IO parametres, defaults to 'rb'
        """
        self.file_params = file_params
    
    def load_pickle(self, filename: str) -> pd.DataFrame:
        """Load and process a Pickle file.
        
        :param filename: Pickle file
        """
        try:
            with open(filename, self.file_params) as file:
                return pd.read_pickle(file)
        except OSError as e:
            print(f"Failed to load {filename}\nError: {e}")
            sys.exit(1)

class ProcessSQLite:
    """Functions for processing SQLite databases."""

    def __init__(self, db_name: str) -> None:
        """Functions for processing SQLite databases.

        Database file will be created in local path if it doesn't yet exist.

        :param db_name: SQLite database file name
        """
        self.db_name = db_name
        self.engine = create_engine(f"sqlite:///{db_name}", echo=False)
    
    def export_html_to_sqlite_db(self, df: pd.DataFrame) -> None:
        """Export consolidated BeautifulSoup HTML content to a SQLite DB.

        Converts HTML content from a BeautifulSoup object into a string since SQLAlchemy doesn't accept BeautifulSoup types by default.
        
        :param df: DataFrame with BeautifulSoup HTML content
        """
        df['html_content'] = df['html_content'].astype(str)
        try:
            df.to_sql(self.db_name.split('.')[0], con=self.engine, dtype={'year': Integer()}, index=False, if_exists='fail')
            return None
        except exc.SQLAlchemyError as e:
            print(f"Failed to export {self.db_name}\nError: {e}")
            sys.exit(1)

    def load_sqlite_db(self) -> pd.DataFrame:
        """Load and process exported SQLite DB table.
        
        Converts HTML content from a string back into a BeautifulSoup object.
        """
        try:
            sql_df = pd.read_sql_table(self.db_name.split('.')[0], con=self.engine)
            sql_df['html_content'] = sql_df['html_content'].apply(lambda html_string: BeautifulSoup(html_string))
            return sql_df
        except OSError as e:
            print(f"Failed to load {self.db_name}\nError: {e}")
            sys.exit(1)
    
    def load_sqlite_db_by_year(self, year: int) -> pd.DataFrame:
        """Load and process exported SQLite DB table, filtered by year.

        :param year: Year on which to filter
        """
        try:
            sql_query = f"SELECT * FROM {self.db_name.split('.')[0]} where year={year}"
            sql_df = pd.read_sql(sql_query, con=self.engine)
            sql_df['html_content'] = sql_df['html_content'].apply(lambda html_string: BeautifulSoup(html_string))
            return sql_df
        except OSError as e:
            print(f"Failed to load {self.db_name}\nError: {e}")
            sys.exit(1)

class ProcessWikiXML:
    """Functions for processing Wikipedia XML files."""

    def __init__(self, file_params: str = 'rb', bsoup_params: str = 'lxml', from_encoding: str = 'utf-8') -> None:
        """Functions for processing Wikipedia XML files.

        :param file_params: File IO parametres, defaults to 'rb'
        :param bsoup_params: BeautifulSoup parametres, defaults to 'lxml'
        :param from_encoding: Encoding, defaults to 'utf-8'
        """
        self.file_params = file_params
        self.bsoup_params = bsoup_params
        self.from_encoding = from_encoding

    def load_xml(self, filename: str) -> BeautifulSoup:
        """Load and process a local Wikipedia XML file.
        
        :param filename: XML file
        """
        try:
            with open(filename, self.file_params) as file:
                return BeautifulSoup(file, self.bsoup_params, from_encoding=self.from_encoding)
        except OSError as e:
            print(f"Failed to load {filename}\nError: {e}")
            sys.exit(1)