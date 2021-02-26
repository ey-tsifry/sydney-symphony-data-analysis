#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Combine individual 2018-2021 SSO concert HTML file content into a DataFrame and export to a local SQLite database.

Prereq: HTML files have been downloaded to local disk
Purpose: Aggregate all concert information into an external database for more convenient future processing
"""

# %%
from collections import namedtuple
from pathlib import Path
from sso_utilities import file_utils
import pandas as pd
import sys

# %%
## import 2018-2021 concert HTML file content
def sso_import_concerts(start_year: int = 2018, end_year: int = 2021, path_separator: str = '/') -> pd.DataFrame:
    """Return DataFrame of HTML file content for a specified time period.

    :param start_year: Start year
    :param end_year: End year
    :param path_separator: Forward or back-slash
    """
    FileRecord = namedtuple('FileRecord', 'year key html_content')
    file_list = []
    
    # load existing HTML files, one by one, from each {year}/events directory
    for year in range(start_year, end_year + 1):
        # import existing HTML content from valid paths only
        path = Path(f"{str(year)}{path_separator}events")
        if path.exists():
            for html_file in sorted(path.glob('*.html')):
                # Input: {year}/events/some-key.html
                print(f"loading {html_file}...")
                concert_obj = file_utils.ProcessHTML()
                file_record = FileRecord(year=year, key=html_file.name.split('.')[0], html_content=concert_obj.load_html(str(html_file)).contents[-1])
                file_list.append(file_record)
        else:
            raise OSError(f"Error: Path \'{path}\' does not exist")
    
    if file_list:
        return pd.DataFrame(file_list)
    else:
        return None

# %%
def main():
    # check whether to use back- or forward-slash path separators, depending on platform (Windows or Unix-based)
    if sys.platform in ['cygwin', 'win32']:
        path_separator = '\\'
    else:
        path_separator = '/'

    # time period
    START_YEAR = 2018
    END_YEAR = 2021

    # SQLite DB file name
    DB_NAME = f"sso_html_{START_YEAR}_{END_YEAR}.db"
    
    # create DataFrame of imported concert HTML files
    if not Path(DB_NAME).exists():
        df = sso_import_concerts(START_YEAR, END_YEAR, path_separator)
        # export DataFrame to designated SQLite DB
        if type(df) == pd.pandas.core.frame.DataFrame and not df.empty:
            try:
                sqlite_obj = file_utils.ProcessSQLite(DB_NAME)
                sqlite_obj.export_html_to_sqlite_db(df)
                print(f"Successfully exported HTML content to {DB_NAME}")
            except:
                raise
        else:
            raise ValueError("Error: No data imported")
    else:
        raise OSError(f"Error: {DB_NAME} already exists")

# %%
if __name__ == '__main__':
    main()