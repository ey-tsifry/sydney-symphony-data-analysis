#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Parse pre-fetched 2018-2021 SSO calendar files and output CSVs with concert URL 'keys'.

(keys = unique string identifiers that the SSO website assigns to each concert)

Note that string identifiers are apparently only unique within the same year, not across different years.

Prereq: Calendar HTML (2018-2020) and JSON (2021) files have been downloaded to local disk
Purpose: Allows programmatic retrieval of individual concert files from the SSO website based on concert key naming scheme
"""

# %%
from collections import namedtuple
from sso_utilities import file_utils
from typing import NamedTuple
import pandas as pd
import sys

# %%
class SSOCalendar:
    """Methods for extracting unique concert identifiers from SSO season calendar files."""

    def __init__(self, year: str, path_separator: str) -> None:
        """Methods for extracting unique concert identifiers from SSO season calendar files.

        :param year: 2018, 2019, 2020, or 2021 (in string format)
        :param path_separator: Forward or back-slash
        """
        self.year = year
        self.path_separator = path_separator
        self.Concert = namedtuple('Concert', 'year key')

    def parse_html_calendar(self, month: str) -> NamedTuple:
        """Load and parse SSO season calendar from local HTML file.

        Month is used to construct the file name and path: e.g. 2018/sso_2018_February.html

        :param month: Example: February
        """
        if self.year not in ['2018', '2019', '2020']:
            raise ValueError("Error: Year must be between 2018 and 2020")

        html_file = f"{self.year}{self.path_separator}sso_{self.year}_{month}.html"
        print(f"loading {html_file}...")

        # parse concerts into a list - one concert per row
        calendar_obj = file_utils.ProcessHTML()
        concerts = calendar_obj.load_html(html_file).find_all(class_='reveal calendar-perf-modal')
        # initialise Concert object with pre-filled year
        concert_keys = self.Concert(year=self.year, key=[])

        for row in concerts:
            key = row.find('a', attrs={'alt': 'Read More'})['href'].strip().split('/')[-1]
            if key not in concert_keys.key:
                concert_keys.key.append(key)
        return concert_keys
    
    def parse_json_calendar(self) -> NamedTuple:
        """Load and parse 2021 SSO season calendar from local JSON file.
        
        The 2021 SSO season calendar is available in JSON format, whereas previous season calendars were only available in HTML format.

        Example file: 2021/sso-concerts-2021.json
        """
        if self.year != '2021':
            raise ValueError("Error: Year must be 2021")

        json_file = f"{self.year}{self.path_separator}sso-concerts-{self.year}.json"
        print(f"loading {json_file}...")

        # parse concerts into a list - one concert per row
        calendar_obj = file_utils.ProcessJSON()
        concerts = calendar_obj.load_json(json_file)['data']
        # initialise Concert object with pre-filled year
        concert_keys = self.Concert(year=self.year, key=[])

        for row in concerts:
            key = row['url'].split('/')[-1].strip()
            if key not in concert_keys.key:
                concert_keys.key.append(key)
        return concert_keys

# %%
def main():
    # SSO seasons are from February-December
    months = ['February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    # check whether to use back- or forward-slash path separators, depending on platform (Windows or Unix-based)
    if sys.platform in ['cygwin', 'win32']:
        path_separator = '\\'
    else:
        path_separator = '/'

    # Loop through all the months/years and add concert keys to a list
    concert_list = []
    year_list = ['2018', '2019', '2020', '2021']
    for year in year_list:
        calendar = SSOCalendar(year=year, path_separator=path_separator)
        if year == '2021':
            concert_list.append(calendar.parse_json_calendar())
        else:
            concert_list.extend([ calendar.parse_html_calendar(month) for month in months ])

    # write all keys to disk, separately by year
    df = pd.DataFrame(concert_list).explode('key').drop_duplicates().dropna(how='any').reset_index(drop=True)
    for year in year_list:
        print(f"\n{year}:")
        out_file = f"{year}{path_separator}sso_{year}_keys.csv"
        out_df = df.loc[df['year'] == year, 'key']
        out_df.to_csv(path_or_buf=out_file, index=False, header=False)
        print(f"Successfully wrote to disk: {out_file} ({out_df.shape[0]} keys)")

# %%
if __name__ == '__main__':
    main()