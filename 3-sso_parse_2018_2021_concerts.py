#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Parse and combine specific 2018-2021 SSO concert details (dates, titles, conductors, composers, artists, etc) into a DataFrame.

Prereq: SQLite DB of aggregated HTML content
"""

# %%
from bs4 import BeautifulSoup, element
from collections import namedtuple
from datetime import datetime
from pydantic import BaseModel, conint, constr, root_validator, validator
from sso_utilities import file_utils
from typing import Any, List, NamedTuple, Optional, Tuple
import html
import pandas as pd
import re
import sys

# %%
class ArtistConductor(BaseModel):
    """Class to represent Conductors and Artists."""

    class Config:
        arbitrary_types_allowed=True
    
    CONDUCTOR_KEYWORDS = ['conductor', 'condcutor', 'musical director', 'artistic director']
    ENSEMBLE_KEYWORDS = ['choir', 'choirs', 'orchestra', 'symphony']
    DEFAULT_CONDUCTOR = 'Unknown'
    DEFAULT_ARTIST = ('Artist', 'Unknown')

    artist_data: List[Tuple[str, str]]
    conductor: Optional[str] = DEFAULT_CONDUCTOR
    artists: Optional[List[str]] = []

    @validator('artist_data', each_item=True)
    def check_artist_data_type(cls, v):
        assert isinstance(v, str), f"Artist data is not in List[Tuple[str, str]] format"
        return v.strip()
    
    @property
    def namedtuple(self) -> NamedTuple:
        """Return NamedTuple with conductor and artist_metadata."""
        ArtistConductorNT = namedtuple('ArtistConductorNT', 'Conductor Artist_Metadata')
        return ArtistConductorNT(Conductor=self.conductor, Artist_Metadata=self.artists)
    
    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
        # populate with default values if artist_data is empty
        if not values['artist_data']:
            self.artists.append(self.DEFAULT_ARTIST)
        else:
            # extract conductors and artists from artist_data
            for artist_tuple in values['artist_data']:
                if self.conductor == self.DEFAULT_CONDUCTOR:
                    self._add_conductor(artist_tuple)
                if self.artists != [self.DEFAULT_ARTIST]:
                    self._add_artists(artist_tuple)
            # if artists list is still empty (i.e. there were no artists), populate with DEFAULT_ARTIST
            if not self.artists:
                self.artists.append(self.DEFAULT_ARTIST)
    
    def _add_artists(self, artist_tuple: Tuple[str, str]) -> None:
        """Extract artists to artist_list."""
        artist_split = artist_tuple[0].split(',')
        # case: artists who are also conducting
        if artist_tuple[0].lower().endswith('-director') or artist_tuple[0].lower().startswith('director and'):
            self.artists.append((artist_tuple[0].title(), artist_tuple[1]))
        # case: artists who are also conducting, but different format = ('conductor, instrument, ...', 'John Doe')
        elif (len(artist_split) > 1) and (artist_split[0].strip().lower() == 'conductor'):
            for instr in artist_split[1:]:
                self.artists.append((instr.strip().title(), artist_tuple[1]))   
        # case: artists that are actually ensembles rather than individuals
        elif artist_tuple[1].lower().endswith(tuple(self.ENSEMBLE_KEYWORDS)):
            self.artists.append(('Artist', artist_tuple[1]))
            """ edge case: ensemble artists are usually paired with a conductor
            Example: ('Nicholas Carter, conductor', 'Sydney Symphony Orchestra')
            In these cases, if there are multiple conductor/ensemble pairs, we add the first 
            conductor via _add_conductor (see below), but we add any subsequent conductors as 'artists' 
            """
            if (self.conductor != self.DEFAULT_CONDUCTOR) and (len(artist_tuple[0].split(',')) > 1):
                self.artists.append((artist_split[1].strip().title(), artist_split[0].strip()))
        # only process remaining tuples if they do not contain conductor keywords
        else:
            if not any(keyword in artist_tuple[0].lower() for keyword in self.CONDUCTOR_KEYWORDS) and not any(keyword in artist_tuple[1].lower() for keyword in self.CONDUCTOR_KEYWORDS):
                # exclude edge cases like 'Evanescence is:', which is not a valid artist
                if not artist_tuple[0]:
                    if not artist_tuple[1].endswith(':'):
                        self.artists.append(('Artist', artist_tuple[1]))
                elif not artist_tuple[1]:
                    self.artists.append(('Artist', artist_tuple[0]))
                else:
                    # replace ('John Doe', 'Film Score') with ('Artist', 'John Doe')
                    if any(keyword in artist_tuple[1].lower() for keyword in ['film credit', 'film score']):
                        self.artists.append(('Artist', artist_tuple[0]))
                    # replace ('John Doe', 'random_label') with ('Random_label', 'John Doe')
                    elif any(keyword in artist_tuple[1].lower() for keyword in ['concertmaster', 'narrator', 'soprano']):
                        self.artists.append((artist_tuple[1].title(), artist_tuple[0]))
                    else:
                        self.artists.append((artist_tuple[0].title(), artist_tuple[1]))

    def _add_conductor(self, artist_tuple: Tuple[str, str]) -> None:
        """Extract and assign conductor."""
        # case: assume artist is a conductor if any conductor keywords match the first tuple element
        if any(keyword in artist_tuple[0].lower() for keyword in self.CONDUCTOR_KEYWORDS):
            artist_split = artist_tuple[0].split(',')
            # parse ('John Doe, conductor', 'Some Ensemble') instead of ('conductor', 'John Doe')
            if len(artist_split) > 1:
                # edge case: ('conductor, instrument, ...', 'John Doe')
                if artist_split[1].strip() == 'conductor':
                    self.conductor = artist_split[0].strip()
                else:
                    self.conductor = artist_tuple[1]
            # if string can't be split, assume we're dealing with normal format ('conductor', 'John Doe')
            else:
                self.conductor = artist_tuple[1]
        # case: name/conductor format is reversed to conductor/name
        elif any(keyword in artist_tuple[1].lower() for keyword in self.CONDUCTOR_KEYWORDS):
            self.conductor = artist_tuple[0]
        # case: artists who are also conducting
        elif any(keyword in artist_tuple[0].lower() for keyword in ['-director', 'director and']):
            self.conductor = artist_tuple[1]


# %%
class Repertoire(BaseModel):
    """Class to represent Composers and Pieces."""
    
    class Config:
        arbitrary_types_allowed=True

    # regex for cleaning up composer and piece strings
    COMPOSER_REGEX_PAT = re.compile(r'([\^\*\"\'])')
    PIECE_REGEX_PAT = re.compile(r'([\^\*])')
    # default composer and piece values
    DEFAULT_COMPOSER = 'Unknown'
    DEFAULT_PIECE = 'Various'

    repertoire_data: List[Tuple[str, str]]
    composers: Optional[List[str]] = []
    pieces: Optional[List[str]] = []

    @validator('repertoire_data', each_item=True)
    def check_repertoire_data_type(cls, v):
        assert isinstance(v, str), f"Repertoire data is not in List[Tuple[str, str]] format"
        return v.strip()

    @property
    def namedtuple(self) -> NamedTuple:
        """Return NamedTuple with repertoire data."""
        RepertoireNT = namedtuple('RepertoireNT', 'Piece Composer')
        return RepertoireNT(Piece=self.pieces, Composer=self.composers)

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
        # populate with default values if repertoire_data is empty
        if not values['repertoire_data']:
            values['repertoire_data'] = [(self.DEFAULT_PIECE, self.DEFAULT_COMPOSER)]
        # create separate composer and piece lists from repertoire_data
        pieces, composers = map(list, zip(*values['repertoire_data']))
        self._add_composers(composers)
        self._add_pieces(pieces, composers)
        # if composer_list is still empty (i.e. there were no named composers), populate with DEFAULT_COMPOSER
        if not self.composers:
            self.composers.append(self.DEFAULT_COMPOSER)
        # if piece_list is still empty (i.e. there were no named pieces), populate with DEFAULT_PIECE
        if not self.pieces:
            self.pieces.append(self.DEFAULT_PIECE)
    
    def _add_composers(self, composer_list: List[str]) -> None:
        """Extract composers and add to composer_list."""
        for composer in composer_list:
            if not composer:
                self.composers.append(self.DEFAULT_COMPOSER)
            else:
                self.composers.append(re.sub(self.COMPOSER_REGEX_PAT, r'', composer.title()))
    
    def _add_pieces(self, piece_list: List[str], composer_list: List[str]) -> None:
        """Extract pieces and add to piece_list."""
        # if piece is empty (i.e. no specific piece name was provided) but composer is populated, 
        # assign piece = 'Various'
        for idx, piece in enumerate(piece_list):
            if not piece:
                if composer_list[idx]:
                    self.pieces.append(self.DEFAULT_PIECE)
            else:
                self.pieces.append(re.sub(self.PIECE_REGEX_PAT, r'', piece))


# %%
class ConcertBase(BaseModel):
    """Class to represent base SSO concert record."""
    
    class Config:
        arbitrary_types_allowed=True
    
    # modified date string format with year
    DATE_MOD_FORMAT = '%a %d %b %Y %I:%M %p'
    # date output format
    DATE_OUTPUT_FORMAT = '%Y-%m-%d %H:%M'

    title: constr(strip_whitespace=True)
    key: constr(strip_whitespace=True)
    date: constr(strip_whitespace=True)
    year: conint(ge=2018, le=2021)

    @root_validator
    def check_date_format(cls, values):
        DATE_INPUT_FORMAT = '%a %d %b, %I:%M %p'
        if values['year'] == 2020:
            # edge case: hacky workaround for '29 February 2020' since datetime 
            # doesn't play nice with leap year dates when year is not part of the input string
            assert datetime.strptime(' '.join([str(values['year']), values['date']]), f"%Y {DATE_INPUT_FORMAT}"), "Date is not in expected format: Sat 29 Feb, 2:00 pm"
        else:
            assert datetime.strptime(values['date'], DATE_INPUT_FORMAT), "Date is not in expected format: Fri 27 Mar, 6:00 pm"
        return values
    
    @validator('title')
    def check_title_format(cls, v):
        CONCERT_REGEX_PAT = re.compile(r'^(\b\w.+\b){3} [|] \w.+\Z')
        assert re.match(CONCERT_REGEX_PAT, v), "Concert title is not in expected format: Sydney Symphony Orchestra | Some Concert"
        return v
    
    @property
    def namedtuple(self) -> NamedTuple:
        """Return NamedTuple with base concert data."""
        ConcertNT = namedtuple('ConcertNT', 'Concert Key Date')
        return ConcertNT(Concert=self.title, Key=self.key, Date=self.date)

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
        self._format_title(values['title'])
        self._format_date(values['date'], values['year'])

    def _format_title(self, title: str) -> None:
        """Extract and format concert title (stripped of extra spaces).

        :param concert: Expected input: 'Sydney Symphony Orchestra | Some Title'
        """
        self.title = re.sub(r'\s+', ' ', title.split('|')[-1].strip())

    def _format_date(self, date: str, year: int) -> None:
        """Reformat concert date. Hour is included as there can be more than one performance of a specific concert on a single day (e.g. 2pm and 8pm).

        :param year: Expected years: 2018, 2019, 2020
        :param date: Expected input format: 'Fri 27 Mar, 6:00 pm'
        :returns: Expected output: '[YEAR]-03-27 18:00'
        """
        date_split = date.split(',')
        self.date = datetime.strptime(f"{date_split[0].strip()} {year} {date_split[1].strip()}", self.DATE_MOD_FORMAT).strftime(self.DATE_OUTPUT_FORMAT)

class ConcertFull(ConcertBase):
    """Class to represent full SSO concert record."""

    repertoire: Repertoire
    artistconductor: ArtistConductor

    @property
    def namedtuple(self) -> NamedTuple:
        """Return NamedTuple with base concert data."""
        ConcertNT = namedtuple('ConcertNT', 'Concert Key Date Piece Composer Conductor Artist_Metadata')
        return ConcertNT(Concert=self.title, Key=self.key, Date=self.date, Piece=self.repertoire.pieces, Composer=self.repertoire.composers, Conductor=self.artistconductor.conductor, Artist_Metadata=self.artistconductor.artists)


# %%
def sso_parse_artists_legacy(concert) -> ArtistConductor:
    """Return conductor and artists for a particular concert. For seasons before 2021."""
    # if there are no listed artists, return ArtistConductor dict with default values ('Unknown')
    if concert.find('h5', text='Artists') == None:
        return ArtistConductor(artist_data=[])
    # else return ArtistConductor dict with relevant conductor and artist values
    else:
        concert_list = []
        for dt in concert.find('h5', text='Artists').findNextSibling('dl').find_all('dt'):
            if not dt.find_next_sibling('dd'):
                raise ValueError("Error: Tag '<dd>' is missing from HTML source")
            else:
                concert_list.append((dt.find_next_sibling('dd').text.strip(), dt.text.strip()))
        return ArtistConductor(artist_data=concert_list)

def sso_parse_artists_current(concert) -> ArtistConductor:
    """Return conductor and artists for a particular concert. For seasons on or after 2021."""
    # remove extraneous <br> tags
    [ br_tag.decompose() for br_tag in concert.find_all('br') ]

    # if there are no listed artists, return ArtistConductor dict with default values ('Unknown')
    if concert.find('h2', text=re.compile(r'Artist|ARTIST')) == None:
        return ArtistConductor(artist_data=[])
    # else return ArtistConductor dict with relevant conductor and artist values
    else:
        repertoire_list = []
        artist = concert.find('h2', text=re.compile(r'Artist|ARTIST')).find_next_sibling('p')
        # remove items with empty Tag content
        [ strong_tag.decompose() for strong_tag in artist.find_all('strong') if not strong_tag.text.strip() ]
        artist_items = [ item for item in filter(lambda e: (isinstance(e, element.Tag) and e.name != 'br' and e.text.strip()) or isinstance(e, element.NavigableString), artist.contents) ]
        for idx, item in enumerate(artist_items):
            if isinstance(item, element.Tag):
                current_item = [ c.string.strip() for c in filter(lambda ic: not isinstance(ic, element.Tag), item.contents) ]
                # if next_sibling is a NavigableString, it's probably a conductor ('conductor') or artist label (e.g. 'piano')
                if isinstance(item.next_sibling, element.NavigableString):
                    if current_item[0].strip().lower() == 'sydney symphony orchestra musicians':
                        current_item = ''.join(current_item).title()
                        repertoire_list.append(('Artist', current_item))
                    else:
                        current_item = ''.join(current_item).title()
                        next_item = artist_items[idx+1].string.strip()
                        repertoire_list.append((next_item, current_item))
                        del artist_items[idx+1]
                else:
                    current_item = ''.join(current_item).title()
                    repertoire_list.append(('Artist', current_item))
            else:
                current_item = item.string.strip()
                current_item = ''.join(current_item).title()
                repertoire_list.append(('Artist', current_item))
        return ArtistConductor(artist_data=repertoire_list)


# %%
def sso_parse_repertoire_legacy(concert) -> Repertoire:
    """Return composers and pieces for a particular concert. For seasons before 2021."""
    GENERAL_EXCLUDE_KEYWORDS = ['and more', 'based on', 'featuring', 'highlight', 'including', 'plus previous', 'with australian interludes']
    COMPOSER_EXCLUDE_KEYWORDS = ['friday', 'interval', 'performs', 'program', 'songs for', 'thursday', 'wednesday']

    # if there are no listed composer/pieces, return Repertoire dict with default values ('Unknown')
    if concert.find('h5', text='Program') == None:
        return Repertoire(repertoire_data=[])
    # else return Repertoire dict with relevant composer and piece values
    else:
        repertoire_list = []
        composer = ''
        for dt in concert.find('h5', text='Program').findNextSibling('dl').find_all('dt'):
            if not dt.find_next_sibling('dd'):
                raise ValueError("Error: Tag '<dd>' is missing from HTML source")
            else:
                """
                Do some edge case pre-processing and populate any missing composer data.
                One composer can have multiple pieces programmed in a single concert, 
                so if no composer is specified for a piece, the piece's author is assumed to be
                the last stored composer value
                """
                dt_stripped = dt.text.strip()
                dd_stripped = dt.find_next_sibling('dd').text.strip()
                if dt_stripped:
                    # only capture composer values that are plausibly composer names
                    if not any(word in dt_stripped.lower() for word in (GENERAL_EXCLUDE_KEYWORDS + COMPOSER_EXCLUDE_KEYWORDS)):
                        # edge case: flag items for deletion that include 'composer' or any excluded keywords
                        if (dd_stripped == 'composer') or any(word in dd_stripped.lower() for word in GENERAL_EXCLUDE_KEYWORDS):
                            dd_stripped = 'DELETE'
                        # finally, set composer value if the <dt> value is not 'And' (i.e. not a valid composer)
                        if dt_stripped != 'And':
                            composer = dt_stripped.title()
                    else:
                        dd_stripped = 'DELETE'
                else:
                    # if item is using one of the excluded words, mark entry for exclusion from repertoire list
                    if any(word in dd_stripped.lower() for word in GENERAL_EXCLUDE_KEYWORDS):
                        dd_stripped = 'DELETE'
                # only add repertoire items that haven't been marked for exclusion
                if (dd_stripped != 'DELETE') :
                    repertoire_list.append((dd_stripped, composer))
        return Repertoire(repertoire_data=repertoire_list)

def sso_parse_repertoire_current(concert) -> Repertoire:
    """Return composers and pieces for a particular concert. For seasons on or after 2021."""
    # if there are no listed composer/pieces, return Repertoire dict with default values ('Unknown')
    if concert.find('h2', text=re.compile(r'Program|PROGRAM')) == None:
        return Repertoire(repertoire_data=[])
    # else return Repertoire dict with relevant composer and piece values
    else:
        repertoire_list = []
        composer = []
        program = concert.find('h2', text=re.compile(r'Program|PROGRAM')).find_next_sibling('p')
        program_items = [ item for item in filter(lambda e: (isinstance(e, (element.Tag)) and e.name != 'br' and e.text) or isinstance(e, (element.NavigableString)), program.contents) ]

        """Do some edge case pre-processing and populate any missing composer data.
        One composer can have multiple pieces programmed in a single concert, 
        so if no composer is specified for a piece, the piece's author is assumed to be
        the last stored composer value
        """
        for idx, item in enumerate(program_items):
            # composer names are almost always a Tag (except when they aren't)
            if isinstance(item, element.Tag):
                current_item = [ c.string.strip() for c in filter(lambda ic: ic.name != 'br', item.contents) ]
                # if next_sibling is a NavigableString, it's probably a piece name
                if isinstance(item.next_sibling, element.NavigableString):
                    # edge case: ['FIFTY FANFARES COMMISSION', 'Actual Composer']
                    if len(current_item) == 2:
                        composer = [current_item[1]]
                        if current_item[0].strip().lower() == 'fifty fanfares commission':
                            next_item = current_item[0].title()
                            current_item = ['Unknown']
                    # otherwise process as normal
                    else:
                        composer = current_item
                        next_item = item.next_sibling.string.strip()
                        del program_items[idx+1]
                # if next_sibling is a Tag, assume it's an actual composer
                else:
                    # edge case: ['FIFTY FANFARES COMMISSION', 'Actual Composer']
                    if current_item[0].strip().lower() == 'fifty fanfares commission':
                        next_item = current_item[0].title()
                        current_item = ['Unknown']
                    else:
                        next_item = item.next_sibling
                        current_item.append(next_item.text.strip())
                        composer = current_item
                        next_item = next_item.next_sibling.string.strip()
                        del program_items[idx:idx+2]
            # else assume that the composer is the last stored composer value
            else:
                current_item = composer
                next_item = item
            current_item = ' '.join(current_item).title()
            repertoire_list.append((next_item, current_item))
        return Repertoire(repertoire_data=repertoire_list)


# %%
def sso_parse_individual_concerts(df) -> List[NamedTuple]:
    """Return list of concerts for a specified time period."""
    concert_list = []

    # parse pre-imported concert HTML content from the DB, one by one
    for _, row in df.iterrows():
        year = row['year']
        event = row['html_content']
        title = html.unescape(event.find('title').text)

        # extract concert dates - 2021 dates require extra ugly pre-processing
        if year == 2021:
            # convert 'Sun 04 November, 07:00 PM' to 'Sun 04 Nov, 07:00 PM'
            cdate = [ date_str.text.strip() for date_str in event.find_all('span', attrs={'class': 'u-show-inline@small'}) ]
            cdate = [ datetime.strptime(re.sub(r'([,]?\n[ ]{1,}?)(\w.+)([ap]m\Z)', r', \2 \3', date_str), '%a %d %B, %I:%M %p').strftime('%a %d %b, %I:%M %p') for date_str in cdate ]
        else:
            cdate = event.find('h5', text='Dates').findNextSibling('dl').find_all('div', class_='date')

        # if there's only one date, extract it and append concert object to concerts list
        if isinstance(cdate, element.Tag):
            if year == 2021:
                repertoire = sso_parse_repertoire_current(event)
                conductor_artists = sso_parse_artists_current(event)
            else:
                repertoire = sso_parse_repertoire_legacy(event)
                conductor_artists = sso_parse_artists_legacy(event)
            concert_combined = ConcertFull(title=title, key=row['key'], date=cdate.text, year=year, repertoire=repertoire, artistconductor=conductor_artists).namedtuple
            concert_list.append(concert_combined)
        # else loop through all of the dates and construct concert objects accordingly
        else:
            for c in cdate:
                # we assume that the same repertoire is played for each performance of
                # a particular concert (in the majority of cases)
                if year == 2021:
                    repertoire = sso_parse_repertoire_current(event)
                    conductor_artists = sso_parse_artists_current(event)
                    concert_combined = ConcertFull(title=title, key=row['key'], date=c, year=year, repertoire=repertoire, artistconductor=conductor_artists).namedtuple
                else:
                    repertoire = sso_parse_repertoire_legacy(event)
                    conductor_artists = sso_parse_artists_legacy(event)
                    concert_combined = ConcertFull(title=title, key=row['key'], date=c.text, year=year, repertoire=repertoire, artistconductor=conductor_artists).namedtuple
                concert_list.append(concert_combined)
    return concert_list

# %%
def main():
    # check whether to use back- or forward-slash path separators, depending on platform (Windows or Unix-based)
    if sys.platform in ['cygwin', 'win32']:
        path_separator = '\\'
    else:
        path_separator = '/'
    
    START_YEAR = "2018"
    END_YEAR = "2021"
    DB_NAME = f"sso_html_{START_YEAR}_{END_YEAR}.db"
    OUTFILE_PREFIX = f"sso_{START_YEAR}_{END_YEAR}_raw"
    # load data from SQLite DB
    df_imported_db = file_utils.ProcessSQLite(DB_NAME).load_sqlite_db()

    # parse into DataFrame and write to disk
    pd.set_option("display.width", 120)
    df = pd.DataFrame(sso_parse_individual_concerts(df_imported_db))
    
    # output pickle file (preserves lists and tuples correctly, whereas JSON does not)
    df.to_pickle(f"data{path_separator}{OUTFILE_PREFIX}.pkl")
    print(f"Wrote Pickle file: data{path_separator}{OUTFILE_PREFIX}.pkl")
    # also output human-readable CSV copy
    df.to_csv(path_or_buf=f"data{path_separator}{OUTFILE_PREFIX}.csv", index=False)
    print(f"Wrote CSV file: data{path_separator}{OUTFILE_PREFIX}.csv")

# %%
if __name__ == '__main__':
    main()