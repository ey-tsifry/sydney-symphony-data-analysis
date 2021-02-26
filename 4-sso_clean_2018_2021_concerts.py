#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Clean the raw compiled SSO 2018-2021 concert data."""

# %%
from functools import reduce
from sso_utilities import file_utils
import pandas as pd
import re
import sys

# %%
class SSOClean:
    """Functions to manually clean raw aggregated SSO concert data."""
    
    def __init__(self, sso_raw_df: pd.DataFrame) -> None:
        """Manually clean raw aggregated SSO concert data.

        :param sso_raw_df: DataFrame with data from imported SSO raw pickle file
        """
        self._sso_df = sso_raw_df.copy()
        self._preclean_extraneous_rows()
        self._clean_artists()
        self._clean_pieces()
        self._clean_composers()
        self._clean_conductors()
    
    @property
    def df(self) -> pd.DataFrame:
        """Return DataFrame of cleaned data."""
        self._sso_df.reset_index(inplace=True, drop=True)
        return self._sso_df
    
    def _preclean_extraneous_rows(self) -> None:
        """Pre-clean extraneous and fully NaN records.  This should be run first."""
        # remove cancelled 2019 Previn/Stoppard concert
        self._sso_df.drop(self._sso_df.loc[(self._sso_df['Key'] == 'tom-stoppard-and-andre-previns-every-good-boy-deserves-favour')].index, inplace=True)
        # drop fully NaN rows
        self._sso_df.dropna(axis=0, how='all', inplace=True)

    def _clean_artists(self) -> None:
        """Manually correct Artist_Metadata edge cases.

        Note: Artist metadata isn't used for any analysis currently (it would require significantly more standardisation work first), but is captured here anyway for potential future iterations of this project.
        """
        # update 'ben-folds' edge case
        self._sso_df.loc[(self._sso_df['Key'] == 'ben-folds-the-symphonic-tour'), 'Artist_Metadata'] = self._sso_df.loc[(self._sso_df['Key'] == 'ben-folds-the-symphonic-tour'), 'Artist_Metadata'].apply(lambda artist: [('Artist', 'Ben Folds')])
        # update 'eskimo-joe' edge case
        self._sso_df.loc[(self._sso_df['Key'] == 'eskimo-joe'), 'Artist_Metadata'] = self._sso_df.loc[(self._sso_df['Key'] == 'eskimo-joe'), 'Artist_Metadata'].apply(lambda artist: [('Artist', 'Eskimo Joe')])
        # update 'johann-johannsson-last-and-first-men' edge case
        self._sso_df.loc[(self._sso_df['Key'] == 'johann-johannsson-last-and-first-men'), 'Artist_Metadata'] = self._sso_df.loc[(self._sso_df['Key'] == 'johann-johannsson-last-and-first-men'), 'Artist_Metadata'].apply(lambda artist: [('Narrator', 'Tilda Swinton'), ('Harmonium', 'Yair Glotman'), ('Vocalist', 'Else Torp'), ('Vocalist', 'Kate Macoboy')])

    def _clean_conductors(self) -> None:
        """Manually correct Conductor edge cases."""
        # update 'introduced-species-at-sydney-ideas' edge case
        self._sso_df.loc[(self._sso_df['Key'] == 'introduced-species-at-sydney-ideas'), 'Conductor'] = 'Iain Grandage'
        # update 'music-of-count-basie' edge case
        self._sso_df.loc[(self._sso_df['Key'] == 'music-of-count-basie-and-duke-ellington'), 'Conductor'] = 'Wynton Marsalis'
    
    def _clean_pieces(self) -> None:
        """Manually correct Piece edge cases."""
        # update concert where pieces were added to composer column
        self._sso_df.loc[(self._sso_df['Key'] == 'james-morrison-with-the-sydney-symphony-orchestra'), 'Piece'] = self._sso_df.loc[(self._sso_df['Key'] == 'james-morrison-with-the-sydney-symphony-orchestra'), 'Composer']
        
        # update 'songs-of-the-north' edge case - remove erroneous ('Iain Grandage', 'conductor')
        self._sso_df.loc[(self._sso_df['Key'] == 'introduced-species-at-sydney-ideas'), 'Piece'] = self._sso_df.loc[(self._sso_df['Key'] == 'introduced-species-at-sydney-ideas'), 'Piece'].apply(lambda piece: [ p for p in piece if p != 'conductor' ])
    
        # update piece names for live film score concerts to '<NAME> Film Score'
        keys_to_update = ['blue-planet-2-live-in-concert', 'casino-royale-in-concert', 
        'disney-in-concert-mary-poppins', 'funny-girl', 'harry-potter-and-the-half-blood-prince-in-concert', 
        'harry-potter-and-the-order-of-the-phoenix-in-concert', 'harry-potter-and-the-prisoner-of-azkaban', 
        'johann-johannsson-last-and-first-men', 'planet-earth-ii-live-in-concert', 
        'skyfall-in-concert', 'star-wars-a-new-hope', 'star-wars-return-of-the-jedi', 
        'star-wars-the-empire-strikes-back', 'star-wars-the-force-awakens']
        for key in keys_to_update:
            if key.startswith(('blue-planet', 'casino', 'harry-potter', 'planet-earth', 'skyfall', 'star-wars')):
                self._sso_df.loc[self._sso_df['Key'] == key, 'Piece'] = self._sso_df.loc[self._sso_df['Key'] == key, 'Concert'].apply(lambda piece: [re.sub(r'(\w.+) (in concert)', r'\1', piece, flags=re.IGNORECASE).strip('™') + ' Film Score'] if piece.lower().endswith('in concert') else [piece.strip('™') + ' Film Score'])
            elif key.startswith('funny-girl'):
                self._sso_df.loc[self._sso_df['Key'] == key, 'Piece'] = self._sso_df.loc[self._sso_df['Key'] == key, 'Concert'].apply(lambda piece: [re.sub(r'(\w.+) (in concert)', r'\1', piece, flags=re.IGNORECASE).strip()])
            elif key.startswith(('disney-in-concert', 'johann-johannsson')):
                self._sso_df.loc[self._sso_df['Key'] == key, 'Piece'] = self._sso_df.loc[self._sso_df['Key'] == key, 'Concert'].apply(lambda piece: [re.sub(r'((disney in concert:)|(jóhann jóhannsson\'s)) (\w.+)', r'\4 Film Score', piece, flags=re.IGNORECASE).strip()])

    def _clean_composers(self) -> None:
        """Manually correct Composer edge cases.

        How to extract all existing composer values: from functools import reduce; sorted(set(reduce(lambda x, y: x + y, sso_df['Composer'])))
        """
        # update 'introduced-species-at-sydney-ideas' edge case - remove erroneous ('Iain Grandage', 'conductor') item
        self._sso_df.loc[(self._sso_df['Key'] == 'introduced-species-at-sydney-ideas'), 'Composer'] = self._sso_df.loc[(self._sso_df['Key'] == 'introduced-species-at-sydney-ideas'), 'Composer'].apply(lambda composer: [ c for c in composer if c != 'Iain Grandage' ])

        # if composer was added to Artist_Metadata but not Composer, impute that value to Composer
        am_composer_keys = []
        for _, row in self._sso_df.loc[(self._sso_df['Composer'].apply(lambda x: 1 if x==['Unknown'] else 0) == 1)].iterrows():
            if (len(row['Artist_Metadata']) == 1) and ([ artist[0] for artist in row['Artist_Metadata'] if artist[0].lower() == 'composer' ]):
                am_composer_keys.append(row['Key'])
        if am_composer_keys:
            am_composer_keys = sorted(set(am_composer_keys))
            for key in am_composer_keys:
                self._sso_df.loc[(self._sso_df['Key'] == key), 'Composer'] = self._sso_df.loc[(self._sso_df['Key'] == key), 'Artist_Metadata'].apply(lambda composer: [ c[1] for c in composer ])

        # manually impute other missing and/or ambiguous or incorrect composer values
        missing_dict = {'eskimo-joe': ['Eskimo Joe'], 'evanescence': ['Evanescence'], 
        'paloma-faith-with-the-sso': ['Paloma Faith'], 'thum-prints': ['Gordon Hamilton'], 
        'johann-johannsson-last-and-first-men': ['Jóhann Jóhannsson'], 'funny-girl': ['Jule Styne'], 
        'disney-in-concert-mary-poppins': ['Robert B. and Richard M. Sherman'], 
        'harry-potter-and-the-half-blood-prince-in-concert': ['John Williams'], 
        'harry-potter-and-the-order-of-the-phoenix-in-concert': ['John Williams'],  
        'harry-potter-and-the-prisoner-of-azkaban': ['John Williams'], 'star-wars-a-new-hope': ['John Williams'], 
        'star-wars-return-of-the-jedi': ['John Williams'], 'star-wars-the-empire-strikes-back': ['John Williams'], 
        'star-wars-the-force-awakens': ['John Williams'], 'planet-earth-ii-live-in-concert': ['Hans Zimmer'], 
        'blue-planet-2-live-in-concert': ['Hans Zimmer'], 
        'james-morrison-with-the-sydney-symphony-orchestra': ['Cole Porter', 'George Gershwin', 'George Gershwin', 'Duke Ellington'], 
        'lea-salonga-in-concert-with-the-sydney-symphony-orchestra': ['Claude-Michel Schönberg', 'Benj Pasek and Justin Paul']}
        for key in missing_dict:
            self._sso_df.loc[(self._sso_df['Key'] == key), 'Composer'] = self._sso_df.loc[(self._sso_df['Key'] == key), 'Composer'].apply(lambda composer: missing_dict[key])

        # sanitise composer strings
        self._sso_df['Composer'] = self._sso_df['Composer'].apply(lambda composers: [ re.sub(r'^(\w.+)( (After|Arr.|Orch.|\(?Text By|Trans.) \w.+)$', r'\1', composer).replace('&', 'and') for composer in composers ])
    
    @staticmethod
    def generate_composer_name_map_template(sso_cleaned_df: pd.DataFrame, out_csv_file: str = 'sso_composer_name_map.csv') -> pd.DataFrame:
        """Generate a CSV with a pre-filled Composer->ComposerFullName mapping and Gender.

        After generation, ComposerFullName and Gender should be updated manually with the real values.

        :param sso_cleaned_df: DataFrame of pre-cleaned SSO data
        :param out_csv_file: CSV file name
        """
        deduped_composers = sorted(set(reduce(lambda x, y: x+y, sso_cleaned_df['Composer'])))
        map_df = pd.DataFrame(deduped_composers, columns=['Composer'])
        map_df['ComposerFullName'] = map_df['Composer']
        map_df['Gender'] = 'Male'
        map_df.to_csv(path_or_buf=out_csv_file, encoding='utf-8', index=False)
        print(f"Wrote composer name map template to: {out_csv_file}")
        return map_df
    
    @staticmethod
    def fix_composer_name_spellings(sso_cleaned_df: pd.DataFrame, sso_composer_map_file: str) -> pd.DataFrame:
        """Normalise composer names to match their spellings in Wikipedia.

        :param sso_cleaned_df: DataFrame of pre-cleaned SSO data
        :param sso_composer_map_file: CSV file with pre-filled composer name mappings and gender
        """
        try:
            with open(sso_composer_map_file, 'r', encoding='utf-8') as map_file:
                sso_composer_name_map = pd.read_csv(map_file, encoding='utf-8')
        except OSError as e:
            print(e)
            sys.exit(1)
        
        name_dict = dict(zip(sso_composer_name_map['Composer'].apply(lambda name: name.lower()), sso_composer_name_map['ComposerFullName']))
        df_copy = sso_cleaned_df.copy()

        df_copy['Composer'] = df_copy['Composer'].apply(lambda composers: [ name_dict[composer.lower() ] if composer.lower() in name_dict else composer for composer in composers ])
        return df_copy

# %%
def main():    
    START_YEAR = "2018"
    END_YEAR = "2021"
    PICKLE_IN_FILE = f"sso_{START_YEAR}_{END_YEAR}_raw.pkl"
    COMPOSER_MAP_FILE = "sso_composer_name_map.csv"
    OUTFILE_PREFIX = f"sso_{START_YEAR}_{END_YEAR}_cleaned"

    # check whether to use back- or forward-slash path separators, depending on platform (Windows or Unix-based)
    if sys.platform in ['cygwin', 'win32']:
        path_separator = '\\'
    else:
        path_separator = '/'

    # load Pickle file into a DataFrame
    sso_raw = file_utils.ProcessPickle().load_pickle(f"data{path_separator}{PICKLE_IN_FILE}")
    print(f"Raw import dimensions: {sso_raw.shape}")

    # manually clean Artist, Piece, Composer, Conductor data edge cases
    sso_cleaned = SSOClean(sso_raw).df

    # (if needed) generate composer name map CSV template
    #SSOClean.generate_composer_name_map_template(sso_cleaned, f"data{path_separator}{COMPOSER_MAP_FILE}")
    
    # if COMPOSER_MAP_FILE does not already exist, uncomment the previous line to generate the file
    # (then remember to update it manually)
    sso_cleaned = SSOClean.fix_composer_name_spellings(sso_cleaned, f"data{path_separator}{COMPOSER_MAP_FILE}")
    print(f"sso_cleaned dimensions: {sso_cleaned.shape}\n")

    # write to Pickle
    sso_cleaned.to_pickle(f"data{path_separator}{OUTFILE_PREFIX}.pkl")
    print(f"Wrote Pickle file: data{path_separator}{OUTFILE_PREFIX}.pkl")
    # write to CSV
    sso_cleaned.to_csv(path_or_buf=f"data{path_separator}{OUTFILE_PREFIX}.csv", index=False)
    print(f"Wrote CSV file: data{path_separator}{OUTFILE_PREFIX}.csv")

# %%
if __name__ == '__main__':
    main()