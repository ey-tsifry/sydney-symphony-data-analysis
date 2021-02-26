# Notes on data acquisition and preparation

## Table of Contents
1. [Data sources](#sources)
2. [Data acquisition](#acquisition)
3. [Data preparation and analysis](#preparation)
4. [Data strategy and integrity](#strategy)
5. [Final note](#finalnote)

## DATA SOURCES <a name="sources"></a>

All SSO data was aggregated from the orchestra's [public website](https://www.sydneysymphony.com) in 2019 and 2020.

An initial list of composer and conductor metadata (canonical names, birth/death dates, nationalities) was sourced from [Wikipedia](https://en.wikipedia.org) and [Wikidata](https://query.wikidata.org).  Any missing or unavailable metadata was augmented by information in the New Grove Dictionary of Music, the author's brain and from internet research.

## DATA ACQUISITION <a name="acquisition"></a>

Linux command line tools (`curl`, `wget`) were used to retrieve all concert data (HTML, JSON).

### SSO CONCERT DATA: 2018-2021
As of October 2019, full 2018-2020 season event calendars were available at https://www.sydneysymphony.com/concerts-and-tickets/whats-on/full-calendar. (These disappeared after SSO rolled out their new website design in 2020).

2018-2020 event pages used the following URL scheme: `https://www.sydneysymphony.com/concerts-and-tickets/whats-on/event/{EVENT_KEY}`

As of October 2020, the full 2021 season event calendar was available at https://www.sydneysymphony.com.  Conveniently, all 2021 calendar metadata was aggregated in a single JSON file (*concerts.json*), so there was no need to download multiple HTML files.

2021 event pages used the following URL scheme: `https://www.sydneysymphony.com/concerts/{EVENT_KEY}`

***Note for 2021:** Only concerts in the official SSO 2021 season were retrieved. This excludes any concerts that were part of the Sydney Festival in January.*

High-level data acquisition process:

1. Download **2018-2020 season calendars** (seasons run February-December) to local HTML files.  Example file: `2020/sso_2020_February.html`

2. Download **2021 season calendar** to local JSON file: `2021/sso-concerts-2021.json`

3. Run `1-sso_parse_2018_2021_calendars.py` to create `sso_{YEAR}_keys.csv` files from the calendar data. The key values are **required for the next step**.
    * [**Optional**] 2021 concert keys can also be extracted directly from the JSON using `jq` (*requires Linux*): `jq -r '.data[].url | split("/") | .[-1]' < 2021/sso-concerts-2021.json | sort > 2021/sso_2021_keys.csv`

4. Download **2018-2021 individual events** to local HTML files (**prerequisite:** `sso_{YEAR}_keys.csv` files with event keys in each relevant download directory).  Example file: `2021/events/adams-shostakovich.html`

### WIKIPEDIA: COMPOSER NATIONALITY AND TIMELINE METADATA

Wikipedia data was downloaded in XML format from https://en.wikipedia.org/w/index.php?title=Special:Export

* `Wikipedia-Composers_by_nationality.xml` - included categories:
  * Lists_of_composers_by_nationality
* `Wikipedia-Composers_by_sub_nationality.xml` - included categories:
  * List_of_American_composers
  * List_of_Armenian_composers
  * List_of_Australian_composers
  * List_of_Australian_female_composers
  * Chronological_list_of_Australian_classical_composers
  * List_of_Austrian_composers
  * Chronological_list_of_Austrian_classical_composers
  * List_of_Canadian_composers
  * List_of_Czech_composers
  * List_of_Estonian_composers
  * List_of_French_composers
  * List_of_German_composers
  * List_of_Icelandic_composers
  * Chronological_list_of_English_classical_composers
  * List_of_Italian_composers

### WIKIDATA: COUNTRY AND CONDUCTOR METADATA

An initial CSV list of **country** data was downloaded from https://query.wikidata.org using the following SPARQL query:
```
SELECT DISTINCT ?countryLabel WHERE {
  { ?country wdt:P31 wd:Q3624078. }
    UNION
  { ?country wdt:P31 wd:Q6256. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY (?countryLabel)
```

A second column was then manually added to the CSV to map each Wikidata country name to a more 'standard' name.  In most cases, this was the same name.  But in a few cases, the name changed.  Example:
```
United Kingdom of Great Britain and Ireland --> United Kingdom
```

**Conductor** metadata was compiled in three stages:
1. Generate de-duplicated list of conductors from the SSO data.
2. Best effort query of conductor nationality and gender from the Wikidata API.  Use country name map (see above) to standardise nationality values.
3. Manually impute any nationality or gender values that were not matched in Wikidata.

(***General note**: If collecting Wikipedia/Wikidata data was a more frequent process, I would have put more effort into automating it.  For the purposes of this project, however, a semi-manual process was acceptable*).

### SEMI-MANUALLY GENERATED DATA

For the most part, the raw SSO data only lists composers by their last name.  This presents a barrier to programmatically retrieving composer metadata (nationality, birthdate, etc) from data sources such as Wikipedia since the join must be made on a composer's name.  Matching becomes even more complicated for composers who happen to share last names (e.g. Adams, Bach, Mendelssohn, Mozart, Schumann, Strauss).

The workaround for this was to manually create a *last name -> full name* map file (`sso_composer_name_map.csv`), which was then used to match as much metadata as possible from Wikipedia and Wikidata sources.  Any unmatched metadata was imputed manually, and the final aggregated data was written to `sso_composer_nationality_map.csv` and `sso_conductor_nationality_map.csv`.

## PREPARING AND ANALYSING THE DATA <a name="preparation"></a>

Overview of the data preparation and analysis process:

1. Create utility functions for processing various file formats (CSV, HTML, JSON, Pickle, SQLite, XML)
    * Script: `sso_utilities/file_utils.py`

2. Aggregate raw HTML for each concert into a SQLite database:
    * Script: `2-sso_import_export_2018_2021_concerts.py` 
    * Output: `sso_html_2018_2021.db`

3. Extract and aggregate relevant concert details (dates, titles, conductors, composers, artists) from the HTML:
    * Script: `3-sso_parse_2018_2021_concerts.py`
    * Output 1: `sso_2018_2021_raw.pkl`
    * Output 2: `sso_2018_2021_raw.csv` [human readable]

4. Clean the processed concert data:
    * Script: `4-sso_clean_2018_2021_concerts.py`
    * Output 1: `sso_2018_2021_cleaned.pkl`
    * Output 2: `sso_2018_2021_cleaned.csv` [human readable]
    * Output 3: `sso_composer_name_map.csv` [initial composer name map template]

5. Merge cleaned concert data with Wikipedia/Wikidata metadata:
    * Notebook: `5-sso_clean_merge_wikipedia_composer_data.ipynb`
    * Output 1: `sso_2018_2021_cleaned_merged.pkl`
    * Output 2: `sso_2018_2021_cleaned_merged.csv` [human readable]

6. Calculate stats and generate graphs
    * Notebook: `6-sso_analysis_and_graphs.ipynb`

## DATA STRATEGY AND INTEGRITY <a name="strategy"></a>

### STRATEGY

Below is an outline of the general strategy and considerations that went into the data acquisition and preparation process:

1. Work with downloaded local copies of all concert HTML content
    * Minimises necessity for live HTTP/S requests
    * Caveat: Unless syncing on a regular basis, we may miss out on subsequent updates to website data. But on the other hand, once concert seasons have been published, their underlying details mostly don't change, so any data discrepancies should be minimal in practise.
2. Aggregate all HTML data into a single local data store (SQLite database). This minimises the number of future file read operations.
3. First-pass data munging/cleaning:
    * Automate as much of the munging/cleaning as possible
        * Funnel data from disparate sources and formats (2018-2020 vs 2021) into uniform data structures
        * Side note: The 2018-2020 data is far messier than the 2021 data. It looks like SSO redesigned their website for the 2021 season, so the 2021 content structure is different but more uniform and consistent and actually required no cleaning whatsoever.
    * Aim to realistically cover *most* (90%+) but not all data quirks:
        * Website pages are presumably generated in a systematic way from a backend data store, so *most* of the data is in a predictable format.
        * There are edge cases that do not follow a completely predictable format, however.  We automate munging/cleaning these when possible, but save the one- and two-offs for manual cleanup (see below).
4. Second-pass data munging/cleaning:
    * Fix any edge cases that could not be addressed via automation
5. Match SSO composer data with Wikipedia/Wikidata composer data
    * Standardise composer names
    * Impute composer gender (most are male; we update any female composers manually)
    * Match nationality automatically when possible. Fix edge cases manually.
    * Impute animate status (living/dead) automatically when possible. Fix edge cases manually.
    * Export merged data to CSV for future re-use
6. Match SSO conductor data with Wikidata composer data
    * Standardise conductor names
    * Query conductor nationality and gender from Wikidata.  Manually impute missing values.
    * Export merged data to CSV for future re-use

### INTEGRITY

The final data set is as accurate and thorough as possible given the circumstances (after much data cleaning and general gnashing of teeth).  With that being said, there are a few caveats:

* SSO almost certainly has a database, or even multiple databases, of concert information somewhere (otherwise they could not dynamically generate their web page content)
    * However, these databases are also almost certainly not publicly available.  So scraping the public website was the next best available option.
    * As a consequence, the finished data product is less perfect than it could be due to the significant amount of processing it had to endure.

* Wikipedia and Wikidata were used as authoritative, albeit problematic, sources for composer and conductor metadata:
    * I briefly considered Classical.net, but its data hasn't been updated since the late 1990s and was thus too stale.
    * New Grove Dictionary of Music would have been my ideal source in an ideal world, but it doesn't provide a public API or a publicly accessible omnibus list of composers, so it is not conducive to programmatic queries.
    * Wikidata was easier to query for conductor details since there are relatively few unique conductors (60) in the SSO data set.
    * Wikipedia was easier to query for initial composer metadata since Wikidata queries for all 200+ composers consistently timed out because SPARQL.  I used Wikidata (and manual research, as necessary) to augment any metadata that was not in Wikipedia.

* Other general Wikipedia/Wikidata caveats:
    * It's Wikipedia.
    * It's Wikidata.
    * Composer names, nationalities and birth/death dates are assumed to be *largely* but not *completely* accurate.
    * Ditto for conductor metadata.

## FINAL NOTE <a name="finalnote"></a>

Upwards of 90% of the labour in this project was devoted to acquiring, cleaning and munging imperfect data from disparate sources into a unified form.  (Which, yes, does describe most typical data projects. :)

The amount of effort required would have been reduced significantly had the data been available via, e.g., an API.  Alas, it was not.  (Perhaps that would have made life too easy).  But it does seem that this could be a great open data transparency initiative for an orchestra to pilot in future!  Just a thought...