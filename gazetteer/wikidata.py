from csv import QUOTE_ALL
from glob import glob

import pandas.errors
import re
from time import sleep

from numpy import nan
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from helper.helpers_logging import setup_logger
from helper.methods import normalize_text

logger = setup_logger('wikidata_gazetteer', 'results/wikidata/logs/wikidata_gazetteer.log')

LINK_PATTERN = re.compile('http://www.wikidata.org/entity/')

SPARQL = SPARQLWrapper("https://query.wikidata.org/sparql",
                       agent="Gazetteer_Builder_Bot/0.0 (lm28cizi@studserv.uni-leipzig.de) SPARQLWrapper/1.8.5 " + "(Building a gazetteer for places in Saudi Arabia including settlements and holy sites")
SPARQL.setReturnFormat(JSON)


class Country:
    def __init__(self, country, en_label, coordinates, country_label, date_start, date_end):
        self.country = country
        self.enLabel = en_label
        self.coordinates = coordinates
        self.countryLabel = country_label
        self.dateStart = date_start
        self.dateEmd = date_end


class Place:
    def __init__(self, place, en_label, place_label, place_alt_labels, coordinates, type_label, country):
        self.place = place
        self.enLabel = en_label
        self.placeLabel = place_label
        self.placeAltLabel = place_alt_labels
        self.coordinates = coordinates
        self.typeLabel = type_label
        self.country = country


def request_countries_within_radius():
    SPARQL.setQuery("""
    SELECT DISTINCT ?place ?placeLabel ?arLabel ?location ?start ?end WHERE {
        wd:Q5806 wdt:P625 ?mainLoc.
        ?place (wdt:P31/(wdt:P279*)) wd:Q6256.
        SERVICE wikibase:around {
            ?place wdt:P625 ?location.
            bd:serviceParam wikibase:center ?mainLoc;
                wikibase:radius "5000".
        }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        ?place rdfs:label ?arLabel.
        FILTER((LANG(?arLabel)) = "ar")
        OPTIONAL{ ?place wdt:P571 ?start. }
        OPTIONAL{ ?place  wdt:P576 ?end. }
    }
    """)
    countries = []
    results = SPARQL.queryAndConvert()

    try:
        for result in results["results"]["bindings"]:
            country = LINK_PATTERN.sub('', result['place']['value'])
            en_label = result['placeLabel']['value']
            coordinates = result['location']['value'] if 'location' in result else nan
            country_label = result['arLabel']['value']
            date_start = result['start']['value'] if 'start' in results else nan
            date_end = result['end']['value'] if 'end' in results else nan
            countries.append(Country(country, en_label, coordinates, country_label, date_start, date_end))
        df = pd.DataFrame([country.__dict__ for country in countries])
        df.to_csv('results/wikidata/query_results/Around_Mecca.csv', index=False)
    except Exception as e:
        logger.error(e)


def request_settlements_in_country(idx, path):
    start = """    SELECT DISTINCT ?place ?enLabel ?placeLabel ?placeAltLabel ?typeLabel ?coordinates WHERE {"""
    middle = f"        ?place wdt:P17 wd:{idx};"
    end = """            (wdt:P31/(wdt:P279*)) wd:Q486972.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "ar". }
        ?place rdfs:label ?enLabel.
        FILTER((LANG(?enLabel)) = "en")
        OPTIONAL { ?place wdt:P625 ?coordinates. }
        ?place wdt:P31 ?type.
        ?type rdfs:label ?typeLabel.
        FILTER(lang(?typeLabel) = "en").
    }"""
    query = '\n'.join([start, middle, end])
    SPARQL.setQuery(query)
    # query = """
    # SELECT DISTINCT ?place ?enLabel ?placeLabel ?placeAltLabel ?typeLabel ?coordinates WHERE {
    #     ?place wdt:P17 wd:Q851;
    #         (wdt:P31/(wdt:P279*)) wd:Q486972.
    #     SERVICE wikibase:label { bd:serviceParam wikibase:language "ar". }
    #     ?place rdfs:label ?enLabel.
    #     FILTER((LANG(?enLabel)) = "en")
    #     OPTIONAL { ?place wdt:P625 ?coordinates. }
    #     ?place wdt:P31 ?type.
    #     ?type rdfs:label ?typeLabel.
    #     FILTER(lang(?typeLabel) = "en").
    # }
    # """
    # logger.info(query)
    places = []
    results = SPARQL.queryAndConvert()

    try:
        for result in results["results"]["bindings"]:
            place = LINK_PATTERN.sub('', result['place']['value'])
            en_label = result['enLabel']['value']
            coordinates = result['coordinates']['value'] if 'coordinates' in result else nan
            place_label = result['placeLabel']['value']
            place_alt_labels = result['placeAltLabel']['value'] if 'placeAltLabel' in result else nan
            type_label = result['typeLabel']['value']
            places.append(Place(place, en_label, place_label, place_alt_labels, coordinates, type_label, idx))
        df = pd.DataFrame([place.__dict__ for place in places])
        df.to_csv('results/wikidata/query_results/' + path + '_settlements.csv', index=False)
    except Exception or ValueError as e:
        logger.error(e)


def request_holy_sites_in_country(idx, path):
    start = """    SELECT DISTINCT ?place ?enLabel ?placeLabel ?placeAltLabel ?typeLabel ?coordinates WHERE {"""
    middle = f"        ?place wdt:P17 wd:{idx};"
    end = """            (wdt:P31/(wdt:P279*)) wd:Q105889895.
            SERVICE wikibase:label { bd:serviceParam wikibase:language "ar". }
            ?place rdfs:label ?enLabel.
            FILTER((LANG(?enLabel)) = "en")
            OPTIONAL { ?place wdt:P625 ?coordinates. }
            ?place wdt:P31 ?type.
            ?type rdfs:label ?typeLabel.
            FILTER(lang(?typeLabel) = "en").
        }"""
    query = '\n'.join([start, middle, end])
    SPARQL.setQuery(query)
    # SPARQL.setQuery("""
    # SELECT DISTINCT ?place ?enLabel ?placeLabel ?placeAltLabel ?typeLabel ?coordinates WHERE {
    #     ?place wdt:P17 wd:Q851;
    #         (wdt:P31/(wdt:P279*)) wd:Q105889895.
    #     SERVICE wikibase:label { bd:serviceParam wikibase:language "ar". }
    #     ?place rdfs:label ?enLabel.
    #     FILTER((LANG(?enLabel)) = "en")
    #     OPTIONAL { ?place wdt:P625 ?coordinates. }
    #     ?place wdt:P31 ?type.
    #     ?type rdfs:label ?typeLabel.
    #     FILTER(lang(?typeLabel) = "en").
    # }
    # """)
    places = []
    results = SPARQL.queryAndConvert()

    try:
        for result in results["results"]["bindings"]:
            place = LINK_PATTERN.sub('', result['place']['value'])
            en_label = result['enLabel']['value']
            coordinates = result['coordinates']['value'] if 'coordinates' in result else nan
            place_label = result['placeLabel']['value']
            place_alt_labels = result['placeAltLabel']['value'] if 'placeAltLabel' in result else nan
            type_label = result['typeLabel']['value']
            places.append(Place(place, en_label, place_label, place_alt_labels, coordinates, type_label, idx))
        df = pd.DataFrame([place.__dict__ for place in places])
        df.to_csv('results/wikidata/query_results/' + path + '_holy_sites.csv', index=False)
    except Exception or ValueError as e:
        logger.error(e)


def get_topomyms(row):
    row['toponyms'].append(row['placeLabel'])
    return '، '.join([toponym for toponym in row['toponyms'] if toponym])


def build_wikidata_gazetteer():
    places = []
    for file in glob('results/wikidata/query_results/*_settlements.csv'):
        try:
            places.append(pd.read_csv(file, converters={'placeAltLabel': lambda x: x.split('، ')}))
        except pandas.errors.EmptyDataError as e:
            logger.info(f'{file}')
            logger.error(e)

    for file in glob('results/wikidata/query_results/*_holy_sites.csv'):
        try:
            places.append(pd.read_csv(file, converters={'placeAltLabel': lambda x: x.split('، ')}))
        except pandas.errors.EmptyDataError as e:
            logger.info(f'{file}')
            logger.error(e)
    wikidata = pd.concat(places, ignore_index=True)
    wikidata = wikidata[~wikidata['typeLabel'].str.fullmatch('Wikimedia disambiguation page', na=False)]
    wikidata = wikidata[~wikidata['placeLabel'].str.contains('\d', na=False)]
    wikidata.dropna(subset='coordinates', inplace=True)

    wikidata.rename(columns={'placeAltLabel': 'toponyms', 'coordinates': 'geometry'}, inplace=True)
    wikidata['placeLabel'] = wikidata['placeLabel'].apply(normalize_text)
    wikidata['toponyms'] = wikidata['toponyms'].apply(lambda topos: [normalize_text(t) for t in topos])
    wikidata['toponyms'] = wikidata[['placeLabel', 'toponyms']].apply(get_topomyms, axis=1)

    wikidata.drop_duplicates(subset=['place'], inplace=True)

    wikidata.to_csv('results/wikidata/wikidata_gazetteer.csv', index=False, quoting=QUOTE_ALL)


def build_gazetteer():
    # request_countries_within_radius()
    # df = pd.read_csv('results/wikidata/Around_Mecca.csv', index_col='country')
    # missed = pd.Index(['Q29', 'Q41', 'Q142', 'Q38', 'Q36', 'Q184', 'Q184', 'Q31', 'Q117', 'Q810', 'Q34', 'Q233', 'Q32', 'Q39', 'Q219', 'Q191', 'Q37', 'Q40', 'Q43', 'Q211', 'Q183', 'Q218', 'Q954', 'Q668', 'Q236', 'Q794', 'Q227', 'Q219060'])
    # df = df.loc[df.index.isin(missed)]
    # print(df.index)
    # for idx, row in df.iterrows():
    #     try:
    #         print(f'{idx, row["enLabel"]}, settlements')
    #         request_settlements_in_country(idx, row['enLabel'])
    #         sleep(60)
    #         print(f'{idx, row["enLabel"]}, holy sites')
    #         request_holy_sites_in_country(idx, row['enLabel'])
    #         sleep(60)
    #     except Exception as e:
    #         logger.error(e)
    #         missed.append(idx)

    # logger.info(missed)
    build_wikidata_gazetteer()

