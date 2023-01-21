import geopandas as gpd
import glob
import pandas as pd

from helper.methods import normalize_text


def get_althurayya():
    files = glob.iglob('../althurayya.github.io/places/*.geojson')
    gdfs = (gpd.read_file(filepath) for filepath in files)
    df = pd.concat(gdfs)

    df.drop(['althurayyaData', 'sources_english', 'sources_arabic'], axis=1, inplace=True)
    df['placeLabel'] = df['cornuData'].apply(lambda x: x.get('toponym_arabic'))
    df['typeLabel'] = df['cornuData'].apply(lambda x: x.get('top_type_hom'))
    df = df[df['placeLabel'].str.contains('[ا-ي]+', regex=True)]
    df['toponyms'] = df['cornuData'].apply(lambda x: x.get('toponym_arabic_other'))
    df['placeLabel'] = df['placeLabel'].apply(normalize_text)
    df['toponyms'] = df['toponyms'].apply(normalize_text)

    df[['placeLabel', 'toponyms', 'typeLabel', 'geometry', 'cornuData']].to_csv('results/gazetteer/toponyms.csv', index=False)
