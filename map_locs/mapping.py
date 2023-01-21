from collections import Counter

from numpy import nan
import geopandas as gpd
import folium

from shapely import wkt
import matplotlib.pyplot as plt

from helper.helpers_logging import setup_logger
from helper.methods import WHICH

logger_locs_mapped_qm = setup_logger('qm_locs_mapped_qm_logger', 'results/' + WHICH + '/logs/locs_mapped_qm.log')


def match_locs(row, locs_counted):
    matched = []
    for loc in locs_counted.keys():
        if row['toponyms_pattern'].fullmatch(loc):
            for _ in range(locs_counted.get(loc)):
                matched.append((loc, row.name))
    if len(matched):
        return matched
    else:
        return nan


def get_topos_indices(locs, df_topos):
    if type(locs) is float:
        return nan, nan

    locs_counted = Counter(locs)

    matches = df_topos.apply(match_locs, axis=1, locs_counted=locs_counted)
    matches = matches.explode(ignore_index=True).dropna().to_list()
    if not len(matches):
        return nan, nan
    return zip(*matches)


def get_non_matched_locs(row, key_all, key_matched):
    if type(row[key_all]) is float:
        return nan
    if type(row[key_matched]) is float:
        return row[key_all]

    s_matched = set(row[key_matched])
    unmatched = [loc for loc in row[key_all] if loc not in s_matched]

    if not len(unmatched):
        return nan

    return unmatched


def map_locs(locs_series, df_topos, where):
    locs_series.name = 'value_counts'
    locs_series = locs_series.explode(ignore_index=True)

    df_value_counts = locs_series.value_counts().to_frame()

    logger_locs_mapped_qm.info(f'{where}, {locs_series[:10]}')

    df_topos = df_topos.merge(df_value_counts, how='left', left_index=True, right_index=True)

    df_topos[['geometry', 'toponyms', 'value_counts']].sort_values(by='value_counts', ascending=False).to_csv(
        'results/' + WHICH + '/' + where + '_with_value_counts.csv')
    df_topos[['geometry', 'toponyms', 'value_counts']].sort_values(by='value_counts', ascending=False)[0:10].to_csv(
        'results/' + WHICH + '/' + where + '_with_value_counts_top.csv')
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    df_topos['value_counts'].plot.box(ax=ax, showfliers=False)
    ax.set_title('How often is a place matched?')
    fig.savefig('results/' + WHICH + '/qm/locs_mapped_' + where + '_boxplot_wo_fliers.png')
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    df_topos['value_counts'].plot.box(ax=ax)
    ax.set_title('How often is a place matched?')
    fig.savefig('results/' + WHICH + '/qm/locs_mapped_' + where + '_boxplot_w_fliers.png')
    logger_locs_mapped_qm.info(f'mean, how often is a place matched on average: {df_topos["value_counts"].mean()} {where}')

    df_topos.dropna(inplace=True)
    df_topos.drop('toponyms_pattern', axis=1, inplace=True)
    df_topos['geometry'] = df_topos['geometry'].apply(wkt.loads)
    df_topos = gpd.GeoDataFrame(df_topos)

    if not df_topos.empty:
        generate_map(df_topos, where)

    return df_topos


def generate_map(df_topos, where):
    tileurl = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Physical_Map/MapServer/tile/{z}/{y}/{x}'
    m = folium.Map(location=[33.333333, 44.383333],  # center of the folium map
                   tiles=tileurl,  # type of map
                   attr='Esri',
                   min_zoom=3, max_zoom=12,  # zoom range
                   zoom_start=4)  # initial zoom

    m = df_topos.explore('value_counts', m=m, tooltip='toponyms', popup=True)
    m.save('results/' + WHICH + '/' + where + '_map.html')
