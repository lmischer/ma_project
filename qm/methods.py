import re
import pandas as pd
from os import listdir

from helper.helpers_logging import setup_logger

logger = setup_logger('qm', 'results/openiti/logs/qm_entries.log')


def get_precision_and_recall(true_positive, false_positive, false_negative):
    precision = (true_positive / (true_positive + false_positive)) * 100
    recall = (true_positive / (true_positive + false_negative)) * 100

    return precision, recall


def get_entry_counts():
    pattern = re.compile(r'\d+_controlled.csv')
    files = listdir('results/openiti/qm/samples/')
    list_controlled = [file for file in files if pattern.fullmatch(file)]
    logger.info(f'Controlled samples: {len(list_controlled)}')
    df = pd.read_csv('results/openiti/extended.csv')
    logger.info(f'Number of extracted entries: {len(df.index)}')
    logger.info(f'Ration controlled to found: {len(list_controlled) / len(df.index) * 100}')

    df_top_matched = pd.read_csv('results/openiti/gazetteer_with_value_counts.csv', index_col=0)
    df_top_unmatched = pd.read_csv('results/openiti/qm/non_matched_locs_thurayya.csv', usecols=['index', 'value_counts'], index_col='index')
    matched = len(df_top_matched.index)
    unmatched = len(df_top_unmatched.index)
    logger.info(f'How much have been enriched by Thurayya? {matched/(matched+unmatched) * 100}')

    df_top_matched = pd.read_csv('results/openiti/wikidata_with_value_counts.csv', index_col=0)
    df_top_unmatched = pd.read_csv('results/openiti/qm/non_matched_locs_wikidata.csv', usecols=['index', 'value_counts'], index_col='index')
    matched = len(df_top_matched.index)
    unmatched = len(df_top_unmatched.index)
    logger.info(f'How much have been enriched by Wikidata? {matched/(matched+unmatched) * 100}')

