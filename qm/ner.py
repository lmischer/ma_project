import re
from glob import iglob
import pandas as pd
from camel_tools.tokenizers.word import simple_word_tokenize
from numpy import isnan
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from helper.methods import WHICH
from helper.helpers_logging import setup_logger
from qm.methods import get_precision_and_recall

logger = setup_logger('qm', 'results/' + WHICH + '/logs/qm_assessment.log')
logger_files = setup_logger('files', 'results/' + WHICH + '/logs/qm_files.log')
logger_dates = setup_logger('dates', 'results/' + WHICH + '/logs/qm_dates.log')

IDX_PATTERN = re.compile(r'[^0-9]*(?P<idx>\d+)')
COLUMNS = ['Automated', 'Manually', 'Truth']


def get_dates():
    files_controlled = iglob('results/' + WHICH + '/qm/samples/*_dates_controlled.csv')
    df_master = []
    for filepath in files_controlled:
        idx = IDX_PATTERN.match(filepath).group('idx')
        df_controlled = pd.read_csv(filepath)
        if len(df_controlled.columns) < 8:
            logger_dates.info(f'{idx}')
        else:
            try:
                df = pd.read_csv('results/' + WHICH + '/qm/samples/' + idx + '_dates_controlled.csv')
                df = df.join(df_controlled, rsuffix='_controlled').drop(columns=['match', 'match_controlled'])
                df_master.append(df)
            except FileNotFoundError as e:
                pass
    return pd.concat(df_master)


def get_ner_controlled(what):
    files = iglob('results/' + WHICH + '/qm/samples/*_' + what + '_controlled.csv')
    dfs = []
    if what != 'dates':
        for filepath in files:
            df = pd.read_csv(filepath)
            if len(df.columns) != 3 or len([i for i, j in zip(df.columns, COLUMNS) if i != j]) > 0:
                logger_files.info(f'{what}: {IDX_PATTERN.match(filepath).group(0)}')
            else:
                dfs.append(df)
    else:
        for filepath in files:
            df = pd.read_csv(filepath)
            if len(df.columns) < 8:
                logger_files.info(f'{what}: {IDX_PATTERN.match(filepath).group(0)}')
            else:
                dfs.append(df)
    return pd.concat(dfs)


def evaluate_dates():
    df = get_ner_controlled('dates')
    df_truth_value_counts = df['Truth'].value_counts()
    true_positive = df_truth_value_counts[True]
    false_positive = df_truth_value_counts[False]
    false_negative = 0

    precision, recall = get_precision_and_recall(true_positive, false_positive, false_negative)
    logger.info(f'dates: {len(df.index)}')

    return precision, recall


def get_bool_value_date(row):
    date_parts = ['day', 'month', 'year']
    bools = []
    print(f'{row}')
    for what in date_parts:
        try:
            if pd.isna(row[what + '_controlled']):
                bools.append(isnan(row[what]))
            else:
                bools.append(int(row[what]) == int(row[what + '_controlled']))
        except ValueError:
            bools.append('nan')

    bools.append(row['category_controlled'] == row['category'])
    bools.append(row['place_controlled'] == row['place'])

    return bools


def evaluate_date_parsing():
    date_parts = ['day', 'month', 'year', 'category', 'place']
    df = get_dates()
    res = df.apply(get_bool_value_date, axis=1)
    df['day_bool'], df['month_bool'], df['year_bool'], df['category_bool'], df['place_bool'] = zip(*res)
    list_pr = []

    for what in date_parts:
        df_truth_value_counts = df[what + '_bool'].value_counts()
        true_positive = df_truth_value_counts[True] if df_truth_value_counts.index.isin([True]).any() else 0
        false_positive = df_truth_value_counts[False] if df_truth_value_counts.index.isin([False]).any() else 0
        false_negative = df_truth_value_counts['nan'] if df_truth_value_counts.index.isin(['nan']).any() else 0
        precision, recall = get_precision_and_recall(true_positive, false_positive, false_negative)
        list_pr.append((what, precision, recall))

    return list_pr


def evaluate_ner():
    ner = ['locs', 'pers', 'misc', 'orgs']
    results = []
    for what in ner:
        print(what)
        df = get_ner_controlled(what)
        df['Manually'] = df['Manually'].notna()
        df['Automated'] = df['Automated'].notna()
        df_truth_value_counts = df.value_counts()
        true_positive = df_truth_value_counts[True, True, True] if df_truth_value_counts.index.isin(
            [(True, True, True)]).any() else 0
        false_positive = df_truth_value_counts[True, False, False] if df_truth_value_counts.index.isin(
            [(True, False, False)]).any() else 0
        false_negative = df_truth_value_counts[False, True, False] if df_truth_value_counts.index.isin(
            [(False, True, False)]).any() else 0

        precision, recall = get_precision_and_recall(true_positive, false_positive, false_negative)
        results.append((what, precision, recall))
        logger.info(f'{what}: {len(df.index)}')

    precision, recall = evaluate_dates()
    results.append(('dates', precision, recall))
    results.extend(evaluate_date_parsing())

    df = pd.DataFrame(results, columns=['what', 'precision', 'recall'])
    df.to_csv('results/' + WHICH + '/precision_and_recall.csv')


def align_names(row, vectorizer):
    query_a = vectorizer.transform([row['Automated']])
    query_m = vectorizer.transform([row['Manually']])

    return cosine_similarity(query_a, query_m)[0][0] * 100


def similarity_names():
    df = get_ner_controlled('pers')
    df = df.drop(columns='Truth')
    df.dropna(inplace=True)
    df = df[(df['Automated'].str.split().apply(lambda s: len(s) > 1)) & (df['Manually'].str.split().apply(lambda s: len(s) > 1))]

    vectorizer = CountVectorizer(tokenizer=simple_word_tokenize, ngram_range=(1, 2))
    vectorizer.fit(df['Manually'])
    words = vectorizer.get_feature_names_out()
    vectorizer = CountVectorizer(tokenizer=simple_word_tokenize, ngram_range=(1, 2)).fit(words)

    df['similarity_score'] = df[['Automated', 'Manually']].apply(align_names, axis=1, vectorizer=vectorizer)
    df.to_csv('results/' + WHICH + '/cosine_comparison_names.csv')

