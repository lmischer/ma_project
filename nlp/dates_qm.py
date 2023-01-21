import re

import pandas as pd
from dask import dataframe
from numpy import nan, isnan

from helper.methods import preprocess_text
from helper.helpers_logging import setup_logger
from nlp.dates import DATE_PATTERN, MONTHS, ONES, TEN, HUNDRED, DAY_ONES, DAY_TEN

BRACKETS = re.compile('[\[\]]')


def subn_brackets(text_n):
    new_str, rpls = BRACKETS.subn('', text_n)
    return ' ' + new_str


def dates_qm(row):
    m = DATE_PATTERN.search(' ' + ' '.join(row['text_nt']))
    if not m:
        return False, False, False, nan, nan, nan, nan
    month = nan
    year = 0
    day = 0
    day_b, month_b, year_b = False, False, False

    if m.group('day_ones'):
        day += DAY_ONES.get(m.group('day_ones'))
    if m.group('day_ten'):
        day += DAY_TEN.get(m.group('day_ten'))

    if m.group('month'):
        month = MONTHS.get(m.group('month'))

    if m.group('ones'):
        year += ONES.get(m.group('ones'))
    if m.group('ten'):
        year += TEN.get(m.group('ten'))
    if m.group('hundred'):
        year += HUNDRED.get(m.group('hundred'))

    if day == 0:
        day = nan
    if year == 0:
        year = nan

    try:
        if pd.isna(row['day']):
            day_b = isnan(day)
        else:
            day_b = day == int(row['day'])
    except ValueError:
        day_b = nan

    try:
        if pd.isna(row['month']):
            month_b = isnan(month)
        else:
            month_b = month == int(row['month'])
    except ValueError:
        month_b = nan

    try:
        if pd.isna(row['year']):
            year_b = isnan(year)
        else:
            year_b = year == int(row['year'])
    except ValueError:
        year_b = nan

    return day_b, month_b, year_b, day, month, year, m


if __name__ == '__main__':
    logger = setup_logger('date_comp', 'results/dates/date_regex_qm.log')

    df = pd.read_json('resources/dates.json')

    df = df[df['text'].str.contains(r'\d', regex=True, na=False) == False]

    df.to_csv('resources/dates_used.csv')

    ddf = dataframe.from_pandas(df, 18)
    df['text_nt'] = ddf['text'].apply(subn_brackets, meta=('normalized', str)).compute()
    ddf = dataframe.from_pandas(df, 18)
    df['text_nt'] = ddf['text_nt'].apply(preprocess_text, meta=('normalized', str)).compute()

    ddf = dataframe.from_pandas(df, 18)
    res = ddf.apply(dates_qm, axis=1, meta=('list', object)).compute()
    df['day_bool'], df['month_bool'], df['year_bool'], df['day_match'], df['month_match'], df['year_match'], df['match'] = zip(*res)

    day_counts = df['day_bool'].fillna('nan').value_counts()
    month_counts = df['month_bool'].fillna('nan').value_counts()
    year_counts = df['year_bool'].fillna('nan').value_counts()

    logger.info('day:')
    logger.info(day_counts)
    logger.info('month:')
    logger.info(month_counts)
    logger.info('year:')
    logger.info(year_counts)

    df[df['day_bool'] == False].to_csv('results/dates/dates_pattern_qm_day.csv', index=False)
    df[df['day_bool'].isna()].to_csv('results/dates/dates_pattern_qm_day_nan.csv', index=False)
    df[df['month_bool'] == False].to_csv('results/dates/dates_pattern_qm_month.csv', index=False)
    df[df['month_bool'].isna()].to_csv('results/dates/dates_pattern_qm_month_nan.csv', index=False)
    df[df['year_bool'] == False].to_csv('results/dates/dates_pattern_qm_year.csv', index=False)
    df[df['year_bool'].isna()].to_csv('results/dates/dates_pattern_qm_year_nan.csv', index=False)
