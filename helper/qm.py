import pandas as pd
from dask import dataframe
from numpy import nan

from helper.helpers_logging import setup_logger
from helper.methods import WHICH
from helper.plotting import box_plot, histogram

logger_qm = setup_logger('qm_dates_logger', 'results/' + WHICH + '/logs/dates_qm.log')


def check_for_date_duplicates(date_list):
    if type(date_list) is float:
        return nan
    birth = [date for date in date_list if date.category == 'birth']
    death = [date for date in date_list if date.category == 'death']

    return len(birth) > 1 or len(death) > 1


def check_date_order(date_list):
    dates_iter = date_list.iter()
    next_date = next(dates_iter)
    current = False
    wrong_order = False
    while next_date:
        if current:
            wrong_order = current.year > next_date.year or (
                    current.year > 100 > next_date.year and current.year % 100 > next_date.year % 100)
        current = next_date
        next_date = next(dates_iter)

    return wrong_order


def track_entries_w_date_duplicates(df):
    ddf = dataframe.from_pandas(df[df['dates'].notna()], 18)
    df['date_duplicates'] = ddf['dates'].apply(check_for_date_duplicates, meta=('date_duplicates', bool)).compute()
    df_date_duplicates = df[df['date_duplicates'] == True]
    for idx, row in df_date_duplicates.iterrows():
        name = idx.__str__()
        with open('results/' + WHICH + '/qm/dates/date_duplicates/' + name + '.txt', 'w', encoding='utf-8') as f:
            f.write(' '.join(row['text_nt']))

        df_dates = pd.DataFrame([date.__dict__ for date in row['dates']])
        df_dates.to_csv('results/' + WHICH + '/qm/dates/date_duplicates/' + name + '_dates.csv', index=False)

    return len(df_date_duplicates)


def track_entries_wo_dates(df):
    text_len = []
    df_no_dates = df[df['dates'].isna()]
    for idx, row in df_no_dates.iterrows():
        name = idx.__str__()
        text_len.append(len(row['text_nt']))
        with open('results/' + WHICH + '/qm/dates/no_dates/' + name + '.txt', 'w', encoding='utf-8') as f:
            f.write(' '.join(row['text_nt']))

    text_len_s = pd.Series(text_len)
    logger_qm.info(f'Mean of text length from entries without dates: {text_len_s.mean()}')

    box_plot(text_len_s, 'Number of words across entries without dates',
             'Entries without Dates',
             'results/' + WHICH + '/qm/dates/average_text_len_no_dates')

    return len(df_no_dates)


def check_for_any_date_w_century(dates):
    for d in dates:
        if d.year > 100:
            return False

    return True


def track_entries_wo_century(df):
    df_dates = df[df['dates'].notna()]
    df_dates[df_dates['dates'].apply(check_for_any_date_w_century)].to_csv(
        'results/' + WHICH + '/qm/dates/entries_wo_any_century.csv')


# def track_entries_w_falsy_date_order(df):
#     df_dates = df[df['dates'].notna()]
#     for idx, row in df_dates.iterrows():
#
#         if not correct_order:
#             name = idx.__str__()
#             with open('results/' + WHICH + '/qm/date_order/' + name + '.txt', 'w', encoding='utf-8') as f:
#                 f.write(' '.join(row['text_nt']))
#
#             df_dates = pd.DataFrame([date.__dict__ for date in row['dates']])
#             df_dates.to_csv('results/' + WHICH + '/qm/date_order/' + name + '_dates.csv', index=False)
#
#
# def track_entries_w_uncertain_century(df):


def date_method_qm(df):
    s_dates = df['dates'].explode(ignore_index=True).dropna()
    df_dates = pd.DataFrame([date.__dict__ for date in s_dates])
    df_dates.to_csv('results/' + WHICH + '/dates.csv', index=False)
    df_dates[df_dates['category'].str.fullmatch('ijaza', na=False)].to_csv('results/' + WHICH + '/dates_ijaza.csv',
                                                                           index=False)
    histogram(df_dates['year'], 'results/' + WHICH + '/qm/dates/histogram_years', 180)
    histogram(df_dates.loc[df_dates['year'] < 100, 'year'], 'results/' + WHICH + '/qm/dates/histogram_years_wo_century',
              20)
    histogram(df_dates.loc[(df_dates['year'] >= 700) & (df_dates['year'] < 900), 'year'],
              'results/' + WHICH + '/qm/dates/histogram_years_w_century', 20)
    df_dates[(df_dates['year'] > 100) & (df_dates['year'] < 700)].to_csv(
        'results/' + WHICH + '/qm/dates/dates_century_below.csv', index=False)
    df_dates[df_dates['year'] > 900].to_csv('results/' + WHICH + '/qm/dates/dates_century_above.csv', index=False)

    df_dates_birth = df_dates[df_dates['category'].str.fullmatch('birth', na=False)]
    histogram(df_dates_birth.loc[df_dates_birth['year'] < 100, 'year'],
              'results/' + WHICH + '/qm/dates/histogram_birth_years_wo_century', 20)
    histogram(df_dates_birth.loc[(df_dates_birth['year'] >= 700) & (df_dates_birth['year'] < 900), 'year'],
              'results/' + WHICH + '/qm/dates/histogram_birth_years_w_century', 20)
    df_dates_death = df_dates[df_dates['category'].str.fullmatch('death', na=False)]
    histogram(df_dates_death.loc[df_dates_death['year'] < 100, 'year'],
              'results/' + WHICH + '/qm/dates/histogram_death_years_wo_century', 20)
    histogram(df_dates_death.loc[(df_dates_death['year'] >= 700) & (df_dates_death['year'] < 900), 'year'],
              'results/' + WHICH + '/qm/dates/histogram_death_years_w_century', 20)

    track_entries_wo_century(df)

    n_date_duplicates = track_entries_w_date_duplicates(df)
    n_wo_dates = track_entries_wo_dates(df)
    n_biographical_entries = len(df.index)
    stat = [('Biographical entries', n_biographical_entries), ('Entries without dates', n_wo_dates),
            ('Entries without dates percent', n_wo_dates / n_biographical_entries),
            ('Entries with death or birth duplicates', n_date_duplicates),
            ('Entries with death or birth duplicates percent', n_date_duplicates / n_biographical_entries)]

    stat_df = pd.DataFrame(stat)

    stat_df.to_csv('results/' + WHICH + '/qm/dates/date_qm_stat.csv', index=False)

    text_len = []
    for idx, row in df.iterrows():
        text_len.append(len(row['text_nt']))
    text_len_s = pd.Series(text_len)
    box_plot(text_len_s, 'Number of words across various entries',
             'All Entries',
             'results/' + WHICH + '/qm/dates/average_text_len')
