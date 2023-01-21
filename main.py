import pandas as pd
from dask import dataframe

from helper.methods import get_df_openITI, get_df_shamela, get_titles_and_names, WHICH, TAKE_SAMPLES, take_samples, \
    get_df_thurayya, get_df_wikidata
from helper.plotting import pie
from helper.qm import date_method_qm
from helper.helpers_logging import setup_logger
from map_locs.mapping import get_topos_indices, map_locs, get_non_matched_locs, generate_map
from map_locs.mapping_qm import locs_matching_qm, locs_missing_qm
from nlp.dates import get_dates, get_date_category_stat
from nlp.institutions import get_hankah, analyse_institutions
from nlp.methods import get_token_list, ner_analysis, prep_word_frequencies, model_topics

NO_NAMES = ['سقط', 'انْتهى التّكْرَار', 'وَاذْكُر أَخا عَاد']
NAMES = []
NAME_INDICATORS_PRE = ['أَبو', 'ابن', 'أخو', 'أمة', 'إمام', 'أمير', 'خادم', 'خال', 'خطيب', 'رئيس', 'زين', 'سبط', 'شيخ',
                       'صاحب', 'قاصد', 'نقيب', 'والي', 'ولي', 'نائب']
NAME_INDICATORS_SUF = ['الدين', 'بلله']

logger = setup_logger('main_logger', 'results/' + WHICH + '/logs/main.log')
logger_date_regex = setup_logger('date_comp', 'results/' + WHICH + '/logs/date_regex_qm.log')

if __name__ == '__main__':
    if WHICH == 'shamela':
        df = get_df_shamela()
    elif WHICH == 'openiti':
        df = get_df_openITI()
        # Gets collective entries
        df_collective_entries = df[df['entry_type'].str.contains('alt', regex=False, na=False)]
        df_collective_entries.to_csv('results/' + WHICH + '/collective_entries.csv')
        # Gets entries of single biographee
        # df = df[df['entry_type'].str.contains('male', regex=False, na=False)]  # Gets male and female
    df = df[df['text_nt'].notna()]
    logger.info(f'Entries analysed: {len(df.index)}')
    df_thurayya = get_df_thurayya()
    df_wikidata = get_df_wikidata()

    ddf = dataframe.from_pandas(df, 18)
    df['token_list'] = ddf['text_nt'].apply(get_token_list, meta=('token_list', object)).compute()

    ddf = dataframe.from_pandas(df, 18)
    df['dates'] = ddf['text_nt'].apply(get_dates, meta=('dates', object)).compute()

    date_category_stat_df = pd.DataFrame(get_date_category_stat(), columns=['event_type', 'count']).set_index('event_type')
    date_category_stat_df.sort_values(by='count', ascending=False, inplace=True)
    date_category_stat_df.to_csv('results/' + WHICH + '/date_category_stat.csv')
    pie(date_category_stat_df, 'results/' + WHICH + '/date_category_stat', 'count')

    ddf = dataframe.from_pandas(df, 18)
    res = ddf[['token_list']].apply(ner_analysis, axis=1, meta=('list', object)).compute()
    df['locs'], df['misc'], df['orgs'], df['pers'] = zip(*res)
    ddf = dataframe.from_pandas(df, 18)
    res = ddf['text_nt'].apply(get_hankah, meta=('list', object)).compute()
    df['institutions'], df['institutions_extended'] = zip(*res)
    ddf = dataframe.from_pandas(df, 18)
    res = ddf['locs'].apply(get_topos_indices, meta=('list', object), df_topos=df_thurayya).compute()
    df['locs_matched_thurayya'], df['thurayya_indices'] = zip(*res)
    ddf = dataframe.from_pandas(df, 18)
    df['locs_not_matched_thurayya'] = ddf[['locs', 'locs_matched_thurayya']].apply(
        get_non_matched_locs,
        axis=1,
        meta=('list', object),
        key_all='locs',
        key_matched='locs_matched_thurayya'
    ).compute()
    ddf = dataframe.from_pandas(df, 18)
    res = ddf['locs_not_matched_thurayya'].apply(
        get_topos_indices,
        meta=('list', object),
        df_topos=df_wikidata
    ).compute()
    df['locs_matched_wikidata'], df['wikidata_indices'] = zip(*res)
    ddf = dataframe.from_pandas(df, 18)
    df['locs_not_matched_wikidata'] = ddf[['locs_not_matched_thurayya', 'locs_matched_wikidata']].apply(
        get_non_matched_locs,
        axis=1,
        meta=('list', object),
        key_all='locs_matched_wikidata',
        key_matched='locs_not_matched_thurayya'
    ).compute()

    df.to_csv('results/' + WHICH + '/extended.csv')

    analyse_institutions(df['institutions'], False)
    analyse_institutions(df['institutions_extended'], True)

    date_method_qm(df)

    df_mapping_thurayya = map_locs(df['thurayya_indices'], df_thurayya, 'gazetteer')
    df_mapping_wikidata = map_locs(df['wikidata_indices'], df_wikidata, 'wikidata')
    df_mapping_all = pd.concat([df_mapping_thurayya, df_mapping_wikidata], ignore_index=True)
    generate_map(df_mapping_all, 'total')

    ddf = dataframe.from_pandas(df, 18)
    res = ddf[['locs', 'locs_matched_thurayya']].apply(
        locs_matching_qm,
        axis=1,
        meta=('list', object),
        key_all='locs',
        key_matched='locs_matched_thurayya'
    ).compute()
    num_matched_locs, num_missed_locs, percent_missed_locs, missed = zip(*res)
    locs_missing_qm(num_matched_locs, num_missed_locs, percent_missed_locs, missed, 'thurayya')
    ddf = dataframe.from_pandas(df, 18)
    res = ddf[['locs_not_matched_thurayya', 'locs_matched_wikidata']].apply(
        locs_matching_qm,
        axis=1,
        meta=('list', object),
        key_all='locs_not_matched_thurayya',
        key_matched='locs_matched_wikidata'
    ).compute()
    num_matched_locs, num_missed_locs, percent_missed_locs, missed = zip(*res)
    locs_missing_qm(num_matched_locs, num_missed_locs, percent_missed_locs, missed, 'wikidata')

    ddf = dataframe.from_pandas(df, 18)
    res = ddf['token_list'].apply(prep_word_frequencies, meta=('frequencies', object)).compute()
    res.dropna(inplace=True)
    model_topics(res)

    if TAKE_SAMPLES:
        take_samples(df)

    # ddf = dataframe.from_pandas(df, 18)
    # meta = pd.DataFrame(columns=['name', 'book_title', 'section_title'], dtype=object)
    # df_names = ddf.apply(get_titles_and_names, axis=1, meta=meta).compute()
    # df_names.drop_duplicates(inplace=True)

    # df_names.to_csv('results/' + WHICH + '/titles_and_names.csv', index=False)
