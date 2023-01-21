import glob
import pandas as pd
from numpy import nan
from camel_tools.tokenizers.word import simple_word_tokenize
from camel_tools.utils.dediac import dediac_ar
from camel_tools.utils.normalize import normalize_unicode, normalize_alef_ar
from dask import dataframe
import re

WHICH = 'openiti' # 'shamela'
TAKE_SAMPLES = False

SLASH_PATTERN = re.compile(r'[/\\()[\]{}|]')
NEWLINE_PATTERN = re.compile(r'\n')


def normalize_text(text):
    nt_text = dediac_ar(text)
    nt_text = normalize_unicode(nt_text)
    nt_text = normalize_alef_ar(nt_text)
    nt_text, n = NEWLINE_PATTERN.subn(' ', nt_text)
    nt_text, n = SLASH_PATTERN.subn('', nt_text)
    return nt_text


def preprocess_text(text):
    if type(text) is float:
        return nan
    n_text = normalize_text(text)
    tokens = simple_word_tokenize(n_text)
    if tokens:
        return tokens
    else:
        return nan


def get_dummy_df():
    texts = []

    for file in glob.glob('test_bios/*.txt'):
        with open(file, 'r') as f:
            texts.append(f.read())

    df = pd.DataFrame(data=texts, columns=['text'])
    df['text_nt'] = df['text'].apply(preprocess_text)

    return df


def get_df_openITI():
    df = pd.read_csv('results/openiti/entries.csv')#[6000:6030]
    ddf = dataframe.from_pandas(df, 18)

    df['text_nt'] = ddf['text'].apply(preprocess_text, meta=('text_nt', object)).compute()

    return df


def get_df_shamela():
    df = pd.read_csv('results/shamela/entries.csv')#[6000:6030]
    ddf = dataframe.from_pandas(df, 18)

    df['text_nt'] = ddf['text'].apply(preprocess_text, meta=('text_nt', object)).compute()

    return df


def get_regex(words):
    return re.compile('|'.join(words))


def get_df_thurayya():
    df = pd.read_csv('results/gazetteer/toponyms_extended.csv', converters={'toponyms': lambda x: x.split('، ')})
    ddf = dataframe.from_pandas(df, 18)
    df['toponyms_pattern'] = ddf['toponyms'].apply(get_regex, meta=('toponyms_pattern', object)).compute()
    return df.drop(columns=['cornuData'])


def get_df_wikidata():
    df = pd.read_csv('results/wikidata/wikidata_gazetteer.csv', converters={'toponyms': lambda x: x.split('، ')},
                     index_col='place')
    ddf = dataframe.from_pandas(df, 18)
    df['toponyms_pattern'] = ddf['toponyms'].apply(get_regex, meta=('toponyms_pattern', object)).compute()
    df.drop(columns='enLabel', inplace=True)
    return df[~df['geometry'].str.match('http://www.wikidata.org/.well-known/genid')]


def get_titles_and_names(row):
    if type(row['text_nt']) is float:
        row['name'] = nan
        return row[['name', 'book_title', 'section_title']]
    else:
        name = []
        for token, lemma, ne_label, pos in row['token_list']:
            if pos != 'punc':
                name.append(token)
            if len(name) == 2:
                break

        row['name'] = ' '.join(name)
        return row[['name', 'book_title', 'section_title']]


def take_samples(df):
    indices = pd.read_csv('resources/sample_indices.csv', index_col='samples')

    # for idx, row in df.sample(n=200).iterrows():
    for idx, row in df.loc[df.index.isin(indices.index)].iterrows():
        name = idx.__str__()

        df_analysis = pd.DataFrame([(t, tokenized, l, ne_l, pos) for t, tokenized, l, ne_l, pos in row['token_list']],
                                   columns=['token', 'tokenized', 'lemma', 'ne_label', 'pos'])
        df_analysis.to_csv('results/' + WHICH + '/qm/samples/' + name + '.csv', index=False)

        with open('results/' + WHICH + '/qm/samples/' + name + '.txt', 'w', encoding='utf-8') as f:
            f.write(' '.join(row['text_nt']))
            f.write('\n')

        if type(row['locs']) is not float:
            df_locs = pd.DataFrame(row['locs'])
            df_locs.to_csv('results/' + WHICH + '/qm/samples/' + name + '_locs.csv', index=False)

        if type(row['institutions']) is not float:
            df_institutions = pd.DataFrame([ins for ins in row['institutions']])
            df_institutions.to_csv('results/' + WHICH + '/qm/samples/' + name + '_institutions.csv', index=False)

        if type(row['pers']) is not float:
            df_pers = pd.DataFrame(row['pers'])
            df_pers.to_csv('results/' + WHICH + '/qm/samples/' + name + '_pers.csv', index=False)

        if type(row['misc']) is not float:
            df_pers = pd.DataFrame(row['misc'])
            df_pers.to_csv('results/' + WHICH + '/qm/samples/' + name + '_misc.csv', index=False)

        if type(row['orgs']) is not float:
            df_pers = pd.DataFrame(row['orgs'])
            df_pers.to_csv('results/' + WHICH + '/qm/samples/' + name + '_orgs.csv', index=False)

        if type(row['dates']) is not float:
            df_dates = pd.DataFrame([date.__dict__ for date in row['dates']])
            df_dates.to_csv('results/' + WHICH + '/qm/samples/' + name + '_dates.csv', index=False)
