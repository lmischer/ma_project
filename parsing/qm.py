import pandas as pd
from camel_tools.tokenizers.word import simple_word_tokenize
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

from helper.methods import get_df_shamela, get_df_openITI


def get_best_parsed_book_of_shamela():
    openiti = get_df_openITI()
    openiti_value_count = openiti['volume'].value_counts()

    shamela = get_df_shamela()
    shamela_value_count = shamela['volume'].value_counts()

    df = pd.concat([shamela_value_count, openiti_value_count], axis=1)
    df['percent'] = shamela_value_count / openiti_value_count

    df.sort_index().to_csv('results/parsing/num_entries_per_vol.csv')


def align_entry(row, vectorizer):
    query_o = vectorizer.transform([' '.join(row['text_nt_openiti'])])
    query_s = vectorizer.transform([' '.join(row['text_nt_shamela'])])

    return cosine_similarity(query_o, query_s)[0][0] * 100


def get_corpus(row):
    text_o = ' '.join(row['text_nt_openiti'])
    text_s = ' '.join(row['text_nt_shamela'])

    return {'openiti': text_o, 'shamela': text_s}


def compare_shamela_openiti():
    openiti = get_df_openITI()
    openiti = openiti[openiti['volume'] == 9]
    openiti = openiti[openiti['entry_type'] != 'book_title']

    shamela = get_df_shamela()
    shamela = shamela[shamela['volume'] == 9]
    shamela = shamela[shamela['entry_type'] != 'book_title']
    # shamela = shamela[shamela['text'].notna()]

    id_comparison = openiti[openiti['book_id'].notna()].join(shamela[shamela['book_id'].notna()].set_index('book_id'), on='book_id', lsuffix='_openiti', rsuffix='_shamela')

    vectorizer = CountVectorizer(tokenizer=simple_word_tokenize, ngram_range=(1, 2))

    texts = id_comparison[['text_nt_openiti', 'text_nt_shamela']].apply(get_corpus, axis=1, result_type='reduce')
    texts_o = pd.Series([d['openiti'] for d in texts])
    texts_s = pd.Series([d['shamela'] for d in texts])
    corpus = pd.concat([texts_o, texts_s], ignore_index=True)

    vectorizer.fit(corpus)
    words = vectorizer.get_feature_names_out()
    vectorizer = CountVectorizer(tokenizer=simple_word_tokenize, ngram_range=(1, 2)).fit(words)

    id_comparison['similarity_score'] = id_comparison[['text_nt_openiti', 'text_nt_shamela']].apply(align_entry, axis=1, vectorizer=vectorizer)
    id_comparison[['book_id', 'page_openiti', 'page_shamela', 'text_nt_openiti', 'text_nt_shamela', 'similarity_score']].to_csv('results/parsing/cosine_comparison.csv')
