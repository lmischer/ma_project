import re

from arabic_reshaper import reshape
from numpy import nan
import pandas as pd
from camel_tools import ner
from camel_tools.disambig.mle import MLEDisambiguator
from camel_tools.tagger.default import DefaultTagger
from camel_tools.tokenizers.morphological import MorphologicalTokenizer
from camel_tools.ner import NERecognizer
from camel_tools.morphology.database import MorphologyDB
from camel_tools.morphology.analyzer import Analyzer
from pyLDAvis import save_html
from pyLDAvis.sklearn import prepare
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from wordcloud import WordCloud

from helper.helpers_logging import setup_logger
from helper.methods import WHICH

db = MorphologyDB.builtin_db()
analyzer = Analyzer(db)
mle = MLEDisambiguator.pretrained('calima-msa-r13')
tokenizer = MorphologicalTokenizer(mle, scheme='d2tok')
tagger = DefaultTagger(mle, 'd2tok')
ner = NERecognizer('CAMeL-Lab/bert-base-arabic-camelbert-ca-ner').pretrained()
PREFIX_PATTERN = re.compile('[بلوف]\+')
STOPWORDS = open('resources/stop_words.txt', 'r', encoding='utf-8').read().splitlines()

logger_locs_split = setup_logger('locs_split', 'results/' + WHICH + '/logs/locs_split.log')
logger_pers_split = setup_logger('pers_split', 'results/' + WHICH + '/logs/pers_split.log')


def preprocess_locs(locs):
    locs_nt = []
    for elem in locs:
        parts = []
        for token in elem:
            parts.extend(token.split('_'))
        locs_nt.append(' '.join([x for x in parts if not re.match(PREFIX_PATTERN, x)]))

    logger_locs_split.info(f'elems: {locs}\nlocs: {locs_nt}\n\n\n\n\n')
    return locs_nt


def preprocess_pers(pers):
    pers_nt = []
    for elem in pers:
        parts = []
        for token in elem:
            parts.extend(token.split('_'))
        pers_nt.append(' '.join([x for x in parts if not re.match(PREFIX_PATTERN, x)]))

    logger_pers_split.info(f'elems: {pers}\nlocs: {pers_nt}\n\n\n\n\n')
    return pers_nt


def preprocess_ne(nes):
    nes_nt = []
    for elem in nes:
        parts = []
        for token in elem:
            parts.extend(token.split('_'))
        nes_nt.append(' '.join([x for x in parts if not re.match(PREFIX_PATTERN, x)]))

    return nes


def ner_analysis(row):
    token_list = row['token_list']
    if any([arg is nan for arg in token_list]):
        return nan, nan, nan, nan
    locs, misc, orgs, pers, curr, tok = [], [], [], [], [], []
    prev = None

    for token, tokenized, lemma, label, pos in token_list:
        if label[0] == 'B' or label == 'O':
            if prev and prev != 'O':
                if prev[2] == 'L':
                    locs.append(tok)
                elif prev[2] == 'M':
                    misc.append(tok)
                elif prev[2] == 'O':
                    orgs.append(tok)
                elif prev[2] == 'P':
                    pers.append(tok)
                else:
                    misc.append(tok)

            if label == 'O':
                tok = None
            else:
                tok = [tokenized]
        elif label[0] == 'I':
            try:
                tok.append(tokenized)
            except AttributeError as e:
                tok = [tokenized]
                fail = pd.DataFrame(list(token_list), columns=['token', 'tokenized', 'lemma', 'label', 'pos'])
                fail.to_csv('results/ner_qm/' + str(row.name) + '.csv', index=False)

        prev = label

    locs = preprocess_locs(locs)
    misc = preprocess_ne(misc)
    orgs = preprocess_ne(orgs)
    pers = preprocess_pers(pers)

    if not locs:
        locs = nan
    if not misc:
        misc = nan
    if not orgs:
        orgs = nan
    if not pers:
        pers = nan

    return locs, misc, orgs, pers


def get_lemmas(text_nt):
    disambig = mle.disambiguate(text_nt)
    tuple_list = [(d.analyses[0].analysis['lex'], d.analyses[0].analysis['pos']) for d in disambig]
    if tuple_list:
        return zip(*tuple_list)
    else:
        return nan, nan


def get_ne_labels(text_nt):
    return ner.predict_sentence(text_nt)


def get_tokens(text_nt):
    return tokenizer.tokenize(text_nt)


def get_token_list(text_nt):
    if type(text_nt) is float:
        return [nan, nan, nan, nan, nan]
    tokenized = get_tokens(text_nt)
    lemmas, pos = get_lemmas(text_nt)
    ne_labels = get_ne_labels(text_nt)
    return list(zip(text_nt, tokenized, lemmas, ne_labels, pos))


def prep_word_frequencies(token_list):
    if any([arg is nan for arg in token_list]):
        return nan
    return [lemma for token, tokenized, lemma, ne_label, pos in token_list if ne_label == 'O' and pos != 'punc']


def topic_modeling(lemmas):
    vectorizer = CountVectorizer()
    tf_matrix = vectorizer.fit_transform(lemmas)
    lda = LatentDirichletAllocation(n_components=5, max_iter=10, learning_method='online', verbose=True)
    data_lda = lda.fit_transform(tf_matrix)

    for idx, topic in enumerate(lda.components_):
        print(f'Topic {idx}')
        print([(vectorizer.get_feature_names_out()[i], topic[i]) for i in topic.argsort()[:-10 - 1:-1]])

    return lda, tf_matrix, vectorizer


def model_topics(lemmas_series):
    lemmas = lemmas_series.explode(ignore_index=True).dropna().to_list()

    lda, tf_matrix, vectorizer = topic_modeling(lemmas)

    p = prepare(lda, tf_matrix, vectorizer, mds='tsne')
    save_html(p, 'results/' + WHICH + '/lda.html')

    text = reshape(' '.join(lemmas))
    WordCloud(font_path='resources/IBMPlexArabic_Regular.otf', scale=10).generate(text).to_file(
        'results/' + WHICH + '/wordcloud.png')
