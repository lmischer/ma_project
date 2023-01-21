import pandas as pd
import re
from numpy import nan

from helper.methods import WHICH
from nlp.ar_strings import AR_STR, WORD

HANKAH_PATTERN = re.compile(r'(?P<institution>خانقاه|زاوية|تكية|رباط|دير)\s(?P<name>ال' + AR_STR + r'ية)\s')
HANKAH_EXTENDED_PATTERN = re.compile(WORD +r'{1,10}\s[بوف]?(?P<institution>خانقاه|زاوية|تكية|رباط|دير)\s(?P<name>ال' + AR_STR + r')(?=' + WORD + '{0,5})')


class Institution:
    def __init__(self, category, name, context=nan):
        self.category = category
        self.name = name
        self.context = context

    def __eq__(self, other):
        return self.category == other.category and self.name == other.name

    def __str__(self):
        return str(self.category + ' ' + self.name)

    def __repr__(self):
        return str(self.__dict__)


def get_hankah(text_nt):
    if type(text_nt) is float:
        return nan

    text = ' '.join(text_nt)
    institutions = []
    institutions_extended = []

    for m in HANKAH_PATTERN.finditer(text):
        institutions.append(Institution(m.group('institution'), m.group('name')))
    for m in HANKAH_EXTENDED_PATTERN.finditer(text):
        institutions_extended.append(Institution(m.group('institution'), m.group('name'), m.group(0)))

    if not institutions:
        institutions = nan
    if not institutions_extended:
        institutions_extended = nan

    return institutions, institutions_extended


def analyse_institutions(institutions, extended):
    s_institutions = institutions.dropna().explode(ignore_index=True)
    if extended:
        df_institutions = pd.DataFrame([i.__dict__ for i in s_institutions], columns=['category', 'name', 'context'])
        df_institutions.to_csv('results/' + WHICH + '/institutions_extended.csv')

        df_value_counts = df_institutions[['category', 'name']].value_counts().to_frame().reset_index()
        df_value_counts.columns = ['category', 'name', 'count']
        df_value_counts.to_csv('results/' + WHICH + '/institutions_extended_w_value_counts.csv', index=False)
    else:
        df_institutions = pd.DataFrame([i.__dict__ for i in s_institutions], columns=['category', 'name'])

        df_value_counts = df_institutions.value_counts().to_frame().reset_index()
        df_value_counts.columns = ['category', 'name', 'count']
        df_value_counts.to_csv('results/' + WHICH + '/institutions_w_value_counts.csv', index=False)
