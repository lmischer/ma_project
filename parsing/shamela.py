import re

import pandas as pd
import camel_tools.utils.dediac as ct
from bs4 import BeautifulSoup

from parsing.entry import Entry, Title
from parsing.methods import check_text_for_ids, check_id

from helper.helpers_logging import setup_logger

BASMALA = 'بسم الله الرحمن الرحيم'
TITLE_P = re.compile('كتاب|جزء')
MESSED_UP_SECTION_TITLE = re.compile(
    '(?P<title>\(حرف الزاي\)) (?P<text>\(زكي الدين\) بن صالح محمد بن محمد بن عبد الرحمن بن صالح والمناوي أبو بكر بن صدقة 505 \(زين الدين\) بن ابي الفضل بن القاضي عبد الله بن عبد الرحمن بن صالح المدني ممن سمع مني بها وابن محمد بن المحب بن الحسين المدني ابن عم عبد المعطي)')
NO_TITLE = re.compile(
    '\(سابق الدين\) \(سديد الدين\) \(السراج\) بن الملقن عمر بن علي بن أحمد بن محمد والسراج البلقيني عمر بن رسلان بن نصير والعبادي عمر بن حسين بن حسن وقاري الهداية عمر بن علي بن فارس والمناوي أحد نواب الحنفية عمر بن علي بن عمر والمناوي آخر تاجر اسمه')
MISSING_TITLE = '(حرف السِّين الْمُهْملَة)'

POETRY_P = re.compile('\.\.\.')
ID_P = re.compile('\d+')
ARABIC_P = re.compile('[ا-ي]+')
PAGE_SPAN_P = re.compile('الجزء:\s(?P<volume>\d+)\s\|\sالصفحة:\s(?P<page>\d+)')

START_OF_PARTS = [(1, 380), (2, 332), (3, 323), (4, 341), (5, 331), (6, 313), (7, 302), (8, 300), (9, 308), (10, 346),
                  (11, 277), (12, 168)]
MISSING_PAGES = [(5, 85), (7, 209), (7, 213), (7, 225), (7, 230), (8, 79), (8, 106), (8, 117), (8, 133), (9, 120)]


def parse_shamela():
    logger_messed_up_ids = setup_logger('messed_up_ids', 'results/shamela/logs/messed_up_ids.log')
    logger_missing_entries = setup_logger('missing_entries', 'results/shamela/logs/missing_entries.log')
    logger_missing_pages = setup_logger('missing_pages', 'results/shamela/logs/missing_pages.log')
    logger_names_in_parenthesis = setup_logger('names_in_parenthesis', 'results/shamela/logs/names_in_parenthesis.log')
    logger_titles = setup_logger('titles', 'results/shamela/logs/titles.log')
    logger_titles_and_names = setup_logger('titles_and_names', 'results/shamela/logs/titles_and_names.log')

    with open('resources/sakhawi_complete.xhtml') as addaw:
        soup = BeautifulSoup(addaw, "xml")

    body = soup.body

    entries = []
    volume = 1
    page = 4
    book_title = ''
    section_title = ''
    titles = []
    curr_entry = None
    text = ''
    prev_id = None

    for child in body.children:
        if child.name == 'div':
            for elem in child.contents:
                if elem.name == 'title':
                    logger_titles.info(f'{elem.text}\n')
                    if curr_entry is None:
                        curr_entry = Entry(None, volume, page, book_title, section_title, '', False, False, 'book_title')
                    curr_entry.set_text(text)
                    split_entries, prev_id = check_text_for_ids(curr_entry, prev_id)
                    if len(split_entries) > 0:
                        entries.extend(split_entries)
                        for entry in split_entries:
                            tokens = entry.text.split()
                            logger_titles_and_names.info(' '.join(tokens[0:10]))
                    else:
                        entries.append(curr_entry)
                        tokens = curr_entry.text.split()
                        logger_titles_and_names.info(' '.join(tokens[0:10]))
                    text = ''
                    title = ct.dediac_ar(elem.text)
                    if re.search(BASMALA, title):
                        pass
                    else:
                        if TITLE_P.search(title):
                            book_title = elem.text
                            titles.append(Title(elem.text, volume, page, 'book'))
                            logger_titles_and_names.info(f'Title:\n{elem.text}\n\n')
                        elif MESSED_UP_SECTION_TITLE.match(title):
                            m = MESSED_UP_SECTION_TITLE.match(title)
                            section_title = m.group('title')
                            text = text + ' ' + m.group('text')
                            titles.append(Title(m.group('title'), volume, page))
                            logger_titles_and_names.info(f'Title:\n{m.group("title")}\n\n')
                        elif not NO_TITLE.match(title):
                            section_title = elem.text
                            titles.append(Title(elem.text, volume, page))
                            logger_titles_and_names.info(f'Title:\n{elem.text}\n\n')
                    curr_entry = None

                elif elem.name == 'span':
                    if POETRY_P.match(elem.text):
                        text = text + ' ' + elem.text
                    elif ID_P.search(elem.text):
                        curr_id = int((ID_P.search(elem.text).group(0)))
                        corrected = False
                        if curr_entry is None:
                            curr_entry = Entry(None, volume, page, book_title, section_title, '')
                        curr_entry.set_text(text)
                        split_entries, prev_id = check_text_for_ids(curr_entry, prev_id)
                        if len(split_entries) > 0:
                            entries.extend(split_entries)
                            for entry in split_entries:
                                tokens = entry.text.split()
                                logger_titles_and_names.info(' '.join(tokens[0:10]))
                        elif ARABIC_P.search(text):
                            entries.append(curr_entry)
                            tokens = curr_entry.text.split()
                            logger_titles_and_names.info(' '.join(tokens[0:10]))

                        text = ''
                        if prev_id:
                            curr_id, corrected = check_id(prev_id, curr_id)
                        curr_entry = Entry(curr_id, volume, page, book_title, section_title, '', corrected)
                        prev_id = curr_entry.book_id

                elif elem.name is None:
                    text = text + ' ' + elem.text

                elif elem.name == 'br':
                    text = text + '\n'

        elif child.name == 'page':
            m = PAGE_SPAN_P.search(child.text)
            volume = int(m.group('volume'))
            page = int(m.group('page'))

    curr_entry.set_text(text)
    entries.append(curr_entry)

    print(len(entries))
    df_entries = pd.DataFrame([entry.__dict__ for entry in entries])

    df_entries[['book_id']] = df_entries[['book_id']].astype('Int64')
    df_entries_w_book_id = df_entries[pd.notna(df_entries['book_id'])]
    prev_row = None
    for idx, row in df_entries.iterrows():
        if prev_row is not None and pd.notna(prev_row['book_id']) and pd.notna(row['book_id']):
            try:
                next_row = df_entries_w_book_id[df_entries_w_book_id.index > idx].iloc[0]
                if prev_row['book_id'] + 1 != row['book_id']:
                    if prev_row['book_id'] + 2 == next_row['book_id']:
                        logger_messed_up_ids.info('prev: %d, curr: %d, next: %d, volume: %d, page: %d',
                                                  prev_row["book_id"], row["book_id"], next_row["book_id"], row["volume"],
                                                  row["page"])
                        df_entries.loc[idx, 'book_id'] = prev_row['book_id'] + 1
                        df_entries.loc[idx, 'corrected'] = True
                    elif prev_row['volume'] == row['volume']:
                        logger_missing_entries.info('prev: %d, curr: %d, next: %d, volume: %d, page: %d',
                                                    prev_row["book_id"], row["book_id"], next_row["book_id"],
                                                    row["volume"], row["page"])
            except IndexError:
                pass

        if pd.notna(row['book_id']):
            prev_row = df_entries.loc[idx]

    df_entries.to_csv('results/shamela/entries.csv', index=False)

    df_titles = pd.DataFrame([entry.__dict__ for entry in titles])
    df_titles.to_csv('results/shamela/titles.csv', index=False)

    for text in df_entries['text']:
        for m in re.finditer('[\(\{][^\)\.\}]*\)', text):
            logger_names_in_parenthesis.info(m.group(0))

