import re
from numpy import nan
import pandas as pd

from helper.helpers_logging import setup_logger
from parsing.entry import Entry

BIO_PATTERN = re.compile(r'###\s(?P<entry_type>\$+)\s(?:(?P<sakhawi_id>\d*))?')
PAGE_PATTERN = re.compile(r'PageV(?P<volume>\d{2})P(?P<page>\d{3})')
HEADER_END_PATTERN = re.compile(r'#META#Header#End#')
MISSSPELLING_PATTERN = re.compile(r'\s?ms\d+')
TITLE_PATTERN = re.compile(r'###\s(?P<heading>(\||=)+)')
PARAGRAPH_PATTERN = re.compile(r'#\s[^%]')
POETRY_PATTERN = re.compile(r'#\s%~%')
NEW_LINE_PATTERN = re.compile(r'~~')


def parse_openiti():
    logger_missing_pages = setup_logger('missing_pages', 'results/openiti/logs/missing_pages.log')
    logger_titles = setup_logger('titles', 'results/openiti/logs/titles.log')

    with open('resources/0902Sakhawi.DawLamic.JK003608-ara1.completed', "r") as md_file:
        lines = md_file.readlines()

    entries = []
    curr_entry = None
    volume = 1
    page = 1
    book_title = ''
    section_title = ''
    subsection_title = ''
    text = ''

    pass_header = True

    lines_iter = iter(lines)
    for line in lines_iter:
        while pass_header:
            pass_header = not HEADER_END_PATTERN.match(line)
            line = next(lines_iter)

        text_wo_tags = line

        if not POETRY_PATTERN.match(line):
            if TITLE_PATTERN.match(line):
                if curr_entry is None:
                    curr_entry = Entry(None, volume, page, book_title, section_title, subsection_title, False, False, 'book_title')
                else:
                    curr_entry.set_text(text)
                    entries.append(curr_entry)
                    text = ''
                m = TITLE_PATTERN.search(line)
                title = TITLE_PATTERN.sub('', line)
                if MISSSPELLING_PATTERN.search(title):
                    title = MISSSPELLING_PATTERN.sub('', title)
                if len(m.group('heading')) == 1:
                    book_title = title
                    section_title = ''
                    subsection_title = ''
                elif len(m.group('heading')) == 2:
                    section_title = title
                    subsection_title = ''
                elif len(m.group('heading')) == 3:
                    sub_section_title = title
                else:
                    print(f'len: {len(m.group("heading"))} {title}')
                logger_titles.info(f'{line}')
                curr_entry = Entry(None, volume, page, book_title, section_title, subsection_title, False, False, 'book_title')
            else:
                if BIO_PATTERN.match(line):
                    if curr_entry is None:
                        curr_entry = Entry(None, volume, page, book_title, section_title, subsection_title)
                    else:
                        curr_entry.set_text(text)
                        entries.append(curr_entry)
                        text = ''

                    m = BIO_PATTERN.match(line)
                    entry_type = nan
                    if not len(m.group('entry_type')):
                        pass
                    elif len(m.group('entry_type')) == 1:
                        entry_type = 'male'
                    elif len(m.group('entry_type')) == 2:
                        entry_type = 'female'
                    elif len(m.group('entry_type')) == 3:
                        entry_type = 'alt'
                    else:
                        entry_type = 'ups'

                    book_id = int(m.group('sakhawi_id')) if m.group('sakhawi_id') else nan
                    curr_entry = Entry(book_id, volume, page, book_title, section_title, subsection_title, False, False, entry_type)

                    text_wo_tags = BIO_PATTERN.sub('', line)

                if PARAGRAPH_PATTERN.match(text_wo_tags):
                    if not len(text):
                        text_wo_tags = PARAGRAPH_PATTERN.sub('', text_wo_tags)
                    else:
                        text_wo_tags = '\n' + PARAGRAPH_PATTERN.sub('', text_wo_tags)

                if NEW_LINE_PATTERN.match(text_wo_tags):
                    text_wo_tags = NEW_LINE_PATTERN.sub('', text_wo_tags)

                if PAGE_PATTERN.search(text_wo_tags):
                    m = PAGE_PATTERN.search(text_wo_tags)
                    old_volume = volume
                    volume = int(m.group('volume'))
                    old_page = page
                    page = int(m.group('page'))
                    if old_page + 1 != page and old_volume == volume:
                        logger_missing_pages.info(f'Volume: {volume}, old page: {old_page}, new page: {page}')

                    text_wo_tags = PAGE_PATTERN.sub('', text_wo_tags)

                if MISSSPELLING_PATTERN.search(text_wo_tags):
                    text_wo_tags = MISSSPELLING_PATTERN.sub('', text_wo_tags)

                text = text + ' ' + text_wo_tags

    print(len(entries))
    df_entries = pd.DataFrame([entry.__dict__ for entry in entries])
    df_entries.to_csv('results/openiti/entries.csv', index=False)
