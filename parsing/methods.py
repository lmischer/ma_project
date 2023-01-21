import math
import copy
import re
from parsing.entry import Entry


def check_id(prev_id, curr_id):
    # correct typos of IDs like messed up digits
    corrected = False
    if prev_id and curr_id != prev_id + 1:
        if curr_id == math.floor(prev_id / 10) or (
                prev_id % 10 == 9 and curr_id == math.floor(prev_id + 1)) or (prev_id + 1) % 100 == curr_id:
            curr_id = prev_id + 1
            corrected = True
    return curr_id, corrected


def check_text_for_ids(entry, previous_id):
    # ID was not tagged with span but was written in the text
    total_text = copy.copy(entry.text)
    split_entries = []
    prev_id = previous_id
    if re.search('\d+', total_text):
        text_part = copy.deepcopy(entry)
        text_part.is_part_of_multi_ids()
        end = 0
        for m in re.finditer('\d+', total_text):
            text_part.set_text(total_text[end:m.start()])
            split_entries.append(text_part)
            end = m.end() + 1

            curr_id, corrected = check_id(prev_id, int(m.group(0)))
            text_part = Entry(curr_id, entry.volume, entry.page, entry.book_title, entry.section_title, corrected, True)
            prev_id = curr_id
        text_part.set_text(total_text[end:])
        split_entries.append(text_part)

    return split_entries, prev_id

