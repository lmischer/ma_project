from numpy import nan


class Entry:
    def __init__(self, book_id, volume, page, book_title, section_title, subsection_title, corrected_id=False, part_of_multi_ids=False, entry_type=nan):
        self.book_id = book_id
        self.text = None
        self.volume = volume
        self.page = page
        self.book_title = book_title
        self.section_title = section_title
        self.subsection_title = subsection_title
        self.corrected_id = corrected_id
        self.part_of_multi_ids = part_of_multi_ids
        self.entry_type = entry_type    # male, female, alt, ups, book_title

    def correct_id(self, book_id):
        self.book_id = book_id
        self.corrected_id = True

    def is_part_of_multi_ids(self):
        self.part_of_multi_ids = True

    def set_text(self, text):
        self.text = text


class Title:
    def __init__(self, text, part, page, t='section'):
        self.text = text
        self.part = part
        self.page = page
        self.t = t
