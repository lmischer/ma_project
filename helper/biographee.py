from numpy import nan


class Biographee:

    def __init__(self):
        # name parts referring to ancestors
        self.ism = nan  # name
        self.iab = nan  # name father
        self.gad = nan  # name grandfather
        self.abg = nan  # name great-grandfather
        self.gag = nan  # name great-great-grandfather

        # name parts referring to honorific titles or the person is commonly known as
        self.suh = nan  # commonly known under
        self.laq = nan  # honorific name with religion or state
        self.lgb = nan  # honorific title
        self.kun = nan  # name referring to known children/relatives

        # name parts referring to religion or school of jurisprudence
        self.din = nan

        # name parts referring to a relation with an ethnicity, tribe, ancestors, place, physics
        self.nsb = nan

        # name parts referring to profession
        self.swm = nan

        # Dates
        self.wld = nan  # birth
        self.mat = nan  # death
        self.trh = nan  # other events

        # Locations
        self.hwl = nan  # Birth
        self.hmt = nan  # Death
        self.hal = nan  # Place of residence
        self.haq = nan  # Place of descendents

        # Transmission of knowledge
        self.min = nan  # lecturers
        self.ila = nan  # disciples

        # Bibliographic Reference as tuple (volume, page)
        self.ref = nan

