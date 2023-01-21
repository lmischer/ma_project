import re
from collections import Counter

from numpy import nan

from helper.helpers_logging import setup_logger
from helper.methods import WHICH
from nlp.ar_strings import WORD, AR_STR

ONES = {'واحد': 1, 'احدى': 1, 'احد': 1, 'اثنين': 2, 'اثنتين': 2, 'اثنتي': 2, 'ثلاث': 3, 'اربع': 4, 'خمس': 5, 'ست': 6,
        'سبع': 7, 'ثماني': 8, 'ثمان': 8, 'تسع': 9}
TEN = {'عشرة': 10, 'عشري': 10, 'عشر': 10, 'عشرين': 20, 'ثلاثين': 30, 'اربعين': 40, 'خمسين': 50, 'ستين': 60, 'سبعين': 70,
       'ثمانين': 80, 'تسعين': 90}
HUNDRED = {'مائة': 100, 'ماية': 100, 'مية': 100, 'مئة': 100, 'مائتين': 200, 'مايتين': 200, 'ميتين': 200,
           'ثلاثمائة': 300, 'ثلاث مائة': 300, 'اربعمائة': 400, 'اربع مائة': 400, 'خمسمائة': 500, 'خمس مائة': 500,
           'ستمائة': 600, 'ست مائة': 600, 'سبعمائة': 700, 'سبع مائة': 700, 'ثمانمائة': 800, 'ثمان مائة': 800,
           'ثمانيمائة': 800, 'ثماني مائة': 800, 'تسعمائة': 900, 'تسع مائة': 900, 'ثلاثماية': 300, 'ثلاث ماية': 300,
           'اربعماية': 400, 'اربع ماية': 400, 'خمسماية': 500, 'خمس ماية': 500, 'ستماية': 600, 'ست ماية': 600,
           'سبعماية': 700, 'سبع ماية': 700, 'ثمانماية': 800, 'ثمان ماية': 800, 'ثمانيماية': 800, 'ثماني ماية': 800,
           'تسعماية': 900, 'تسع ماية': 900, 'ثلاثمية': 300, 'ثلاث مية': 300, 'اربعمية': 400, 'اربع مية': 400,
           'خمسمية': 500, 'خمس مية': 500, 'ستمية': 600, 'ست مية': 600, 'سبعمية': 700, 'سبع مية': 700, 'ثمانمية': 800,
           'ثمان مية': 800, 'ثمانيمية': 800, 'ثماني مية': 800, 'تسعمية': 900, 'تسع مية': 900, 'ثلاثمئة': 300,
           'ثلاث مئة': 300, 'اربعمئة': 400, 'اربع مئة': 400, 'خمسمئة': 500, 'خمس مئة': 500, 'ستمئة': 600, 'ست مئة': 600,
           'سبعمئة': 700, 'سبع مئة': 700, 'ثمانمئة': 800, 'ثمان مئة': 800, 'ثمانيمئة': 800, 'ثماني مئة': 800,
           'تسعمئة': 900, 'تسع مئة': 900}

DAY_ONES = {'واحد': 1, 'حادي': 1, 'ثاني': 2, 'ثالث': 3, 'رابع': 4, 'خامس': 5, 'خميس': 5, 'سادس': 6, 'سابع': 7,
            'ثامن': 8, 'تاسع': 9, 'عاشر': 10}
DAY_TEN = {'عشرة': 10, 'عشري': 10, 'عشر': 10, 'عشرين': 20, 'عشرون': 20, 'ثلاثين': 30, 'ثلاثون': 30}

MONTHS = {'محرم': 1, 'شهر الله المحرم': 1, 'صفر': 2, 'صفر الخير': 2, 'ربيع': 3, 'ربيع الاول': 3, 'ربيع الثاني': 4,
          'ربيع الاخر': 4, 'جمادى الاول': 5, 'جمادى الاولى': 5, 'جمادى الاخرة': 6, 'جمادى الاخر': 6, 'جمادى الثانية': 6,
          'رجب': 7, 'رجب الفرد': 7, 'رجب المبارك': 7, 'شعبان': 8, 'شعبان المكرم': 8, 'رمضان': 9,
          'رمضان المعظم': 9, 'شوال': 10, 'ذي القعدة': 11, 'ذي قعدة': 11, 'ذي الحجة': 12, 'ذي حجة': 12, 'ذو القعدة': 11,
          'ذو قعدة': 11, 'ذو الحجة': 12, 'ذو حجة': 12, 'اخر': 'End of'}

AR_MONTHS = '|'.join(['(?:' + r'\s'.join(key.split()) + ')' for key in MONTHS.keys()])
AR_ONES = '|'.join(ONES.keys())
AR_TEN = '|'.join(TEN.keys())
AR_HUNDRED = '|'.join(['(?:' + r'\s'.join(key.split()) + ')' for key in HUNDRED.keys()])
AR_ONES_DAY = '|'.join(DAY_ONES.keys())
AR_TEN_DAY = '|'.join(DAY_TEN.keys())
DATE = r'(?P<context>' + WORD + r'{0,10}?' + r'(?:\s(?:في|تقريبا))?' + WORD + r'{0,9}?)' + \
       r'(?:\s(:?ال)?(?P<day_ones>' + AR_ONES_DAY + r'))?(?:\s(:?و)?(:?ال)?(?P<day_ten>' + AR_TEN_DAY + r'))?' + \
       r'(?:\s(?:(?:من\s)?(?:شهر\s)?)?(?:ال)?(?P<month>' + AR_MONTHS + r')(?:\s(?:من|في)(?:\sشهور)?)?)?' + \
       r'\s(?:سنة|عام)(?:\s(?P<ones>' + AR_ONES + r'))?' + \
       r'(?:\s[و]?(?P<ten>' + AR_TEN + r'))?' + \
       r'(?:\s[و]?(?P<hundred>' + AR_HUNDRED + r'))?(\sب(?P<place>' + AR_STR + '))?(?=(?:' + WORD + r'|[\s\.,]|$))'

DATE_PATTERN = re.compile(DATE)
MONTH_PATTERN = re.compile(AR_MONTHS)

DATE_CATEGORIES = {'ولد': 'birth', 'مولده': 'birth', 'مات': 'death', 'موته': 'death', 'حخ': 'hadscha', 'سمع': 'samia',
                   'قرا': 'qaraa', 'استقر': 'istaqara', 'اجاز': 'ijaza', 'انفصل': 'infasala', 'لقي': 'laqiya'}
# Treffen und bürgen
AR_DATE_CATEGORIES = '|'.join(DATE_CATEGORIES.keys())
DATE_CATEGORY_PATTERN = re.compile(r'\s[وف]?(?P<date_category>' + AR_DATE_CATEGORIES + r')[تا]?')

logger = setup_logger('dates_matching', 'results/' + WHICH + '/logs/dates_matching.log')

c = {'total': 0, 'day_ones': 0, 'day_ten': 0, 'month': 0, 'month2': 0, 'ones': 0, 'ten': 0, 'hundred': 0}
date_category_list = []


class Date:
    def __init__(self, year, month, month_str, day, category, match, place):
        self.year = year
        self.month = month
        self.month_str = month_str
        self.day = day
        self.category = category
        self.match = match
        self.place = place

    def __eq__(self, other):
        return self.year == other.year and self.month == other.month and self.month_str == other.month_str and self.day == other.day and self.category == other.category and self.match == other.match and self.place == other.place

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)


def get_dates(text_nt):
    if type(text_nt) is float:
        return nan
    msg = ''
    text = ' '.join(text_nt)
    msg += text + '\n'
    dates = []
    for m in DATE_PATTERN.finditer(text):
        c['total'] += 1
        msg += '\n'
        month_str = nan
        month = nan
        year = 0
        day = 0
        place = nan
        if DATE_CATEGORY_PATTERN.search(m.group('context')):
            last = DATE_CATEGORY_PATTERN.findall(m.group('context'))[-1]
            date_category_str = last
        else:
            date_category_str = False
        date_category = DATE_CATEGORIES.get(date_category_str) if date_category_str else nan
        date_category_list.append(date_category)
        if m.group('day_ones'):
            c['day_ones'] += 1
            msg += f'day_ones: {m.group("day_ones")}' + '\n'
            day += DAY_ONES.get(m.group('day_ones'))
        if m.group('day_ten'):
            c['day_ten'] += 1
            msg += f'day_ten: {m.group("day_ten")}' + '\n'
            day += DAY_TEN.get(m.group('day_ten'))
        if m.group('month'):
            c['month'] += 1
            month_str = m.group('month')
            month = MONTHS.get(month_str)
            msg += f'Month: {month_str}: {month}' + '\n'
        else:
            mm = MONTH_PATTERN.search(m[0])
            if mm:
                c['month2'] += 1
                month_str = mm[0]
                month = MONTHS.get(month_str)
                msg += f'Month2: {month_str}: {month}' + '\n'
        if m.group('ones'):
            c['ones'] += 1
            msg += f'ones: {m.group("ones")}' + '\n'
            year += ONES.get(m.group('ones'))
        if m.group('ten'):
            c['ten'] += 1
            msg += f'ten: {m.group("ten")}' + '\n'
            year += TEN.get(m.group('ten'))
        if m.group('hundred'):
            c['hundred'] += 1
            msg += f'hundred: {m.group("hundred")}' + '\n'
            year += HUNDRED.get(m.group('hundred'))
        if m.group('place'):
            place = m.group('place')

        if day == 0:
            day = nan
        if year == 0:
            year = nan

        date = Date(year, month, month_str, day, date_category, m[0], place)
        dates.append(date)
        msg += str(date) + '\n'

    msg += '\n\n\n'
    logger.info(msg)

    if dates:
        return dates
    else:
        return nan


def get_date_category_stat():
    date_category_stat = []
    for key, val in Counter(date_category_list).items():
        date_category_stat.append((key, val))

    return date_category_stat
