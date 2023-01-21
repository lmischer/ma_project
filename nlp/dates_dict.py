from helper.helpers_logging import setup_logger


logger = setup_logger('dates_dict', '../results/dates/dates_dict.log')

hundred = ['مائة', 'ماية', 'مية', 'مئة']

one = {'ثلاث': 3, 'ثلث': 3, 'اربع': 4, 'خمس': 5, 'ست': 6, 'سبع': 7, 'ثمان': 8, 'ثماني': 8, 'تسع': 9}


if __name__ == '__main__':
    final = {}

    for h in hundred:
        for k, value in one.items():
            key = k + h
            final[key] = value * 100
            key = k + ' ' + h
            final[key] = value * 100

    logger.info(final.__str__())
