from numpy import nan
from helper.helpers_logging import setup_logger
import pandas as pd

from helper.methods import WHICH
from helper.plotting import box_plot

logger_qm = setup_logger('qm_locs_logger', 'results/' + WHICH + '/logs/locs_missed_qm.log')


def locs_matching_qm(row, key_all, key_matched):
    num_matched_locs, num_missed_locs, percent_missed_locs, missed = nan, nan, nan, nan
    if type(row[key_all]) is float:
        return num_matched_locs, num_missed_locs, percent_missed_locs, missed
    elif type(row[key_matched]) is float:
        num_matched_locs = 0
        num_missed_locs, percent_missed_locs, missed = len(list(set(row[key_all]))), 100, row[key_all]
    else:
        num_matched_locs = len(list(set(row[key_matched])))
        num_missed_locs = len(list(set(row[key_all]))) - num_matched_locs
        percent_missed_locs = num_missed_locs / len(list(set(row[key_all]))) * 100
        missed = list(set(row[key_all]).difference(set(row[key_matched])))

    return num_matched_locs, num_missed_locs, percent_missed_locs, missed


def locs_missing_qm(num_matched_locs, num_missed_locs, percent_missed_locs, missed, where):
    num_matched_locs = pd.Series(num_matched_locs)
    num_missed_locs = pd.Series(num_missed_locs)
    percent_missed_locs = pd.Series(percent_missed_locs)
    missed = pd.Series(missed)

    logger_qm.info(f'How many have been matched absolute: {num_matched_locs.sum()} {where}')
    logger_qm.info(f'how many have been missed absolute: {num_missed_locs.sum()} {where}')
    logger_qm.info(f'how many have been missed in percent: {percent_missed_locs.mean()} {where}')

    box_plot(num_missed_locs, 'Number of Toponyms not Identified Across all Entries', 'All Entries', 'results/' + WHICH + '/qm/locs_missed_' + where)
    box_plot(num_matched_locs, 'Number of Toponyms Identified Across all Entries?', 'All Enries', 'results/' + WHICH + '/qm/locs_matched_' + where)
    locs_series = missed.explode(ignore_index=True).dropna()
    locs_series.name = 'value_counts'
    df_value_counts = locs_series.value_counts().to_frame().reset_index()
    df_value_counts.sort_values(by='value_counts', ascending=False).to_csv(
        'results/' + WHICH + '/qm/non_matched_locs_' + where + '.csv')
