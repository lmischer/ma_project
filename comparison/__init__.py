import pandas as pd


if __name__ == '__main__':
    df_locs_o = pd.read_csv('results/openiti/topos_with_value_counts.csv') #[['geometry', 'toponyms', 'count']]
    df_locs_s = pd.read_csv('results/shamela/topos_with_value_counts.csv')  # [['geometry', 'toponyms', 'count']]

    df_locs = df_locs_o.join(df_locs_s['value_counts'], lsuffix='_openiti', rsuffix='_shamela')
    df_locs['percent'] = df_locs['value_counts_openiti'] / df_locs['value_counts_shamela']

    df_locs.to_csv('results/comparison/topos.csv')

    df_dates_o = pd.read_csv('results/openiti/dates.csv')
    df_dates_s = pd.read_csv('results/shamela/dates.csv')

