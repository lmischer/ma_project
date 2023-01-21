import pandas as pd
from helper.methods import get_df_openITI, get_df_shamela

if __name__ == '__main__':
    pass


def get_parsing_comparison():
    openiti = get_df_openITI()
    openiti_value_count = openiti['volume'].value_counts()
    openiti_value_count.rename('openITI')

    shamela = get_df_shamela()
    shamela_value_count = shamela['volume'].value_counts()
    shamela_value_count.rename('Shamela')

    df = pd.concat([shamela_value_count, openiti_value_count], axis=1)
    df['percent'] = shamela_value_count / openiti_value_count

    df.to_csv('results/parsing/num_entries_per_vol.csv', index=False)
