import pandas as pd


def write_files(df):
    for idx, row in df.iterrows():
        name = idx.__str__()
        df_analysis = pd.DataFrame(row['token_list'], columns=['tokens', 'tokenized', 'lemmas', 'ne_labels', 'pos'])
        df_analysis.to_csv('out/qm/' + name + '.csv', index=False)
