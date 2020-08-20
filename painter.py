import pandas as pd
import matplotlib.pyplot as plt
import os
import re


def get_dataframes_dict(csv_dir = 'csvs/'):
    directory = os.fsencode(csv_dir)

    dfs = {}

    for file in sorted(os.listdir(csv_dir)):
        filename = os.fsdecode(file)
        filepath = os.path.join(csv_dir, filename)
        df = pd.read_csv(filepath, index_col=0)

        key = re.sub('\.csv$', '', filename)
        dfs[key] = df

    return dfs


if __name__ == '__main__':

    dfs_dict = get_dataframes_dict()

    charts_dir = 'charts/'
    os.makedirs(os.path.dirname(charts_dir), exist_ok=True)

    for key, value in dfs_dict.items():
        value.plot(kind='bar')
        plt.title(key)
        plt.savefig(os.path.join(charts_dir, key + '.png'), bbox_inches='tight', dpi=100)

        plt.show()
