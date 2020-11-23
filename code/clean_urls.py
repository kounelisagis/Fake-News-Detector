import pandas as pd
import os

def clean_save_urls(read_folder, write_folder):
    directory = os.fsencode(read_folder)

    for file in sorted(os.listdir(read_folder)):
        filename = os.fsdecode(file)

        filepath = os.path.join(read_folder, filename)
        df = pd.read_csv(filepath)
        df = df[~df['Url'].str.contains('gdpr|consent')]
        df.to_csv(os.path.join(write_folder, filename), index=False)


if __name__ == '__main__':

    read_folder = 'urls_csvs/'
    write_folder = 'urls_csvs_cleaned/'

    os.makedirs(os.path.dirname(write_folder), exist_ok=True)

    clean_save_urls(read_folder, write_folder)
