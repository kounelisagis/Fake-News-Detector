import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt


def show_plot(x, y, df, xlabel='', ylabel=''):
    _, ax = plt.subplots()
    ax.set_xscale('log')
    sns.set_style('whitegrid')

    sns.boxplot(y=y, x=x, data=df,
                whis=[0, 100], palette='vlag')
    sns.stripplot(y=y, x=x, data=df,
                size=4, color='.3', linewidth=0)

    ax.set(ylabel=ylabel)
    ax.set(xlabel=xlabel)

    plt.tight_layout()
    plt.show()



def aggregate(read_folder, write_folder):

    res = []

    for file in sorted(os.listdir(read_folder)):
        filename = os.fsdecode(file)
        filepath = os.path.join(read_folder, filename)
        df = pd.read_csv(filepath)
        df = df[['likes', 'shares', 'source_followers']]
        series = df.sum(axis = 0, skipna = True)

        title = filename.replace('.csv', '')
        res.append( [ x[1] for x in [('title' , title)] + list(series.items()) ] )


    df1 = pd.DataFrame(res, columns=['title', 'likes', 'shares', 'source_followers'])
    for collumn in ['likes', 'shares', 'source_followers']:
        df1[collumn] = df1[collumn].astype(int)


    df2 = pd.read_excel('data/altmetric_top_2019.xlsx')
    df2 = df2[['Title', 'Subjects', 'Open_Access']]
    df2 = df2.rename(columns={'Title': 'title', 'Subjects': 'subjects', 'Open_Access': 'open_access'})
    df2['title'] = df2['title'].str.replace(' ', '_')
    df2['open_access'] = df2['open_access'].str.replace('\n', '')


    df = pd.merge(df1, df2, on='title')
    df.to_csv(write_folder + '/top_100.csv')

    # Plots
    show_plot('likes', 'subjects', df, 'Likes', 'Subjects')
    show_plot('shares', 'subjects', df, 'Shares', 'Subjects')
    show_plot('source_followers', 'subjects', df, 'Source Followers', 'Subjects')

    show_plot('likes', 'open_access', df, 'Likes', 'Access Type')
    show_plot('shares', 'open_access', df, 'Shares', 'Access Type')
    show_plot('source_followers', 'open_access', df, 'Source Followers', 'Access Type')
    # ---

    df_temp = df.groupby(['open_access']).sum()
    df_temp.to_csv(write_folder + '/open_access_grouped.csv')
    
    df_temp = df.groupby(['subjects']).sum()
    df_temp.to_csv(write_folder + '/subjects_grouped.csv')



if __name__ == '__main__':

    read_folder = 'results_csvs/'
    write_folder = 'aggregated_csvs/'

    os.makedirs(os.path.dirname(write_folder), exist_ok=True)

    aggregate(read_folder, write_folder)
