import os, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import newspaper
import pandas as pd
import os
import multiprocessing
from collections import defaultdict


BASE_URL = 'http://epfl.elasticsearch.spinn3r.com/content*/_search'
BULK_SIZE = 1000

SPINN3R_SECRET = os.environ['SPINN3R_SECRET']

HEADERS = {
    'X-vendor': 'epfl',
    'X-vendor-auth': SPINN3R_SECRET
}


def make_a_query(news_urls):
    '''Receives the tweets that correspond to the query text.
    This function is responsible for finding the matches with the urls provided.
    '''

    results = []

    for news_url in news_urls:
        query = {
            'size': BULK_SIZE,
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'domain': 'twitter.com'
                            }
                        },
                        {
                            "query": {
                                "regexp": {
                                    "expanded_links": {
                                        "value": ".*" + news_url + ".*",
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        with requests.post(BASE_URL, headers = HEADERS, json = query, timeout=10) as resp:
            resp_json = json.loads(resp.text)
            posts_with_links = [post for post in resp_json['hits']['hits']]

            print('{} - Hits with links: {}'.format(multiprocessing.current_process(), len(posts_with_links)))

            for post in posts_with_links:
                source = post['_source']
                tweet_url = source['permalink']
                publish_date = source['published'] if 'published' in source else None
                likes = source['likes'] if 'likes' in source else None
                shares = source['shares'] if 'shares' in source else None
                source_followers = source['source_followers'] if 'source_followers' in source else None
                replied = source['replied'] if 'replied' in source else None
                sentiment = source['sentiment'] if 'sentiment' in source else None

                results.append( (tweet_url, news_url, publish_date, likes, shares, source_followers, replied, sentiment, ) )

    return pd.DataFrame(results, columns=['tweet_url', 'news_url', 'publish_date', 'likes', 'shares', 'source_followers', 'replied', 'sentiment'])



def query_task(key_urls_df):

    key = key_urls_df[0]
    urls_df = key_urls_df[1]

    news_urls = urls_df['Url'].tolist()

    df = make_a_query(news_urls=news_urls)

    if not df.empty:
        results_csv_dir = 'results_csvs/'
        os.makedirs(os.path.dirname(results_csv_dir), exist_ok=True)
        fullname = os.path.join(results_csv_dir, str(key) + '.csv')
        df.to_csv(fullname)
        print('--------CSV-FILE-SAVED----------')



def get_dataframes_dicts(urls_csv_dir = 'urls_csvs_cleaned/'):

    urls_dfs = {}

    for file in os.listdir(urls_csv_dir):
        filename = os.fsdecode(file)
        filepath = os.path.join(urls_csv_dir, filename)
        df = pd.read_csv(filepath)

        key = re.sub('\.csv$', '', filename)
        urls_dfs[key]= df

    return urls_dfs



if __name__ == '__main__':

    urls_dfs = get_dataframes_dicts()

    with multiprocessing.Pool() as p:
        p.map(query_task, urls_dfs.items())
