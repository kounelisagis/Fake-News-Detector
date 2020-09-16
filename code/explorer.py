import os, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import newspaper
from urllib.parse import urljoin


BASE_URL = 'http://epfl.elasticsearch.spinn3r.com/content*/_search?scroll=1m'
BULK_SIZE = 100000

SPINN3R_SECRET = os.environ['SPINN3R_SECRET']

HEADERS = {
    'X-vendor': 'epfl',
    'X-vendor-auth': SPINN3R_SECRET
}


def make_a_query(query_text, news_urls, percentage=60):
    '''Receives the tweets that correspond to the query text.
    This function is responsible for finding the matches with the urls provided.
    '''

    print(query_text)

    query = {
        'size': BULK_SIZE,
        'query': {
            'bool': {
                'must': [
                    {
                        'more_like_this': {
                            'fields': ['main'],
                            'like_text': query_text,
                            'min_term_freq': 1,
                            'max_query_terms': 250,
                            'minimum_should_match': '4<' + str(percentage) + '%',  # if the number of tokens is less than 4 they are all required
                        }
                    },
                    {
                        'match': {
                            'domain': 'twitter.com'
                        }
                    }
                ]
            }
        }
    }

    resp = requests.post(BASE_URL, headers = HEADERS, json = query, stream = True)
    resp_json = json.loads(resp.text)

    posts_with_links = [post for post in resp_json['hits']['hits'] if 'expanded_links' in post['_source']]

    print('Total hits: {}'.format(resp_json['hits']['total']))
    print('Hits with links: {}'.format(len(posts_with_links)))


    for post in posts_with_links:
        for url in post['_source']['expanded_links']:
            try:
                # get article info
                article = newspaper.Article(url=url, keep_article_html=True, follow_meta_refresh=True, fetch_images=False, headers={'User-Agent': 'Mozilla/5.0'})
                article.download()
                article.parse()

                soup = BeautifulSoup(article.article_html, 'lxml')

                hrefs = [a['href'] for a in soup.find_all('a') if a.has_attr('href')]

                for new_url in hrefs:
                    new_url = urljoin(article.canonical_link, new_url)

                    response = requests.head(url=new_url, allow_redirects=True, headers={'User-Agent': 'Mozilla/5.0'})
                    new_url = response.url

                    new_url = new_url.split('?')[0].split('#')[0].strip('/')
                    new_url = re.sub('https?://(www\.)?', '', new_url)

                    if new_url in news_urls:
                        print('---------MATCH FOUND!---------')
                        print(post)
                        print('------------------------------')

            except:
                pass


def get_dataframes_dicts(keywords_csv_dir = 'keywords_csvs/', urls_csv_dir = 'urls_csvs/'):

    keywords_dfs = {}
    urls_dfs = {}

    for file in os.listdir(keywords_csv_dir):
        filename = os.fsdecode(file)
        filepath = os.path.join(keywords_csv_dir, filename)
        df = pd.read_csv(filepath, header=0)

        key = re.sub('\.csv$', '', filename)
        keywords_dfs[key] = df


    for file in os.listdir(urls_csv_dir):
        filename = os.fsdecode(file)
        filepath = os.path.join(urls_csv_dir, filename)
        df = pd.read_csv(filepath)

        key = re.sub('\.csv$', '', filename)
        urls_dfs[key] = df

    return keywords_dfs, urls_dfs


if __name__ == '__main__':

    keywords_dict, urls_dict = get_dataframes_dicts()

    for key in keywords_dict:
        keywords_df = keywords_dict[key]
        keywords_list = keywords_df['Keyword'].tolist()
        query_text = ' '.join(keywords_list[:6])

        urls_df = urls_dict[key]
        news_urls = urls_df['Url'].tolist()

        make_a_query(query_text, news_urls)
