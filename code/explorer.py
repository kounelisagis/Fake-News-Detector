import os, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import newspaper
from urllib.parse import urljoin, urlsplit
import pandas as pd
import os
from threading import Thread, get_ident
from collections import defaultdict


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

    results = []
    visited_lvl1 = set()
    visited_lvl2 = set()

    for post in posts_with_links:
        tweet_url = post['_source']['permalink']
        for url in post['_source']['expanded_links']:
            try:
                # Discard already visited urls
                if url in visited_lvl1:
                    continue

                # Discard urls that have no path
                url_parts = list(urlsplit(url))
                
                if url_parts[2] in ['', '/']:
                    continue

                visited_lvl1.add(url)

                # get article info
                article = newspaper.Article(url=url, keep_article_html=True, follow_meta_refresh=True, fetch_images=False, headers={'User-Agent': 'Mozilla/5.0'})
                article.download()
                article.parse()

                if article.canonical_link in visited_lvl1:
                    continue
                url_parts = list(urlsplit(article.canonical_link))
                if url_parts[2] in ['', '/']:
                    continue

                visited_lvl1.add(article.canonical_link)


                article_domain = list(urlsplit(article.canonical_link))[1]
                segments = 3 if '.co.' in article_domain else 2
                article_domain = '.'.join(article_domain.split('.')[-segments:])

                soup = BeautifulSoup(article.article_html, 'html.parser')
                hrefs = [a['href'] for a in soup.find_all('a') if a.has_attr('href') and 'mailto' not in a['href']]

                for href in hrefs:
                    new_url = urljoin(article.canonical_link, href)

                    if new_url in visited_lvl2:
                        continue
                    new_url_parts = list(urlsplit(new_url))
                    if new_url_parts[2] in ['', '/']:
                        continue

                    visited_lvl2.add(new_url)


                    url_domain = new_url_parts[1]
                    segments = 3 if '.co.' in url_domain else 2
                    url_domain = '.'.join(url_domain.split('.')[-segments:])

                    # Don't send head requests to inner urls
                    if article_domain != url_domain:
                        response = requests.head(url=new_url, allow_redirects=True, headers={'User-Agent': 'Mozilla/5.0'})
                        new_url = response.url

                        if new_url in visited_lvl2:
                            continue
                        new_url_parts = list(urlsplit(new_url))
                        if new_url_parts[2] in ['', '/']:
                            continue

                        visited_lvl2.add(new_url)


                    new_url = re.sub('^(www\.)', '',(new_url_parts[1] + new_url_parts[2]).strip('/'))

                    if new_url in news_urls:
                        print('---------MATCH FOUND!---------')
                        print((tweet_url, article.canonical_link, new_url, ))
                        results.append((tweet_url, article.canonical_link, new_url, ))
                        print('------------------------------')

            except:
                pass

    return pd.DataFrame(results)


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
        urls_dfs[key]= df

    return keywords_dfs, urls_dfs


def query_task(key_keywords_df, urls_df):

    key = key_keywords_df[0]
    keywords_df = key_keywords_df[1]
    keywords_list = keywords_df['Keyword'].tolist()

    for keywords_num in range(5, 11):
        for percentage in range(60, 110, 10):
            print('thread: {}, #keywords: {}, percentage {}'.format(get_ident(), keywords_num, percentage))
            query_text = ' '.join(keywords_list[:keywords_num])
            news_urls = urls_df[1]['Url'].tolist()

            df = make_a_query(query_text=query_text, news_urls=news_urls, percentage=percentage)

            if not df.empty:
                df.columns=['tweet', 'article_lvl1', 'article_lvl2']
                results_csv_dir = 'results_csvs/'
                os.makedirs(os.path.dirname(results_csv_dir), exist_ok=True)

                fullname = os.path.join(results_csv_dir, str(key) + '-' + str(keywords_num) + '-' + str(percentage) + '.csv')
                df.to_csv(fullname)


if __name__ == '__main__':

    keywords_dfs, urls_dfs = get_dataframes_dicts()

    threads = [None] * len(keywords_dfs)

    for i in range(len(threads)):
        threads[i] = Thread(target=query_task, args=(list(keywords_dfs.items())[i], list(urls_dfs.items())[i]))
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()
