from collections import defaultdict 
import requests
from bs4 import BeautifulSoup
import re
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from urllib.parse import urlsplit
import os
import pandas as pd
import spacy
import nltk
from fuzzywuzzy import fuzz
from threading import Thread, get_ident


nlp = spacy.load('en_core_web_lg')
nltk.download('stopwords')


def clean_urls(urls):
    '''Removes protocol + www + parameters (if necessary) + fragments + trailing slash from urls.
    Returns the list of unique urls.
    '''

    r = []

    for i in range(len(urls)):
        url = ''
        url_parts = list(urlsplit(urls[i]))
        if url_parts[2] == '/':
            if url_parts[3] == '':
                continue
            url = url_parts[1] + url_parts[2] + url_parts[3]
        else:
            url = url_parts[1] + url_parts[2]

        url = re.sub('www\.', '', url).strip('/')
        r.append(url)

    # remove duplicates in urls list
    return list(set(r))


def get_titles_and_urls(url, title):
    '''Requires the url of the Altmetric news that mention the paper given and the title of the paper.
    Returns the tiles and the urls of the news.
    '''
    titles = [title]
    urls = [url]

    next_page = 1

    while next_page != None:
        r = requests.get(url.replace('www', 'cdc') + '/page:' + str(next_page))
        print(r.url)
        soup = BeautifulSoup(r.content, 'lxml')

        if soup.find_all('a', {'rel': 'next'}):
            next_page += 1
        else:
            next_page = None

        print(next_page)

        for paragraph in soup.find_all('article'):
            titles.append(paragraph.find('h3').text)
            try:  # a element may be missing
                href = paragraph.find('a')['href']

                # find expanded url
                session = requests.Session()
                resp = session.head(href, allow_redirects=True, timeout=(10, 20), headers={'User-Agent': 'Mozilla/5.0'})
                urls.append(resp.url)
            except:
                pass

    urls = clean_urls(urls)

    return titles, pd.DataFrame(urls)


def get_keywords(titles):
    '''Requires the titles of the Altmetric news that mention the paper given.
    Returns a frequency Dataframe.
    '''
    keywords = defaultdict(int)

    for title in titles:
        try:
            doc = nlp(title)
            tokens = [token.text.lower() for token in doc if not token.is_stop and not token.is_punct and token.is_alpha]

            for token in tokens:
                keywords[token] += 1

            # merge words which are close (used for cases like plural)
            for keyword1 in list(keywords):
                for keyword2 in list(keywords):
                    if keyword1 in keywords and keyword1 != keyword2 and fuzz.ratio(keyword1, keyword2) > 80:
                        large = keyword1 if len(keyword1) > len(keyword2) else keyword2
                        small = keyword1 if large == keyword2 else keyword2
                        keywords[small] += keywords[large]
                        del keywords[large]

        except Exception as e:
            print(e)

    return pd.DataFrame.from_dict(keywords, orient='index', columns=['Appearances'])


def get_top_altmetric_100_papers():

    df = pd.read_excel('data/altmetric_top_2019.xlsx')
    df = df[['Title', 'Details Page URL']]
    df = df.rename(columns={'Title': 'title', 'Details Page URL': 'url'})

    papers = [{**dictionary, 'url': (dictionary['url'] + '/news'),
                'filename': dictionary['title'].replace(" ", "_")} for dictionary in df.to_dict('records')]

    return papers


def get_cdc_mmwr_papers(url):
    '''Collects the papers included in a CDC Morbidity and Mortality Weekly Report.
    Returns a list of tiles and urls.
    '''
    papers = []

    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    content_div = soup.find_all('div', class_='syndicate')[1]
    a_tags = content_div.find_all('a')

    for a_tag in a_tags:
        # get the altmetric details url
        api_url = 'https://api.altmetric.com/v1/doi/10.15585/mmwr.'
        url_end = re.search(r'/ss/(.*?).htm', a_tag['href']).group(1)

        r = requests.get(url = api_url + url_end)
        if not r:  # if fail check the second url version
            r = requests.get(url = api_url + 'ss.' + url_end[2:])
            if not r:
                continue

        data = r.json()
        url = data['details_url'].replace('.php?citation_id=', '/') + '/news'
        url = url.replace('www', 'cdc')
        title = data['title']

        new_paper = {'title': title, 'url': url, 'filename': url_end}
        papers.append(new_paper)

    return papers


def paper_task(paper):
    '''The function that every process executes.
    Responsible for saving the csv files containing the desirable urls.
    '''
    try:
        print('-----------------------------------')
        print('-> {} | {}'.format(get_ident(), paper['title']))

        filename = paper['filename']
        titles, urls_df = get_titles_and_urls(paper['url'], paper['title'])
        
        # save urls csv file
        urls_csv_dir = 'urls_csvs/'
        os.makedirs(os.path.dirname(urls_csv_dir), exist_ok=True)

        fullname = os.path.join(urls_csv_dir, filename + '.csv')
        urls_df.to_csv(fullname, header = ['Url'], index = False)

    except Exception as e:
        print(e)


if __name__ == '__main__':

    papers = get_top_altmetric_100_papers()

    # option 2
    # papers = get_cdc_mmwr_papers('https://www.cdc.gov/mmwr/indss_2020.html')

    threads = [None] * len(papers)

    for i in range(len(papers)):
        threads[i] = Thread(target=paper_task, args=(papers[i], ))
        threads[i].start()
    for i in range(len(papers)):
        threads[i].join()
