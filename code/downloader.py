from collections import defaultdict 
import requests
from bs4 import BeautifulSoup
import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from urllib.parse import urljoin
import os
import pandas as pd
import spacy
from fuzzywuzzy import fuzz
import multiprocessing


nlp = spacy.load('en_core_web_lg')
nltk.download('stopwords')


def get_keywords_and_links(url):
    '''Collects the titles of the news that mention the paper given.
    Returns a frequency Pandas Dataframe of the words that constitute the tiles and the urls of the news.
    '''
    titles = []
    keywords = defaultdict(int)
    links = [url]

    # get the cdc title
    r = requests.get(url = url)
    soup = BeautifulSoup(r.content, 'lxml')
    cdc_title = soup.find(class_='content').find('h1').text.strip()


    # get the altmetric details url
    api_url = 'https://api.altmetric.com/v1/doi/10.15585/mmwr.'
    url_end = re.search(r'/ss/(.*?).htm', url).group(1)

    r = requests.get(url = api_url + url_end)
    if not r:  # check the second version
        r = requests.get(url = api_url + 'ss.' + url_end[2:])
        if not r:
            return None

    data = r.json()


    # get html of altmetric news page
    url = data['details_url'].replace('.php?citation_id=', '/') + '/news'
    url = url.replace('www', 'cdc')
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    for paragraph in soup.find_all('article'):
        titles.append(paragraph.find('h3').text)
        try:  # a element may be missing
            link = paragraph.find('a')['href']

            # find expanded url
            session = requests.Session()
            resp = session.head(link, allow_redirects=True, timeout=(10, 20), headers={'User-Agent': 'Mozilla/5.0'})
            links.append(resp.url)
        except:
            pass

    # remove duplicates in links list
    links = list(set(links))

    # remove parameters from links
    links = [link.split('?')[0] for link in links]

    for title in titles + [cdc_title]:
        try:
            doc = nlp(title)
            tokens = []

            for chunk in doc.noun_chunks:
                for word in chunk.text.split():
                    tokens.append(word)

            # convert to lower case
            tokens = [w.lower() for w in tokens]
            # remove punctuation from each word
            table = str.maketrans('', '', string.punctuation)
            stripped = [w.translate(table) for w in tokens]
            # remove remaining tokens that are not alphabetic
            words = [word for word in stripped if word.isalpha()]
            # filter out stop words
            stop_words = set(stopwords.words('english'))
            words = [w for w in words if not w in stop_words]

            for w in words:
                keywords[w] += 1

            # merge words (used for cases like plural)
            for word1 in list(keywords):
                for word2 in list(keywords):
                    if word1 in keywords and word1 != word2 and fuzz.ratio(word1, word2) > 80:
                        keywords[word1] += keywords[word2]
                        del keywords[word2]

        except Exception as e:
            print(e)


    return url_end, pd.DataFrame.from_dict(keywords, orient='index', columns=['Appearances']), pd.DataFrame(links)


def get_cdc_mmwr_papers(url):
    '''Collects the papers included in a CDC Morbidity and Mortality Weekly Report.
    Returns a dictionary of tiles and links.
    '''
    papers = []

    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    content_div = soup.find_all('div', class_='syndicate')[1]
    a_tags = content_div.find_all('a')

    for a_tag in a_tags:
        text = a_tag.get_text()
        link = urljoin('https://www.cdc.gov/', a_tag['href'])

        new_paper = {'title': text, 'link': link}
        papers.append(new_paper)

    return papers


def paper_task(paper):
    '''The function that every process executes.
    Responsible for saving the csv files containing the desirable keywords and links.
    '''

    try:
        print('-----------------------------------')
        print('-> {} | {}'.format(multiprocessing.current_process(), paper['title']))

        filename, keywords_df, links_df = get_keywords_and_links(paper['link'])
        print(keywords_df.size)

        if not keywords_df.empty:

            # save keywords csv file
            keywords_df = keywords_df.nlargest(10, 'Appearances')
            keywords_df.index.names = ['Keyword']

            keywords_csv_dir = 'keywords_csvs/'
            os.makedirs(os.path.dirname(keywords_csv_dir), exist_ok=True)

            fullname = os.path.join(keywords_csv_dir, filename + '.csv')
            keywords_df.to_csv(fullname)

            # save links csv file
            links_csv_dir = 'links_csvs/'
            os.makedirs(os.path.dirname(links_csv_dir), exist_ok=True)

            fullname = os.path.join(links_csv_dir, filename + '.csv')
            links_df.to_csv(fullname, header = ['Link'], index = False)

    except Exception as e:
        print(e)


if __name__ == '__main__':

    papers = get_cdc_mmwr_papers('https://www.cdc.gov/mmwr/indss_2019.html')

    with multiprocessing.Pool() as p:
        p.map(paper_task, papers)
