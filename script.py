from collections import defaultdict 
import requests
from bs4 import BeautifulSoup
import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from urllib.parse import urljoin


def get_keywords(url_end):
    '''Collects the titles of the news that mention the paper given.
    Returns a frequency dictionary of the words that constitute the tiles.
    '''

    keywords = defaultdict(int)

    # get the altmetric details url
    api_url = 'https://api.altmetric.com/v1/doi/10.15585/mmwr.'
    r = requests.get(url = api_url + url_end)
    data = r.json()

    # get html of news page
    url = data['details_url'].replace('.php?citation_id=', '/') + '/news'
    url = url.replace('www', 'cdc')
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    for paragraph in soup.find_all('article'):
        try:
            title = paragraph.find('a').find('h3').text

            # split into words
            tokens = word_tokenize(title)
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
        except:
            pass

    return keywords



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


if __name__ == '__main__':
    nltk.download('stopwords')

    papers = get_cdc_mmwr_papers('https://www.cdc.gov/mmwr/indss_2019.html')

    for paper in papers:
        try:
            url_end = re.search(r'/ss/(.*?).htm', paper['link']).group(1)
            print('-----------------------------------')
            print(paper['title'])
            print(get_keywords(url_end))
            print('-----------------------------------')
        except:  # irrelevant link
            pass
