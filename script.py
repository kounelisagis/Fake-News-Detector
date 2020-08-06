from collections import defaultdict 
import requests
import json
from bs4 import BeautifulSoup
from newspaper import Article
import nltk


def get_papers():
    """Collects the top 10 papers of the last month.
    Returns a list of tuples which contains pairs of dois and urls.
    """

    papers = []

    r = requests.get(url='https://api.altmetric.com/v1/citations/1m?num_results=10')
    data = r.json()

    for paper in data['results']:
        try:
            papers.append( (paper['doi'], paper['details_url'], ) )
        except:
            pass

    return papers



def get_news_urls(url):
    """Collects the news that mention the paper given.
    Url argument is in the form of the Altmetric API repsonse object.
    Returns a list of the news urls.
    """

    urls = []

    url = url.replace('.php?citation_id=', '/')
    url += '/news'

    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')


    for paragraph in soup.find_all('article'):
        try:
            urls.append(paragraph.find('a')['href'])
        except:
            pass

    return urls


if __name__ == "__main__":

    nltk.download('punkt')  # Required by newspaper3k


    papersDict = defaultdict(list)

    papers = get_papers()

    for paper_doi, paper_url in papers:
        news_urls = get_news_urls(paper_url)

        for news_url in news_urls:
            try:
                article = Article(news_url)
                article.download()
                article.parse()
                article.nlp()

                new_article = {'url': news_url,'text': article.text, 'keywords': article.keywords}
                papersDict[paper_doi].append(new_article)
            except:
                pass


    with open('result.json', 'w') as fp:
        json.dump(papersDict, fp)
