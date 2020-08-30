import os, json, re
import requests
import pandas as pd

BASE_URL = 'http://epfl.elasticsearch.spinn3r.com/content*/_search'
BULK_SIZE = 1000

SPINN3R_SECRET = os.environ['SPINN3R_SECRET']

HEADERS = {
    'X-vendor': 'epfl',
    'X-vendor-auth': SPINN3R_SECRET
}


def make_a_query(text, percentage=60):
    print(text)

    query = {
        "size": BULK_SIZE,
        "query": {
            "bool": {
                "must": [
                    {
                        "more_like_this": {
                            "fields": ["main"],
                            "like_text": text,
                            "min_term_freq": 1,
                            "max_query_terms": 250,
                            "minimum_should_match": "4<" + str(percentage) + "%",  # if the number of tokens is less than 4 they are all required
                        }
                    },
                    {
                        "match": {
                            "domain": "twitter.com"
                        }
                    }
                ]
            }
        }
    }

    resp = requests.post(BASE_URL, headers = HEADERS, json = query)
    resp_json = json.loads(resp.text)

    print("Total hits: {}".format(resp_json["hits"]["total"]))

    with_link = [post for post in resp_json["hits"]["hits"] if "links" in post['_source']]
    without_link = [post for post in resp_json["hits"]["hits"] if "links" not in post['_source']]

    print("Hits with links: {}".format(len(with_link)))
    print("Hits without links: {}".format(len(without_link)))


def get_dataframes_dicts(keywords_csv_dir = 'keywords_csvs/', links_csv_dir = 'links_csvs/'):

    keywords_dfs = {}
    links_dfs = {}

    for file in os.listdir(keywords_csv_dir):
        filename = os.fsdecode(file)
        filepath = os.path.join(keywords_csv_dir, filename)
        df = pd.read_csv(filepath, header=0)

        key = re.sub('\.csv$', '', filename)
        keywords_dfs[key] = df


    for file in os.listdir(links_csv_dir):
        filename = os.fsdecode(file)
        filepath = os.path.join(links_csv_dir, filename)
        df = pd.read_csv(filepath)

        key = re.sub('\.csv$', '', filename)
        links_dfs[key] = df

    return keywords_dfs, links_dfs


if __name__ == '__main__':

    keywords_dict, links_dict = get_dataframes_dicts()

    for key in keywords_dict:
        keywords_df = keywords_dict[key]
        keywords_list = keywords_df['Keyword'].tolist()
        text = ' '.join(keywords_list)

        links_df = links_dict[key]
        links_list = links_df['Link'].tolist()

        make_a_query(text)
