import os, json
import requests

BASE_URL = 'http://epfl.elasticsearch.spinn3r.com/content*/_search'
BULK_SIZE = 1000

SPINN3R_SECRET = os.environ['SPINN3R_SECRET']

HEADERS = {
    'X-vendor': 'epfl',
    'X-vendor-auth': SPINN3R_SECRET
}

query = {
    "size": BULK_SIZE,
    "query": {
        "bool": {
            "must": [{
                    "more_like_this": {
                        "fields": ["main"],
                        "like_text": "lung youth still says health coronavirus americans lose",
                        "min_term_freq": 1,
                        "max_query_terms": 250,
                        "minimum_should_match": "60%",
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
