"""
ADS.TXT Parser
doc: https://iabtechlab.com/wp-content/uploads/2019/03/IAB-OpenRTB-Ads.txt-Public-Spec-1.0.2.pdf
doc_ver: 1.0.2
"""
import argparse
import json
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from urllib.parse import urlparse

import requests
import typedload
from pipetools import pipe

from lib.entities import Record, Variable
from lib.transformers import group_by_domains, get_records, tokenize, strip_comments, split_fn, orchestrator_fn

results = []


def resolve(url: str = None):
    """
    Fetch url and get the content
    :param url:
    :return:
    """
    if url is None:
        raise ValueError('URL is mandatory.')

    print('Processing URL:', url)

    scheme, netloc, *rest = urlparse(url)

    try:
        resp = requests.get(url)

        return resp, (scheme, netloc)
    except Exception as e:
        print(e)


def recursive_parser(url: str = None, sld: bool = False):
    """
    Parse ads.txt
    :param url:
    :param sld:
    :return:
    """
    resp, meta = resolve(url)

    scheme, netloc = meta

    entry: dict = {
        'domain': netloc,
        'sld': sld,
        'results': {
            'recs': [],
            'vars': {
                'sub_domains': [],
                'contacts': []
            }
        }
    }

    inputs = (resp >
              pipe
              | (lambda x: x.text)
              | split_fn
              | strip_comments
              | tokenize
              | orchestrator_fn)

    records = (inputs > pipe
               | get_records)

    for data in records:
        if isinstance(data, Record):
            entry['results']['recs'].append(data)
        elif isinstance(data, Variable):
            if data.key.upper() == 'SUBDOMAIN':
                entry['results']['vars']['sub_domains'].append(data)
            else:
                entry['results']['vars']['contacts'].append(data)
        else:
            print('Skip outlier...')
            continue

    results.append(entry)

    # get subdomain ads.txt for SLD only
    # (sub-domains redirect are not allowed)
    # -- Only root domains should refer crawlers
    # to subdomains. Subdomains should not refer to
    # other subdomains. -- (IAB)
    if sld:
        def get_next_location(sub_domain: Variable):
            return f'{scheme}://{sub_domain.value}/ads.txt'

        # extract optional subdomains ads.txt from the root domain
        next_locations = filter(lambda x: x != url, map(get_next_location, entry['results']['vars']['sub_domains']))
        # recursive concurrent call
        with PoolExecutor(max_workers=4) as executor:
            for _ in executor.map(recursive_parser, next_locations):
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url',
                        help='The URL that points to the ads.txt',
                        required=True,
                        type=str)

    args = parser.parse_args()

    recursive_parser(args.url, sld=True)

    output = group_by_domains(results)

    # to json
    as_json = typedload.dump(output)

    with open('./output/data.json', 'w') as fh:
        json.dump(as_json, fh)


if __name__ == '__main__':
    main()
