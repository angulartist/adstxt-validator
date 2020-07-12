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

from lib.entities import Record
from lib.transformers import tokenize, get_records, get_vars, group_by_domains

NUM_MIN_RECORD_SLOTS = 3
NUM_VARIABLE_SLOTS = 2

processed_urls = set()

results = []


def recursive_parser(url, sld=False):
    print('Processing URL:', url)

    if url in processed_urls:
        print('URL:', url, 'skipped')
        return None

    processed_urls.add(url)

    scheme, netloc, *rest = urlparse(url)

    entry = {
        'domain': netloc,
        'results': {
            'recs': [],
            'vars': {
                'sub_domains': [],
                'contacts': []
            }
        },
        'outliers': []
    }

    try:
        resp = requests.get(url)
    except Exception as e:
        print(e)
    else:
        text = resp.text

        elms = [tokenize(string) for string in text.rsplit('\n')]

        _records, _variables, _outliers = [], [], []

        for tokens, num_slots in elms:
            if num_slots >= NUM_MIN_RECORD_SLOTS:
                target = _records
            elif num_slots == NUM_VARIABLE_SLOTS:
                target = _variables
            else:
                target = _outliers

            target.append(tokens)

        records, variables = get_records(_records), get_vars(_variables)

        for data in records + variables:
            if isinstance(data, Record):
                entry['results']['recs'].append(data)
            else:
                if data.key.upper() == 'SUBDOMAIN':
                    entry['results']['vars']['sub_domains'].append(data)
                else:
                    entry['results']['vars']['contacts'].append(data)

        entry['outliers'] = _outliers

    results.append(entry)

    # get subdomain ads.txt for SLD only
    # (sub-domains redirect are not allowed)
    # -- Only root domains should refer crawlers
    # to subdomains. Subdomains should not refer to
    # other subdomains. -- (IAB)
    if sld:
        def get_next_location(sub_domains):
            _, domain, *other = sub_domains

            return f'{scheme}://{domain}/ads.txt'

        next_locations = map(get_next_location, entry['results']['vars']['sub_domains'])

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
