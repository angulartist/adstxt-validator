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

MIN_RECORD_SLOTS = 3

results = []


def recursive_parser(url):
    print('Location:', url)

    scheme, netloc, *rest = urlparse(url)

    entry = {
        'domain': netloc,
        'results': {
            'recs': [],
            'vars': {
                'sub_domains': [],
                'contacts': []
            }
        }
    }

    try:
        resp = requests.get(url)
    except Exception as e:
        print(e)
    else:
        text = resp.text

        elms = [tokenize(string) for string in text.rsplit('\n')]

        _records, _variables = [], []

        for tokens, num_slots in elms:
            target = _records if num_slots >= MIN_RECORD_SLOTS else _variables
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

    results.append(entry)

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

    recursive_parser(args.url)

    output = group_by_domains(results)

    # to json
    as_json = typedload.dump(output)

    with open('./output/data.json', 'w') as fh:
        json.dump(as_json, fh)


if __name__ == '__main__':
    main()
