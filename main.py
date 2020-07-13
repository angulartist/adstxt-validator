"""
ADS.TXT Parser
doc: https://iabtechlab.com/wp-content/uploads/2019/03/IAB-OpenRTB-Ads.txt-Public-Spec-1.0.2.pdf
doc_ver: 1.0.2
"""
import argparse
import json
from concurrent.futures.thread import ThreadPoolExecutor
from urllib.parse import urlparse

import requests
import typedload
from pipetools import pipe

from lib.entities import Entry, Variable
from lib.transformers import get_records, tokenize, strip_comments, split_fn, orchestrator_fn

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

        return resp.text, (scheme, netloc)
    except Exception as e:
        print('resolver failed:', e)


def recursive_parser(url: str = None, sld: bool = False):
    """
    Parse ads.txt
    :param url:
    :param sld:
    :return:
    """
    document, (scheme, netloc) = resolve(url)

    entry: Entry = Entry(source=netloc, sub_level_domain=sld)

    _ = (document >
         pipe
         | split_fn
         | strip_comments
         | tokenize
         | orchestrator_fn
         | get_records
         | entry.put)

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
        next_locations = filter(
            lambda x: x != url,
            map(get_next_location, entry.sub_domains)
        )
        # recursive concurrent call
        with ThreadPoolExecutor(max_workers=4) as executor:
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

    # results = group_by_domains(results)

    # to json
    as_json = typedload.dump(results)

    with open('./output/data.json', 'w') as fh:
        json.dump(as_json, fh)


if __name__ == '__main__':
    main()
