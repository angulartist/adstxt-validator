"""
ADS.TXT Parser
doc: https://iabtechlab.com/wp-content/uploads/2019/03/IAB-OpenRTB-Ads.txt-Public-Spec-1.0.2.pdf
doc_ver: 1.0.2
"""
import argparse
import json
from urllib.parse import urlparse

import requests
import typedload
from consecution import Pipeline, GlobalState

from lib.nodes import ValidateRecordsNode, ValidateVariablesNode, FilterCommentsNode, TokenizeNode, AggregateNode, \
    TrimSpacesNode, CountLinesNode, orchestrate, FilterOutliersNode, MarkDuplicatesNode, FilterEmptyLinesNode

# node shared state
global_state = GlobalState(
    results=[],
    next_locations=[],
    fingerprints=[],
    lines=0
)


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
        print('Resolver failed:', e)


def recursive_parser(url: str = None, sld: bool = False):
    """
    Parse ads.txt
    :param url:
    :param sld:
    :return:
    """
    document, (scheme, netloc) = resolve(url)

    # extraction pipeline
    pipe = Pipeline(
        FilterCommentsNode('remove comments', cs='#')
        | TrimSpacesNode('trim all whitespaces')
        | CountLinesNode('add line number')
        | FilterEmptyLinesNode('filter out empty line')
        | TokenizeNode('tokenize lines')
        |
        [
            ValidateRecordsNode('get_recs'),
            ValidateVariablesNode('get_vars'),
            FilterOutliersNode('outliers'),
            orchestrate
        ]
        | MarkDuplicatesNode('mark duplicated')
        | AggregateNode(
            'create entries',
            source=netloc,
            sub_level_domain=sld),
        global_state=global_state)

    # convert raw text to list of lines
    lines = document.rsplit('\n')
    # here we go
    pipe.consume(lines)

    # get subdomain ads.txt for SLD only
    # (sub-domains redirect are not allowed)
    # -- Only root domains should refer crawlers
    # to subdomains. Subdomains should not refer to
    # other subdomains. -- (IAB)
    if sld:
        def get_next_location(sub_domain):
            return f'{scheme}://{sub_domain}/ads.txt'

        # extract optional subdomains ads.txt from the root domain
        next_locations = filter(
            lambda x: x != url,
            map(get_next_location, global_state.next_locations)
        )

        # recursive calls
        for location in next_locations:
            recursive_parser(location)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url',
                        help='The URL that points to the ads.txt',
                        required=True,
                        type=str)

    args = parser.parse_args()

    recursive_parser(args.url, sld=True)

    # to json
    as_json = typedload.dump(global_state.results)

    with open('./output/data.json', 'w') as fh:
        json.dump(as_json, fh)


if __name__ == '__main__':
    main()
