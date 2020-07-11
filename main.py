"""
ADS.TXT Parser
doc: https://iabtechlab.com/wp-content/uploads/2019/03/IAB-OpenRTB-Ads.txt-Public-Spec-1.0.2.pdf
doc_ver: 1.0.2
"""
import argparse
import json
import time
from urllib.parse import urlparse

import requests
import typedload

from lib.entities import Record
from lib.transformers import tokenize, build_records, build_variables

# RECORDS

'''
domain:
(Required) The canonical domain name of the
SSP, Exchange, Header Wrapper, etc system that
bidders connect to. This may be the operational
domain of the system, if that is different than the
parent corporate domain, to facilitate WHOIS and
reverse IP lookups to establish clear ownership of
the delegate system. Ideally the SSP or Exchange
publishes a document detailing what domain name
to use.

publisher_id:
(Required) The identifier associated with the seller
or reseller account within the advertising system in
field #1. This must contain the same value used in
transactions (i.e. OpenRTB bid requests) in the
field specified by the SSP/exchange. Typically, in
OpenRTB, this is publisher.id. For OpenDirect it is
typically the publisher’s organization ID.

relationship:
(Required) An enumeration of the type of account.
A value of ‘DIRECT’ indicates that the Publisher
(content owner) directly controls the account
indicated in field #2 on the system in field #1. This
tends to mean a direct business contract between
the Publisher and the advertising system. A value
of ‘RESELLER’ indicates that the Publisher has
authorized another entity to control the account
indicated in field #2 and resell their ad space via
the system in field #1. Other types may be added
in the future. Note that this field should be treated
as case insensitive when interpreting the data.

certification_id:
(Optional) An ID that uniquely identifies the
advertising system within a certification authority
(this ID maps to the entity listed in field #1). A
current certification authority is the Trustworthy
Accountability Group (aka TAG), and the TAGID
would be included here [11].
'''

# VARIABLES

'''
contact:
(Optional) Some human readable contact
information for the owner of the file. This may be
the contact of the advertising operations team for
the website. This may be an email address,
phone number, link to a contact form, or other
suitable means of communication.

subdomain:
(Optional) A machine readable subdomain pointer
to a subdomain within the root domain, on which
an ads.txt can be found. The crawler should fetch
and consume associate the data to the
subdomain, not the current domain. This referral
should be exempt from the public suffix truncation
process. Only root domains should refer crawlers
to subdomains. Subdomains should not refer to
other subdomains. 
'''

results = []


def recursive_parser(url):
    print('Location:', url)

    scheme, netloc, *rest = urlparse(url)

    output = {
        'domain': netloc,
        'results': {
            'records': [],
            'variables': {
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

        inputs = [tokenize(string) for string in text.rsplit('\n')]

        tmp_records = []
        tmp_variables = []

        for tokens, size in inputs:
            if size > 2:
                tmp_records.append(tokens)
            else:
                tmp_variables.append(tokens)

        records = build_records(tmp_records)
        variables = build_variables(tmp_variables)

        for data in records + variables:
            if isinstance(data, Record):
                output['results']['records'].append(data)
            else:
                if data.key.upper() == 'SUBDOMAIN':
                    output['results']['variables']['sub_domains'].append(data)
                else:
                    output['results']['variables']['contacts'].append(data)

    results.append(output)

    for _, domain, *rest in output['results']['variables']['sub_domains']:
        next_location = f'{scheme}://{domain}/ads.txt'
        recursive_parser(next_location)
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url',
                        help='The URL that points to the ads.txt',
                        required=True,
                        type=str)

    args = parser.parse_args()

    recursive_parser(args.url)

    # to json
    as_json = typedload.dump(results)

    with open('./output/data.json', 'w') as fh:
        json.dump(as_json, fh)


if __name__ == '__main__':
    main()
