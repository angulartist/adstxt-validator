"""
ADS.TXT Parser
doc: https://iabtechlab.com/wp-content/uploads/2019/03/IAB-OpenRTB-Ads.txt-Public-Spec-1.0.2.pdf
doc_ver: 1.0.2
"""
import argparse
import json
import re
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from itertools import takewhile
from typing import List

import requests
import typedload

from lib import validators


class Level(Enum):
    INFO = 1
    WARN = 2
    DANG = 3


# Available relationships (see below)
VALID_RELATIONSHIPS = {'DIRECT', 'RESELLER'}
# Available variables
VALID_VARIABLES = {'CONTACT', 'SUBDOMAIN'}

Input = namedtuple('Input', 'tokens size')

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
Record = namedtuple('Record',
                    ['domain',
                     'publisher_id',
                     'relationship',
                     'certification_id',
                     'num_faults',
                     'faults'])

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
Variable = namedtuple('Variable', 'key value num_faults faults')


# Mutable data structure
@dataclass
class OutputRecord:
    type: str
    data: object
    duplicated: bool


Fault = namedtuple('Fault', 'level reason hint')


def strip_comments(cs):
    """
    Takes a string and removes comments.
    :param cs: str - Comment tokens
    :return: str - Parsed line
    """

    def go(cs_):
        return lambda s: ''.join(
            takewhile(lambda c: c not in cs_, s)
        ).strip()

    return lambda txt: '\n'.join(map(
        go(cs),
        txt.splitlines()
    ))


def tokenize(string):
    """
    Takes a string and return an Input.
    :param string: str - Line to parse
    :return: Input - Input that contains tokens
    """
    string_ = strip_comments('#')(string)
    tokens_ = re.split(',|=', string_)
    tokens_ = [token.strip() for token in tokens_]

    return Input(tokens_, len(tokens_))


def build_variables(tmp_variables):
    variables: List[Variable] = []
    for key, value in tmp_variables:
        faults: List[Fault] = []

        if key.upper() not in VALID_VARIABLES:
            faults.append(Fault(
                level=Level.DANG,
                reason='unexpected variable',
                hint=VALID_VARIABLES,
            ))

        if key.upper() == 'SUBDOMAIN':
            if not value.islower():
                faults.append(Fault(
                    level=Level.WARN,
                    reason='domain must be in lower case',
                    hint=value.lower(),
                ))

            if not validators.domain(value):
                faults.append(Fault(
                    level=Level.DANG,
                    reason='unexpected format',
                    hint=None
                ))

        variable = Variable(
            key=key,
            value=value,
            num_faults=len(faults),
            faults=faults,
        )

        variables.append(variable)

    return variables


def build_records(tmp_records):
    records: List[Record] = []

    for domain, publisher_id, relationship, *cid in tmp_records:
        faults: List[Fault] = []

        if not domain.islower():
            faults.append(Fault(
                level=Level.WARN,
                reason='domain must be in lower case',
                hint=domain.lower(),
            ))

        # check domain format
        if not validators.domain(domain):
            faults.append(Fault(
                level=Level.DANG,
                reason=f'unexpected format',
                hint=None,
            ))

        if relationship not in VALID_RELATIONSHIPS:
            faults.append(Fault(
                level=Level.DANG,
                reason='unexpected relationship',
                hint=VALID_RELATIONSHIPS,
            ))

        certification_id = cid[0] if cid else None

        record = Record(
            domain=domain,
            publisher_id=publisher_id,
            relationship=relationship,
            certification_id=certification_id,
            num_faults=len(faults),
            faults=faults,
        )

        records.append(record)

    return records


def mark_duplicated(x: OutputRecord, items: List[OutputRecord]):
    x.duplicated = True if items.count(x) > 1 else False

    return x


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help='The URL that points to the ads.txt', required=True)

    args = parser.parse_args()

    try:
        resp = requests.get(args.url)
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

        output = [OutputRecord(type='record', data=record, duplicated=False)
                  for record in build_records(tmp_records)]

        output += [OutputRecord(type='variable', data=variable, duplicated=False)
                   for variable in build_variables(tmp_variables)]

        # mark duplicated records
        output = [mark_duplicated(x, output) for x in output]

        # to json
        output = typedload.dump(output)

        with open('./output/data.json', 'w') as fh:
            json.dump(output, fh)


if __name__ == '__main__':
    main()
