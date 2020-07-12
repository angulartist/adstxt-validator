import re
from collections import defaultdict
from enum import Enum
from itertools import takewhile
from typing import List

from lib import validators
from lib.entities import Record, Input, Variable, Fault


class Level(Enum):
    INFO = 1
    WARN = 2
    DANG = 3


# Available relationships
VALID_RELATIONSHIPS = {'DIRECT', 'RESELLER'}
# Available variables
VALID_VARIABLES = {'CONTACT', 'SUBDOMAIN'}


# def mark_duplicated(x: OutputEntity, items: List[OutputEntity]):
#     x.duplicated = True if items.count(x) > 1 else False
#
#     return x


def group_by_domains(items):
    for item in items:
        grouped = defaultdict(list)
        for x in item['results']['recs']:
            grouped[x.domain.lower()].append(x)
        item['results']['recs'] = grouped

    return items


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
    string_ = string_.replace(' ', '')
    tokens_ = re.split(',|=', string_)
    tokens_ = [token for token in tokens_]

    return Input(tokens_, len(tokens_))


def get_vars(tmp_variables):
    variables: List[Variable] = []

    for (key, value), line in tmp_variables:
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
            line=line,
            key=key,
            value=value,
            num_faults=len(faults),
            faults=faults,
        )

        variables.append(variable)

    return variables


def get_records(tmp_records):
    records: List[Record] = []

    for (domain, publisher_id, relationship, *cid), line in tmp_records:
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

        if relationship.upper() not in VALID_RELATIONSHIPS:
            faults.append(Fault(
                level=Level.DANG,
                reason='unexpected relationship',
                hint=VALID_RELATIONSHIPS,
            ))

        certification_id = cid[0] if cid else None

        record = Record(
            line=line,
            domain=domain,
            publisher_id=publisher_id,
            relationship=relationship,
            certification_id=certification_id,
            num_faults=len(faults),
            faults=faults,
        )

        records.append(record)

    return records
