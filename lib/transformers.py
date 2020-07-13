import re
from collections import defaultdict
from enum import Enum
from itertools import takewhile
from typing import List, Tuple

from lib import validators
from lib.entities import Record, Input, Variable, Fault

NUM_MIN_RECORD_SLOTS = 3
NUM_VARIABLE_SLOTS = 2


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


def group_by_domains(items: list):
    for item in items:
        grouped = defaultdict(list)
        for x in item['results']['recs']:
            grouped[x.domain.lower()].append(x)
        item['results']['recs'] = grouped

    return items


def split_fn(text):
    return ((line, string) for line, string in enumerate(text.rsplit('\n')))


def orchestrator_fn(items):
    for tokens, num_slots, line in items:
        if num_slots >= NUM_MIN_RECORD_SLOTS:
            yield 'record', tokens, line
        elif num_slots == NUM_VARIABLE_SLOTS:
            yield 'var', tokens, line
        else:
            yield 'outlier', tokens, line


def strip_comments(splitted: List[Tuple], cs: str = '#'):
    def go(cs_):
        return lambda s: ''.join(
            takewhile(lambda c: c not in cs_, s)
        ).strip()

    for line, string in splitted:
        yield line, '\n'.join(map(
            go(cs),
            string.splitlines()
        ))


def tokenize(items):
    for line, string in items:
        # skip empty lines
        if not re.match(r'^\s*$', string):
            string = string.replace(' ', '').strip()
            tokens = re.split(',|=', string)
            tokens = [token for token in tokens]

            yield Input(tokens, len(tokens), line)


def get_records(items):
    for origin, fields, line in items:
        faults: List[Fault] = []

        if origin == 'record':
            domain, publisher_id, relationship, *cid = fields

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

            yield Record(
                line=line,
                domain=domain,
                publisher_id=publisher_id,
                relationship=relationship,
                certification_id=certification_id,
                num_faults=len(faults),
                faults=faults,
            )
        elif origin == 'var':
            key, value = fields

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

            yield Variable(
                line=line,
                key=key,
                value=value,
                num_faults=len(faults),
                faults=faults,
            )
        else:
            yield None
