import re
from collections import defaultdict
from itertools import takewhile
from typing import List, Tuple

from lib import validators
from lib.entities import Record, Input, Variable, Fault, ErrorLevel
from lib.vars import VALID_VARIABLES, NUM_MIN_RECORD_SLOTS, NUM_VARIABLE_SLOTS, VALID_RELATIONSHIPS


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
        origin = None

        if num_slots >= NUM_MIN_RECORD_SLOTS:
            origin = 'record'
        elif num_slots == NUM_VARIABLE_SLOTS:
            origin = 'variable'

        yield origin, tokens, line


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
        if origin is None:
            yield None

        faults: List[Fault] = []

        if origin == 'record':
            domain, publisher_id, relationship, *cid = fields

            # check domain format
            if not validators.domain(domain):
                faults.append(Fault(
                    level=ErrorLevel.DANG,
                    reason=f'unexpected format',
                    hint=None,
                ))

            if relationship.upper() not in VALID_RELATIONSHIPS:
                faults.append(Fault(
                    level=ErrorLevel.DANG,
                    reason='unexpected relationship',
                    hint=VALID_RELATIONSHIPS,
                ))

            if not domain.islower():
                faults.append(Fault(
                    level=ErrorLevel.WARN,
                    reason='domain must be in lower case',
                    hint=domain.lower(),
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
        else:
            key, value = fields

            if key.upper() not in VALID_VARIABLES:
                faults.append(Fault(
                    level=ErrorLevel.DANG,
                    reason='unexpected variable',
                    hint=VALID_VARIABLES,
                ))

            if key.upper() == 'SUBDOMAIN':
                if not value.islower():
                    faults.append(Fault(
                        level=ErrorLevel.WARN,
                        reason='domain must be in lower case',
                        hint=value.lower(),
                    ))

                if not validators.domain(value):
                    faults.append(Fault(
                        level=ErrorLevel.DANG,
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
