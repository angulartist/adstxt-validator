import re
from itertools import takewhile
from typing import List

from consecution import Node

from lib import validators
from lib.entities import Record, Input, Variable, Fault, ErrorLevel, Origin, Entry
from lib.vars import VALID_VARIABLES, NUM_MIN_RECORD_SLOTS, NUM_VARIABLE_SLOTS, VALID_RELATIONSHIPS


class OrchestrateNode(Node):
    def process(self, item: Input):
        origin = None

        tokens, num_slots, line = item

        if num_slots >= NUM_MIN_RECORD_SLOTS:
            origin = Origin.RECORD
        elif num_slots == NUM_VARIABLE_SLOTS:
            origin = Origin.VARIABLE

        self._push((origin, tokens, line))


class TrimNode(Node):
    def process(self, item: str):
        trimmed = item.replace(' ', '').strip()

        self._push(trimmed)


class UncommentNode(Node):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.cs = kwargs['cs']

    def begin(self):
        self.cs = '#'

    def go(self):
        return lambda s: ''.join(
            takewhile(lambda c: c not in self.cs, s)
        ).strip()

    def process(self, item: str):
        cleaned = '\n'.join(map(
            self.go(),
            item.splitlines()
        ))

        self._push(cleaned)


class TokenizeNode(Node):
    def process(self, item: str):
        if not re.match(r'^\s*$', item):
            tokens = re.split(',|=', item)
            input_ = Input(tokens, len(tokens), 0)

            self._push(input_)


class AggregateNode(Node):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.source = kwargs['source']
        self.sub_level_domain = kwargs['sub_level_domain']
        self.entry = None

    def begin(self):
        self.entry: Entry = Entry(
            source=self.source,
            sub_level_domain=self.sub_level_domain
        )

    def process(self, item):
        self.entry.put(item)

    def end(self):
        self.global_state.next_locations = self.entry.sub_domains
        self.global_state.results.append(self.entry)


class ToVariablesNode(Node):
    def process(self, item):
        faults: List[Fault] = []

        origin, fields, line = item

        if origin == Origin.VARIABLE:
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

            variable = Variable(
                line=line,
                key=key,
                value=value,
                num_faults=len(faults),
                faults=faults,
            )

            self._push(variable)


class ToRecordsNode(Node):
    def process(self, item):
        faults: List[Fault] = []

        origin, fields, line = item

        if origin == Origin.RECORD:
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

            record = Record(
                line=line,
                domain=domain,
                publisher_id=publisher_id,
                relationship=relationship,
                certification_id=certification_id,
                num_faults=len(faults),
                faults=faults,
            )

            self._push(record)