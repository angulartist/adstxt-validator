import re
from itertools import takewhile
from typing import List

from consecution import Node

from lib.decorators import yell
from lib.entities import Record, Input, Variable, Fault, Entry
from lib.validators import check_in_set, check_domain
from lib.vars import VALID_VARIABLES, NUM_MIN_RECORD_SLOTS, NUM_VARIABLE_SLOTS, VALID_RELATIONSHIPS


def orchestrate(item):
    tokens, num_slots, line = item

    if num_slots >= NUM_MIN_RECORD_SLOTS:
        return 'get_recs'
    elif num_slots == NUM_VARIABLE_SLOTS:
        return 'get_vars'
    else:
        return None


class TrimNode(Node):
    @yell
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

    @yell
    def process(self, item: str):
        cleaned = '\n'.join(map(
            self.go(),
            item.splitlines()
        ))

        self._push(cleaned)


class LineNode(Node):
    @yell
    def process(self, item: str):
        line = self.global_state.lines

        self._push((item, line))

        self.global_state.lines += 1

    def end(self):
        self.global_state.lines = 0


class TokenizeNode(Node):
    @yell
    def process(self, item: str):
        string, line = item

        if not re.match(r'^\s*$', string):
            tokens = re.split(',|=', string)
            input_ = Input(tokens, len(tokens), line)

            self._push(input_)


class AggregateNode(Node):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.source = kwargs['source']
        self.sub_level_domain = kwargs['sub_level_domain']
        self.entry = None

    def begin(self):
        if not self.sub_level_domain:
            self.global_state.next_locations = []

        self.entry: Entry = Entry(
            source=self.source,
            sub_level_domain=self.sub_level_domain
        )

    @yell
    def process(self, item):
        self.entry.put(item)

    def end(self):
        self.global_state.next_locations = self.entry.sub_domains
        self.global_state.results.append(self.entry)


class ValidateVariablesNode(Node):
    @yell
    def process(self, item):
        faults: List[Fault] = []

        tokens, num_tokens, line = item

        key, value = tokens

        faults = check_in_set(key.upper(), set_=VALID_VARIABLES, faults=faults)

        if key.upper() == 'SUBDOMAIN':
            faults = check_domain(value, faults=faults)

        variable = Variable(
            line=line,
            key=key,
            value=value,
            num_faults=len(faults),
            faults=faults,
        )

        self._push(variable)


class ValidateRecordsNode(Node):
    @yell
    def process(self, item):
        faults: List[Fault] = []

        tokens, num_tokens, line = item

        domain, publisher_id, relationship, *cid = tokens

        faults = check_in_set(relationship.upper(), set_=VALID_RELATIONSHIPS, faults=faults)

        # check domain format
        faults = check_domain(domain, faults=faults)

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
