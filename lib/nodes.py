import re
from itertools import takewhile
from typing import List, Union

from consecution import Node

from lib.decorators import yell
from lib.entities import Record, Input, Variable, Fault, Entry
from lib.validators import check_in_set, check_domain
from lib.vars import VALID_VARIABLES, NUM_MIN_RECORD_SLOTS, NUM_VARIABLE_SLOTS, VALID_RELATIONSHIPS


def orchestrate(item: Input):
    """
    Takes an input and defines the next
    pipeline operation according to its nature.
    :param item: Input
    :return: next pipeline op signature
    """
    if not isinstance(item, Input):
        raise ValueError(f'Expected Input, received {type(item)}')

    if item.num_slots >= NUM_MIN_RECORD_SLOTS:
        return 'get_recs'
    elif item.num_slots == NUM_VARIABLE_SLOTS:
        return 'get_vars'
    else:
        return 'outliers'


class FilterOutliersNode(Node):
    """ Just skip outliers :) """

    def process(self, item):
        pass


class TrimSpacesNode(Node):
    """ Removes all whitespaces from the passed string. """

    @yell
    def process(self, item: str):
        trimmed = item.replace(' ', '').strip()

        self._push(trimmed)


class FilterCommentsNode(Node):
    """ Removes comments from the passed string. """

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self.cs = kwargs['cs']

    def begin(self):
        self.cs = '#'

    def go(self):
        """
        Gathers each character until we reach an in-line comment.
        :return: str
        """
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


class CountLinesNode(Node):
    """
    Affects a line number to the current string.
    Uses the global state to track the number of lines.
    """

    @yell
    def process(self, item: str):
        line = self.global_state.lines

        self._push((item, line))

        # incr the global counter
        self.global_state.lines += 1

    def end(self):
        # reset the global counter (for each domain iteration)
        self.global_state.lines = 0


class FilterEmptyLinesNode(Node):
    """ Filters out empty strings """

    @yell
    def process(self, item):
        string, _ = item

        # if not empty/blank
        # then push it downstream
        if not re.match(r"^\s*$", string):
            self._push(item)


class TokenizeNode(Node):
    """ Tokenize the given string and returns an Input. """

    @yell
    def process(self, item: str):
        string, line = item

        # split comma separated strings and key=value pairs
        # (records: , | variables: = | extensions: ;)
        tokens = [token for token in re.split('[;,=]', string) if token]
        # create a new Input
        input_ = Input(tokens, len(tokens), line)

        self._push(input_)


class MarkDuplicatesNode(Node):
    def begin(self):
        self.global_state.fingerprints = []

    @yell
    def process(self, item: Union[Record, Variable]):
        # track duplicated lines
        if item.identity in self.global_state.fingerprints:
            item.duplicated = True
        else:
            self.global_state.fingerprints.append(item.identity)

        # push item downstream
        self._push(item)

    def end(self):
        # reset duplicated tracking list
        self.global_state.fingerprints = []


class AggregateNode(Node):
    """ Creates a new Entry and merge its records and variables. """

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.source = kwargs['source']
        self.sub_level_domain = kwargs['sub_level_domain']
        self.entry = None

    def begin(self):
        # if current domain is not the sld
        # then no more iteration will be done
        if not self.sub_level_domain:
            self.global_state.next_locations = []

        # build a new Entry at the start of the operation
        self.entry: Entry = Entry(
            source=self.source,
            sub_level_domain=self.sub_level_domain
        )

    @yell
    def process(self, item: Union[Record, Variable]):
        # update Entry's internal storage
        self.entry.put(item)

    def end(self):
        # if current domain is the sld
        # then prepare optional sub_domains for
        # the next iterations
        if self.sub_level_domain:
            self.global_state.next_locations = self.entry.sub_domains

        # add the Entry to the global results list
        self.global_state.results.append(self.entry)


class ValidateVariablesNode(Node):
    """ For each Input, validate format and push a Variable downstream. """

    @yell
    def process(self, item: Input):
        faults: List[Fault] = []

        tokens, num_tokens, line = item

        key, value = tokens

        # validate given variable key
        faults = check_in_set(
            key.upper(),
            field='value',
            set_=VALID_VARIABLES,
            faults=faults)

        # if variable is a subdomain
        # then validate subdomain format
        if key.upper() == 'SUBDOMAIN':
            faults = check_domain(value, faults=faults)

        variable = Variable(
            line=line,
            key=key,
            value=value,
            num_faults=len(faults),
            faults=faults,
            duplicated=False,
        )

        self._push(variable)


class ValidateRecordsNode(Node):
    """ For each Input, validate format and push a Record downstream. """

    @yell
    def process(self, item: Input):
        faults: List[Fault] = []

        tokens, num_tokens, line = item

        domain, publisher_id, relationship, *extra = tokens

        # validate relationship
        faults = check_in_set(
            relationship.upper(),
            field='relationship',
            set_=VALID_RELATIONSHIPS,
            faults=faults)

        # validate *domain format
        faults = check_domain(domain, faults=faults)

        # if any Certification ID
        # first "extra" field must be the Certification ID
        certification_id = extra[0] if extra else None

        # if any extension fields
        # after Certification ID: should be extensions
        extensions = extra[1:] if extra else []

        record = Record(
            line=line,
            domain=domain,
            publisher_id=publisher_id,
            relationship=relationship,
            certification_id=certification_id,
            extensions=extensions,
            num_faults=len(faults),
            faults=faults,
            duplicated=False,
        )

        self._push(record)
