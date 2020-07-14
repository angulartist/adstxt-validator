from collections import namedtuple
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Union


class ErrorLevel(Enum):
    INFO = 1
    WARN = 2
    DANG = 3


class Origin(Enum):
    RECORD = 1
    VARIABLE = 2


Fault = namedtuple('Fault', 'level reason hint')

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


@dataclass
class Record:
    line: int
    domain: str
    publisher_id: str
    relationship: str
    certification_id: str
    faults: List[Fault]
    duplicated: bool
    num_faults: int = 0

    @property
    def identity(self):
        return hash(f'{self.domain}_{self.publisher_id}_{self.relationship}')


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


@dataclass
class Variable:
    line: int
    key: str
    value: str
    faults: List[Fault]
    duplicated: bool
    num_faults: int = 0

    @property
    def identity(self):
        return hash(f'{self.key}_{self.value}')


Input = namedtuple('Input', 'tokens num_slots line')


@dataclass(repr=True)
class Entry:
    source: str
    sub_level_domain: bool
    recs: List[Record] = field(default_factory=list)
    vars: List[Variable] = field(default_factory=list)

    def put(self, item: Union[Record, Variable]):
        switcher = {
            'Record': (
                lambda x: self.recs.append(x)
            ),
            'Variable': (
                lambda x: self.vars.append(x)
            )
        }

        switcher.get(item.__class__.__name__, 'Unknown type')(item)

    @property
    def sub_domains(self):
        return [x.value for x in self.vars if x.key.upper() == 'SUBDOMAIN']
