from collections import namedtuple

Record = namedtuple('Record',
                    ['domain',
                     'publisher_id',
                     'relationship',
                     'certification_id',
                     'num_faults',
                     'faults'])

Variable = namedtuple('Variable', 'key value num_faults faults')

Fault = namedtuple('Fault', 'level reason hint')

Input = namedtuple('Input', 'tokens size')
