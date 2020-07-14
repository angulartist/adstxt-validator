import re

from lib.entities import Fault, ErrorLevel

pattern = re.compile(
    r'^(?:[a-zA-Z0-9]'
    r'(?:[a-zA-Z0-9-_]{0,61}[A-Za-z0-9])?\.)'
    r'+[A-Za-z0-9][A-Za-z0-9-_]{0,61}'
    r'[A-Za-z]$'
)


def to_unicode(obj, charset='utf-8', errors='strict'):
    if obj is None:
        return None
    if not isinstance(obj, bytes):
        return str(obj)
    return obj.decode(charset, errors)


def domain(string):
    try:
        return pattern.match(to_unicode(string).encode('idna').decode('ascii'))
    except (UnicodeError, AttributeError):
        return False


def check_in_set(item, *, set_, faults: list):
    if item not in set_:
        fault: Fault = Fault(
            level=ErrorLevel.DANG,
            reason='unexpected value',
            hint=set_)

        faults.append(fault)

    return faults
