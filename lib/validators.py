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


def domain(string: str):
    try:
        return pattern.match(to_unicode(string).encode('idna').decode('ascii'))
    except (UnicodeError, AttributeError):
        return False


def check_domain(string: str, *, faults: list):
    if not string.islower():
        faults.append(Fault(
            level=ErrorLevel.WARN,
            reason='domain must be in lower case',
            hint=string.lower(),
        ))

    if not domain(string):
        faults.append(Fault(
            level=ErrorLevel.DANG,
            reason='unexpected format',
            hint=None
        ))

    return faults


def check_in_set(item, *, set_, faults: list):
    if item not in set_:
        fault: Fault = Fault(
            level=ErrorLevel.DANG,
            reason='unexpected value',
            hint=set_)

        faults.append(fault)

    return faults
