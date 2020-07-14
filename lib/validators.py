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


def check_domain(item: str, *, faults: list):
    if not item.islower():
        faults.append(Fault(
            level=ErrorLevel.WARN,
            reason=f'Domain: Must be in lower case: {item}',
            hint=item.lower(),
        ))

    if not domain(item):
        faults.append(Fault(
            level=ErrorLevel.DANG,
            reason=f'Domain: Unexpected format: {item}',
            hint=None
        ))

    return faults


def check_in_set(item, *, field, set_, faults: list):
    if not item:
        fault: Fault = Fault(
            level=ErrorLevel.DANG,
            reason=f'Field is mandatory: {field}',
            hint=set_)

        faults.append(fault)

    if item and item not in set_:
        fault: Fault = Fault(
            level=ErrorLevel.DANG,
            reason=f'Unexpected value: {item} (for: {field})',
            hint=set_)

        faults.append(fault)

    return faults
