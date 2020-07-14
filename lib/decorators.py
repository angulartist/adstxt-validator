from functools import wraps


def yell(fn):
    @wraps(fn)
    def wrapper(*args):
        cls, item = args

        print('Node:', cls.__class__.__name__, '|', cls.name, '| processing:', item)

        return fn(*args)

    return wrapper
