"""Dictionary utilities."""

import environment

from .str import peel

def merge(*dicts):
    """Merge the given dictionaries in priority-order. This is useful
    for mix-ins.
    
    - one
    - two

    """
    return environment.mergedict(*dicts)

def getbypath(d, path):
    """Get a dictionary value from a path in dotted-notation
    form. ie.: getbypath(d, 'foo.bar.baz') === d['foo']['bar']['baz']"""
    key, rest = peel(path, '.')
    d = d[key]
    if rest:
        return getbypath(d, rest)
    else:
        return d

def getvalues(d, *keys):
    """Get the given keys from `d', and return them as a tuple. They
    all have to exist."""
    return tuple([d[key] for key in keys])

def filter_dict(filter_fn, d):
    for kv in d.items():
        if filter_fn(kv):
            del d[kv[0]]


def coerce_to_storage(d):
    """Turns a dict and its elemnts recursively into Storage objects."""
    return environment.coerce_to_storage(d)


def coerce_none_to_string(d):
    """Runs through a dict recursively and turns its None
    values into emptry strings."""
    return environment.coerce_none_to_string(d)

def setifnone(d, key, val):
    """Like, dict.setdefault, but also sets if the given value is
    defined as None."""
    if d.get(key, None) is None:
        d[key] = val

    return d[key]

def mapvalues(fun, d):
    """map()s values of the given dictionary."""
    return dict((k, fun(v)) for k, v in d.iteritems())
