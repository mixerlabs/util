"""Text & text formatting utilities."""

import textwrap
from .coerce import coerce

def ljustr(s, width, *args):
    """A truncating version of ljust."""
    return s.ljust(width, *args)[:width]

def rjustr(s, width, *args):
    """A truncating version of rjust."""
    s = s.rjust(width, *args)
    return s[-width:]

def fill(text, offset=0, width=70):
    """Wrapper around textwrap.fill, to fill with an offset."""
    return textwrap.fill(
        text, 
        initial_indent=' '*offset,
        subsequent_indent=' '*offset,
        width=width
    )

def ms_s_m(ms):
    """Print milliseconds, seconds or minutes, depending on the value
    of the argument."""
    if ms < 10000:
        return '%dms' % ms

    s = ms / 1000
    if s < 120:
        return '%ds' % s

    return '%dm' % s / 60

class PrettyFloat(float):
    def __repr__(self):
        return '%.15g' % self

def pretty_floats(item):
    """ Return a deep copy of item where all floats are replaced
    with 'pretty floats' that print like 15.1 instad of 15.0000000001
    If item is a list or a dict, it will be iterated recursively. """
    return coerce(item, float, PrettyFloat)

