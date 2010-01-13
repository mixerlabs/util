"""Coroutine utilities."""

from decorator import decorator

@decorator
def coroutine(f, *a, **kw):
    """This decorator starts the coroutine for us."""
    i = f(*a, **kw)
    i.next()
    return i
