"""Provide decorator interfaces for the Django cache backend, similar
to util.functional.memoize, except that results are stored in the
Django caching backend (eg. memcached), and the results are shared
with everybody using that backend.

In order to account for this global nature, a namespace (a unique and
consistent picklable Python object) must be provided when using this
API. Also a timeout (in seconds) must be provided.

For best results, return a string representation in a custom cache key function.
This prevents inconsisitencies that may arise from pickling ambiguities.

  @cache('expensive_computation', 60*60)
  def expensive_computation(param):
      ...
  
  @cache_key('another_expensive_computation', 60*60, lambda param: str(param // 2))
  def another_expensive_computation(param):
      ...
  
  expensive_computation(1)  # (a) runs expensive_computation(1)
  expensive_computation(2)  # (b) runs expensive_computation(2)
  expensive_computation(1)  # (c) returns the cached results from (a)

  another_expensive_computation(2)  # (d) runs another_expensive_comptuation(2)
  another_expensive_computation(3)  # (e) returns the cached results from (d)"""

import inspect
from decorator import decorator

import django.core.cache as dcache

from . import digest

__all__ = ['cache_', 'cache_key_', 'cache', 'cache_key']


def _mk_key(ns, obj):
    """ Combines namespace and a 128 bit hash of object into a compact string.
    obj will only be pickled if it is not already a string. """
    if isinstance(obj, basestring):
        hash = digest.pydigest_str('%s:%s' % (ns, obj))
    else:
        hash = digest.pydigest((ns, obj))
    return 'util.cache.' + hash


def _cache(key, timeout, fun, *args, **kwargs):
    value = dcache.cache.get(key)
    if value is None:
        try:
            value = fun(*args, **kwargs)
            dcache.cache.set(key, value, timeout)
        except:
            # Always invalidate cache on any exception.
            dcache.cache.delete(key)
            raise
    return value

def cache_(ns, timeout, fun, *args, **kwargs):
    """Return the results of ``fun(*args, **kwargs)'', using a cached
    version if available, and caching the result if it is run. `ns'
    specifies the namespace and is any picklable Python object that
    uniquely identifies this invocation. The *global* cache key is
    derived from the namespace, `*args' and `**kwargs'."""
    if hasattr(fun, '_cache_keyfunc'):
        key = fun._cache_keyfunc(*args, **kwargs)
    else:
        key = args, frozenset(kwargs.iteritems())
    return _cache(_mk_key(ns, key), timeout, fun, *args, **kwargs)

def cache_key_(ns, timeout, keyfun, fun, *args, **kwargs):
    """Cache, but specify a key function."""
    key = keyfun(*args, **kwargs)
    return _cache(_mk_key(ns, key), timeout, fun, *args, **kwargs)

def cache(ns, timeout):
    """Return a decorator to cache the results of the decorated
    function for the namespace `ns'."""
    @decorator
    def wrapper(fun, *args, **kwargs):
        return cache_(ns, timeout, fun, *args, **kwargs)

    return wrapper

def cache_key(ns, timeout, keyfunc):
    """Return a decorator to cache the results of the decorated
    function for the given namespace `ns', keyed by `keyfunc', a
    function that returns a key given the arguments of the decorated
    function."""
    def decorate(fun):
        fun._cache_keyfunc = keyfunc
        return cache(ns, timeout)(fun)

    return decorate

def invalidate_cache_key(ns, key):
    """Invalidates an item in the cache that was made using cache_key for
    the given namespace `ns', keyed by `key'."""
    dcache.cache.delete(_mk_key(ns, key))
