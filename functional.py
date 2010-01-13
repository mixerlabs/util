"""fun(ctional) stuff."""

import copy
from decorator import decorator
import functools
import os
import types
import operator

from util import sym
from util.iter import groupby_safe

__all__ = ['flatten', 'flatmap', 'deleted', 'updated', 'switch',
           'wrapgen', 'times', 'mapreduce', 'memoize', 'safe',
           'pick', 'unique', 'nonrepeated', 'nonrepeated_sorted',
           'compose', 'compose_', 'uncurry']

def concat(l):
    """Flatten lists by one ([[l]] -> [l])."""
    return [v for ll in l for v in ll]

def flatten(l):
    """Flatten list `l'"""

    res = []
    for x in l:
        if isinstance(x, list):
            res += flatten(x)
        else:
            res.append(x)

    return res

def flatmap(fun, l):
    """flatten(map(`fun', `l')) """
    return flatten(map(fun, l))

def deleted(d, *args):
    new = copy.copy(d)

    if isinstance(d, dict):
        for key in args:
            new.pop(key, None)
    else:
        for val in args:
            try:
                new.remove(val)
            except ValueError:
                pass

    return new

def updated(d, *args, **kwargs):
    new = copy.copy(d)
    if args:
        assert len(args) == 1
        new.update(args[0])
    new.update(**kwargs)
    return new

def updated_default(d, **kwargs):
    """Like updated(), but works like setdefault instead."""
    new = copy.copy(d)
    for k, v in kwargs.iteritems():
        new.setdefault(k, v)

    return new

def switch(val, **kwargs):
    for k, v in kwargs.iteritems():
        if str(val) == k:
            return v
    else:
        if '_' in kwargs:
            return kwargs['_']
        else:
            raise sym.option_not_found.exc()

def wrapgen(maybe_gen):
    if isinstance(maybe_gen, types.GeneratorType):
        for g in maybe_gen:
            yield g
    elif maybe_gen is not None:
        yield maybe_gen

def times(n, fun):
    return [fun() for _ in xrange(n)]

def mapreduce(mapper, reducer, data):
    """A simple mapreduce implementation that takes an input `data' in
    either list or generator form, and outputs a generator with the
    mapreduced values."""

    mapped = {}
    for item in data:
        for key, val in mapper(item):
            mapped.setdefault(key, []).append(val)

    return ((key, reducer(key, vals)) for key, vals in mapped.iteritems())

def memoize(fun):
    def wrapper(fun, *args, **kwargs):
        if hasattr(fun, '_memoize_keyfunc'):
            key = fun._memoize_keyfunc(*args, **kwargs)
        else:
            key = args, frozenset(kwargs.iteritems())

        if not key in fun._cache:
            try:
                fun._cache[key] = fun(*args, **kwargs)
            except:
                # Always invalidate cache on exception.
                fun._cache.pop(key, None)
                raise

        return fun._cache[key]

    decorated = decorator(wrapper, fun)
    memoize_zap_cache(decorated)

    return decorated

def memoize_key(keyfunc):
    def decorate_function(fun):
        fun._memoize_keyfunc = keyfunc
        return memoize(fun)

    return decorate_function

def memoize_per_proc(fun):
    """Memoize per process."""
    def keyfunc(*args, **kwargs):
        return os.getpid(), args, frozenset(kwargs.iteritems())

    return memoize_key(keyfunc)(fun)

def memoize_zap_cache(fun):
    fun.undecorated._cache = {}

memoize_cache = {}
def memoize_(fun, *args, **kwargs):
    """An inline memoize."""

    key = fun, args, frozenset(kwargs.iteritems())

    if not key in memoize_cache:
        try:
            memoize_cache[key] = fun(*args, **kwargs)
        except:
            # Always invalidate cache on exception.
            memoize_cache.pop(key, None)
            raise

    return memoize_cache[key]

# We name "singleton" versions of memoize, too, in order to
# distinguish between the usage of memoize as a caching mechanism (as
# in true memoization) and usage where correctness relies on the
# underlying function being called ones (singletons). Right now they
# are equivalent, but it's possible and indeed likely that in the
# future the "memoize" variants will store weakrefs as to allow them
# to be garbage collected aggressively.
singleton, singleton_key, singleton_per_proc, singleton_ = (
    memoize, memoize_key, memoize_per_proc, memoize_
)

def memoizei(meth):
    """A version of memoize that caches data on an *instance* (we
    assume the instance variable is the first passed), thus allowing
    for garbage collection together with the instance."""
    def wrapper(meth, self, *args, **kwargs):
        if hasattr(meth, '_memoize_keyfunc'):
            key = meth, fun._memoize_keyfunc(*args, **kwargs)
        else:
            key = meth, args, frozenset(kwargs.iteritems())

        if not hasattr(self, '__cache'):
            self.__cache = {}

        if not key in self.__cache:
            # Here, python __-rules help us out for once.
            try:

                self.__cache[key] = meth(self, *args, **kwargs)
            except:
                # Always invalidate cache on exception.
                self.__cache.pop(key, None)
                raise

        return self.__cache[key]

    return decorator(wrapper, meth)

def safe(fn, value_if_error):
    """
    Executes fn; if that throws an exception, returns value_if_error
    """
    try:
        return fn()
    except:
        return value_if_error

def dictmap(fun, d):
    return dict((fun(k, v) for k, v in d.iteritems()))

def pick(which, lst):
    return [ent[which] for ent in lst]

def pick_n(*args):
    """
    Similar to pick except that you can pass in multiple keys
    to pick with and get an array of arrays back
    """
    return [map(lambda x: ent[x], args[:-1]) for ent in args[-1]]

# Pickers:
pickr0 = operator.itemgetter(0)
pickr1 = operator.itemgetter(1)

def apick(which, lst):
    return [getattr(ent, which) for ent in lst]

def mk_item_picker(which):
    return lambda ent: ent[which]

def mk_attr_picker(which):
    return lambda ent: getattr(ent, which)

def split(pred, lst):
    yes, no = [], []
    for item in lst:
        if pred(item):
            yes.append(item)
        else:
            no.append(item)

    return yes, no

def crossproduct(*seqs):
    """Returns the cross product of the passed-in sequences."""

    if len(seqs) == 1:
        return [(s,) for s in seqs[0]]
    else:
        this, rest = seqs[0], crossproduct(*seqs[1:])
        return [(x,) + y for x in this for y in rest]

def permutations(seq):
    """Return permutations of elements in the passed-in sequence."""
    howmany = len(seq)

    # Kind of ugly: get the unique set of indices from the
    # crossproduct of indices.
    for indices in crossproduct(*([range(howmany)]*howmany)):
        if len(set(indices)) == howmany:
            yield tuple(seq[i] for i in indices)


def unique(seq, key=lambda x: x):
    """Returns the set of unique elements in `seq'. ``set(seq)''
    doesn't always work because not all Python objects are
    hashable. We just require comparable."""

    seq = sorted(seq, key=key)

    def fil(a, b):
        if len(a) == 0 or key(a[-1]) != key(b):
            return a + [b]
        else:
            return a

    return reduce(fil, seq, [])

def nonrepeated(seq):
    def fil(a, b):
        if b not in a:
            return a + [b]
        else:
            return a

    return reduce(fil, seq, [])

def nonrepeated_sorted(sorted_seq):
    def fil(a, b):
        if a:
            if a[-1] == b:
                return a
            else:
                return a + [b]
        else:
            return [b]
    return reduce(fil, sorted_seq, [])

def assoc(seq, key, *orelse):
    """Looks up `key' in the given sequence of (key, value) tuples,
    raising KeyError if not found. If `orelse' is specified, return
    that instead of raising a KeyError."""
    for k, v in seq:
        if key == k:
            return v
    else:
        if orelse:
            return orelse[0]
        else:
            raise KeyError

def rassoc(seq, key, *orelse):
    """Like assoc, but the sequence is (value, key)."""
    for v, k in seq:
        if key == k:
            return v
    else:
        if orelse:
            return orelse[0]
        else:
            raise KeyError
        
def walk_obj(obj, cb, skip_types=(list, tuple, dict)):
    """ Recurse through dicts, tuples, and lists of a python object, calling
    cb on every node except those of type skip_types. """
    if not isinstance(obj, skip_types):
        cb(obj)
    if isinstance(obj, (list, tuple)):
        for v in obj:
            walk_obj(v, cb, skip_types)
    elif isinstance(obj, dict):
        for v in obj.itervalues():
            walk_obj(v, cb, skip_types)

def compose(*fs):
    """Compose any number of functions. All but the last function must
    accept one argument, which is the output of the previous
    function. This is in ``onion-order'', so

      compose(f1, f2, f3)(*x) == f1(f2(f3(*x)))"""
    return reduce(lambda f1, f2: (lambda *a, **kw: f2(f1(*a, **kw))), fs[::-1])

def compose_(*fs):
    """Like ``compose'', but splat the arguments from the previous
    function (and thus these must also be iterables)"""
    return reduce(lambda f1, f2: (lambda *a, **kw: f2(*f1(*a, **kw))), fs[::-1])

def uncurry(f):
    """Uncurry `f', thus:

      ``f(a, b, c)'' becomes ``f((a, b, c))''"""

    return lambda x: f(*x)
