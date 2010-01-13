import hashlib
import pickle

from util.str import compress_hex

def pydigest(load):
    """A generic digest function for any python object, by uniqueness
    of its pickled representation.
    
    WARNING - cPickle may be fast, but it does not return the same exact string
    for objects that are identical but contain items with different ref counts:  
    Example:
    >>> import cPickle
    >>> a = 'Hello', 'world'
    >>> cPickle.dumps(a)
    "(S'Hello'\nS'world'\ntp1\n."
    >>> b = 'Hello'
    >>> c = b, 'world'
    >>> cPickle.dumps(c)
    "(S'Hello'\np1\nS'world'\np2\ntp3\n."
    >>> cPickle.dumps(a)
    "(S'Hello'\np1\nS'world'\np2\ntp3\n."
    
    Pickle seems to work better, but from http://bugs.python.org/issue5518
    I'd be wary of relying on that also:
    
    >>> import pickle
    >>> a = 'Hello', 'world'
    >>> pickle.dumps(a)
    "(S'Hello'\np0\nS'world'\np1\ntp2\n."
    >>> b = 'Hello'
    >>> c = b, 'world'
    >>> pickle.dumps(c)
    "(S'Hello'\np0\nS'world'\np1\ntp2\n."
    >>> pickle.dumps(a)
    "(S'Hello'\np0\nS'world'\np1\ntp2\n."
    
    Simplejson also works, but that also has limitations. None of these work
    perfectly if there is a dictionary involved with arbitrary key ordering.
    """
    return compress_hex(
        hashlib.md5(pickle.dumps(load)).hexdigest()
    )

def pydigest_str(st):
    """A generic digest function for strings only. """
    return compress_hex(hashlib.md5(st).hexdigest())

