"""iteration tools"""

import random
from itertools import groupby, islice, tee, repeat
from collections import defaultdict

def first(iterable):
    """Give me the first item in the iterable."""
    return iter(iterable).next()

def last(iterable):
    """Give me the last item in the iterable. This can be slow, of
    course."""
    val = None
    for val in iterable:
        pass

    return val

def groupby_safe(iterable, keyfunc):
    """Dispatching to groupby safely -- that is, we sort the input by
    the same (required) key function before passing it to groupby."""
    return groupby(sorted(iterable, key=keyfunc), keyfunc)

def item_counter(iterable, keyfunc=lambda item: item):
    """Counts the number of items in buckets keyed by keyfunc,
    which by default returns the items themselves (i.e. works
    for an indexable value)."""
    items = defaultdict(int)
    for item in iterable:
        items[keyfunc(item)] += 1

    return items.items()

class LazySeq(object):
    """A lazy seq based on the given iterable. Doesn't support `in',
    `+', `*'. Read-only."""

    def __init__(self, iterable, lenfun=lambda iterable: len(list(iterable))):
        self._iterable = iterable
        self._lenfun = lenfun

    def iterable(self):
        self._iterable, copy = tee(self._iterable)
        return copy

    def __getitem__(self, i):
        if type(i) is slice:
            return list(islice(self.iterable(), i.start, i.stop, i.step or 1))
        else:
            return islice(self.iterable(), i, i+1)

    def __len__(self):
        return self._lenfun(self.iterable())


def produce(fun, *args, **kwargs):
    """Returns an iterator that calls 'fun' successively. Like a
    deferred list produced by a given function."""
    while True:
        yield fun(*args, **kwargs)

def expand(iterableiterable):
    """expands the given iterator-iterator. the iterator yields more
    iterators, and we yield an iterator that flattens this
    relationship. Just one level."""
    return (v for iterable in iterableiterable for v in iterable)

def chunk(iterable, size):
    """Split `iterable' into chunks of `size' slices. Each item in the
    resulting iterator is another iterator for the given chunk."""
    iterable = iter(iterable)

    while True:
        offset = [0]

        def gen():
            while offset[0] < size:
                try:
                    offset[0] += 1
                    yield iterable.next()
                except StopIteration:
                    offset[0] = -1
                    raise

        yield gen()

        if offset[0] < 0:
            raise StopIteration
        else:
            # Discard the remaining.
            for _ in xrange(offset[0], size):
                iterable.next()

def sample(iterable, rate):
    """Sample the given iterable at the given rate."""
    for i in iterable:
        if random.uniform(0, 1) <= rate:
            yield i

def merge(iter1, iter2, cmp=cmp):
    '''Merge elements in two sorted iterators in sorted order.'''
    iter1, iter2 = peekiter(iter1), peekiter(iter2)
    while iter1.more() and iter2.more():
        value1, value2 = iter1.peek(), iter2.peek()
        cmp_res = cmp(value1, value2)
        if cmp_res < 0:
            yield value1
            iter1.next()
        elif cmp_res == 0:
            yield value1
            yield value2
            iter1.next()
            iter2.next()
        else: # cmp_res > 0
            yield value2
            iter2.next()
    for value1 in iter1:
        yield value1
    for value2 in iter2:
        yield value2

def difference(iter1, iter2):
    '''Iterate over elements in sorted iter1 and not sorted iter2.'''
    iter1, iter2 = peekiter(iter1), peekiter(iter2)
    while iter1.more() and iter2.more():
        value1, value2 = iter1.peek(), iter2.peek()
        cmp_res = cmp(value1, value2)
        if cmp_res < 0:
            yield value1
            iter1.next()
        elif cmp_res == 0:
            iter1.next()
            iter2.next()
        else: # cmp_res > 0
            iter2.next()
    for value1 in iter1:
        yield value1

def collate(*iters, **kwargs):
    '''
    Iterator over collated elements in sorted iterators in tuples.
    E.g. collate([1, 2], [1, 3], [1]) ->
       [(1, 1, 1), (2, None, None), (None, 3, None)]
    '''
    collate_cmp = kwargs.get('cmp', cmp)
    if len(iters) == 0:
        raise StopIteration
    elif len(iters) == 1:
        for value in iters[0]:
            yield (value,)
    elif len(iters) == 2:
        for value in collate2(iters[0], iters[1], cmp=collate_cmp):
            yield value
    else:
        def cmpn(x, y):
            # x is from iters[0], y is collation of iters[1:]
            for element in y:
                if element is not None:
                    val = collate_cmp(x, element)
                    return val
            raise ValueError
        for a, b in collate2(iters[0],
                             collate(*iters[1:], **dict(cmp=collate_cmp)),
                             cmp=cmpn):
            if b is None:
                b = (None,) * (len(iters) - 1)
            value = (a,) + b
            yield value

def collate2(iter1, iter2, cmp=cmp):
    '''
    Iterator over collated elements in two sorted iterators in pairs.
    E.g. collate([1, 2], [1, 3]) -> [(1, 1), (2, None), (None, 3)]
    '''
    iter1, iter2 = peekiter(iter1), peekiter(iter2)
    while iter1.more() and iter2.more():
        value1, value2 = iter1.peek(), iter2.peek()
        cmp_res = cmp(value1, value2)
        if cmp_res < 0:
            yield (value1, None)
            iter1.next()
        elif cmp_res == 0:
            yield (value1, value2)
            iter1.next()
            iter2.next()
        else: # cmp_res > 0
            yield (None, value2)
            iter2.next()
    for value1 in iter1:
        yield (value1, None)
    for value2 in iter2:
        yield (None, value2)

def izip_longest(*iters):
    '''izip iters, but fill in elements with None if one sequence is
    longer than the other. (Like izip_longest in itertoosl in Python
    2.6)'''
    if len(iters) == 0:
        raise StopIteration
    elif len(iters) == 1:
        for value in iters[0]:
            yield (value,)
    elif len(iters) == 2:
        for value in izip_longest2(iters[0], iters[1]):
            yield value
    else:
        for a, b in izip_longest2(iters[0], izip_longest(*iters[1:])):
            if b is None:
                b = (None,) * (len(iters) - 1)
            yield (a,) + b

def izip_longest2(iter1, iter2):
    '''izip iter1 and iter2, but fill in elements with None if one sequence
    is longer than the other.'''
    iter1, iter2 = peekiter(iter1), peekiter(iter2)
    while iter1.more() and iter2.more():
        yield iter1.next(), iter2.next()
    for value1 in iter1:
        yield (value1, None)
    for value2 in iter2:
        yield (None, value2)

def alternate(*iters):
    '''Alterate between elements in iters.  The itererators need not
    be the same length.'''
    for zipped in izip_longest(*iters):
        for elem in zipped:
            if elem is not None:
                yield elem

class Cursor(object):
    """Cursor class to make it easy/convenient to peek & consume the 
    contents of an iterator object."""
    def __init__(self, source):
        self.source = source
        self.head = None

    def peek(self):
        if self.head is None:
            try:
                self.head = self.source.next()
            except StopIteration:
                pass

        return self.head

    def get(self):
        rv = self.peek()
        self.head = None
        return rv


class peekiter(object):
    '''Peekable iterator.'''
    def __init__(self, collection):
        self.collection = iter(collection)
        self.has_element = False
        self.element = None
        self.raised_stop_iteration = False

    def __iter__(self):
        return self

    def next(self):
        if self.raised_stop_iteration:
            raise StopIteration
        if self.has_element:
            self.has_element = False
            return self.element
        else:
            return self.collection.next()

    def peek(self, default=None):
        if self.raised_stop_iteration:
            return None
        if not self.has_element:
            try:
                self.element = self.collection.next()
                self.has_element = True
            except StopIteration:
                self.raised_stop_iteration = True
                return default
        return self.element

    def more(self):
        if self.raised_stop_iteration:
            return False
        if not self.has_element:
            try:
                self.element = self.collection.next()
                self.has_element = True
            except StopIteration:
                self.raised_stop_iteration = True
                return False
        return True


class repeat_indexable(object):
    """ Just like the repeat function, except the resulting object is also
    indexable.  I don't know why the bult-in function doesn't do this. """
    def __init__(self, obj, count=None):
        self.obj = obj
        self.count = count
    
    def __len__(self):
        return self.count
    
    def __getitem__(self, key):
        i = int(key)
        if i < 0 or (self.count and i > self.count):
            raise IndexError
        return self.obj
    
    def __iter__(self):
        return repeat(self.obj, self.count)
