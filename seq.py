"""Utilities for sequences."""

def first(seq, orelse=None):
    """Take the first item from sequence, otherwise return `orelse'"""
    try:
        return seq[0]
    except IndexError:
        return orelse

def empty(seq):
    """Return whether `seq' is empty, assuming that accessing seq[0]
    is cheaper than computing the sequence length. This is the case in
    Django querysets, for example."""
    return len(seq[:1]) == 0

def nonempty(seq):
    return not empty(seq)

def rindex(seq, val):
    idx = reduce(lambda prev, (i, v): i if v == val else prev,
                 enumerate(seq), -1)

    if idx < 0:
        raise ValueError
    else:
        return idx

def getindex(items, key_fn, default=-1):
    """Returns the first index in items for which key_fn is True. Otherwise,
    returns default, which defaults to -1."""
    count = 0
    for item in items:
        if key_fn(item):
            return count
        count += 1
    return default


def nonrepeated(seq):
    """Return a sequence without any repetitions. Earliest instance of a 
    repeated item is the one that remains."""
    def fil(a, b):
        if b not in a:
            return a + [b]
        else:
            return a
    return reduce(fil, seq, [])

def partition(seq, num_partitions=1):
    '''Partition sequence into num_partitions partitions.'''
    partition_length, num_longer_partitions = divmod(len(seq), num_partitions)
    partitions = []
    start = 0
    for i in range(num_longer_partitions):
        end = start + partition_length + 1
        partitions.append(seq[start:end])
        start = end
    for i in range(num_longer_partitions, num_partitions):
        end = start + partition_length
        partitions.append(seq[start:end])
        start = end
    return partitions

class Deferred(object):
    """A sequence with deferred evaluation of an underlying iterator."""
    def __init__(self, iterable):
        self._iter       = iterable
        self.__evaluated = None

    @property
    def _evaluated(self):
        if self.__evaluated is None:
            self.__evaluated = list(self._iter)
        return self.__evaluated

    def __repr__(self):
        return '<deferred of %r>' % (self._iter)

    def __len__(self):
        return len(self._evaluated)

    def __getitem__(self, i):
        return self._evaluated[i]

    def __iter__(self):
        return iter(self._evaluated)
