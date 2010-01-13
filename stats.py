"""Basic statistics."""

from __future__ import division
from __future__ import absolute_import

import time
from collections import deque

from util.functional import pick, pickr0

def average(values):
    return sum(values) / len(values)

def median(values):
    assert len(values)
    return sorted(values)[len(values)//2]

# TODO(marius): this is really inefficient right now. Add proper
# bucketing support, so that we keep some sort of intervalled buckets
# and can add/remove them in batch.
class BucketStats(object):
    def __init__(self, *buckets):
        self.b = []
        self.total = 0

        for interval in buckets:
            self.b.append((interval, deque()))

    def __iadd__(self, val):
        now = time.time()

        for i, b in self.b:
            b.append((now, val))

        self.gc(now)
        self.total += val

        return self

    def gc(self, now):
        for i, b in self.b:
            while len(b) > 0 and (now - b[0][0]) > i:
                b.popleft()

    def __repr__(self):
        return '<BucketStats %s>' % (self)

    def counts(self):
        self.gc(time.time())
        return [(i, sum(pick(1, b)))
                for i, b in sorted(self.b, key=pickr0)]

    def __str__(self):
        now = time.time()
        self.gc(now)

        ret = ['%d: %.2f/sec' % (i, sum(pick(1, b)) / (b[-1][0] - b[0][0]))
               for i, b in sorted(self.b, key=pickr0)
               if len(b) > 1]

        return 'total: %.2f [%s]' % (self.total, '; '.join(ret))

class HistogramBucketStats(object):
    """Keep a bucketstats for each range in the histogram."""

    def __init__(self, cutoffs, buckets):
        """We assume that values are positive integers."""
        # Compute ranges from the cutoffs
        cutoffs = sorted(cutoffs)
        self.buckets = [
            ((min, max), BucketStats(*buckets))
            for min, max in zip(cutoffs[:-1], cutoffs[1:])]
        self.below = BucketStats(*buckets)
        self.above = BucketStats(*buckets)
        self.total = BucketStats(*buckets)

    def __iadd__(self, val):
        if val < self.buckets[0][0][0]:
            stats = self.below
        elif val >= self.buckets[-1][0][1]:
            stats = self.above
        else:
            for (min, max), s in self.buckets:
                if min <= val < max:
                    stats = s
                    break

        stats += 1
        self.total += val
        return self

    @property
    def all_buckets(self):
        return [self.below] + self.buckets + [self.above]

    def average(self):
        self.total.gc(time.time())
        return 'avg', [(i, average(pick(1, q)) if len(q) else None)
                       for i, q in self.total.b]

    def median(self):
        self.total.gc(time.time())
        return 'med', [(i, median(pick(1, q)) if len(q) else None)
                       for i, q in self.total.b]

    def variables_and_stats(self):
        vs = [('<%d' % self.buckets[0][0][0], self.below)]
        for (_, max), s in self.buckets:
            vs.append(('<%d' % max, s))
        vs.append(('<inf', self.above))
        return vs

    def __str__(self):
        return ', '.join(
            '%s: %s' % (v, s)
            for v, s in self.variables_and_stats() + [self.average()])

    def variables(self):
        return [(v, s.counts()) for v, s in self.variables_and_stats()] + \
               [self.average(), self.median()]

