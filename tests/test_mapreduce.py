import unittest
import random
from util.mapreduce import mapreduce, serialize, deserialize

class TestMapreduce(unittest.TestCase):
    # generate the same stuff in random order, etc. etc. 

    def _basic_mapreduce(self, mem_limit_mb, count=10000, serialized=False):
        def mapper(val):
            yield val, '%d' % val
            yield val - (val % 1000), '%d' % val

        def reducer(key, values):
            res = map(int, values)
            return res

        if serialize:
            mapper = serialize(mapper)
            reducer = deserialize(reducer)

        inp = range(count)
        random.shuffle(inp)

        key_count = 0
        for key, value in mapreduce(mapper, reducer, inp,
                                    mem_limit_mb=mem_limit_mb):
            key = int(key)

            if key % 1000 == 0:
                self.assertEquals(1001, len(value))
            else:
                self.assertEquals(1, len(value))

            key_count += 1

        self.assertEquals(key_count, count)

    def test_basic(self):
        for serialized in [False, True]:
            # NOTE: we're doing test asserts in the _basic_mapreduce
            self._basic_mapreduce(1024, serialized=serialized)
            self._basic_mapreduce(512, serialized=serialized)
            # To break the barrier:
            #self._basic_mapreduce(1, count=200000)
            self._basic_mapreduce(0, serialized=serialized)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
