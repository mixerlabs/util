from util.seq import *
import unittest

class TestSeq(unittest.TestCase):
    def test_partition(self):
        for seq, num_partitions, expected in [
            ([],              1, [[]]),
            ([],              2, [[], []]),
            ([1],             1, [[1]]),
            ([1],             2, [[1], []]),
            ([1],             3, [[1], [], []]),
            ([1, 2],          1, [[1, 2]]),
            ([1, 2],          2, [[1], [2]]),
            ([1, 2],          3, [[1], [2], []]),
            ([1, 2, 3],       1, [[1, 2, 3]]),
            ([1, 2, 3],       2, [[1, 2], [3]]),
            ([1, 2, 3],       3, [[1], [2], [3]]),
            ([1, 2, 3, 4],    1, [[1, 2, 3, 4]]),
            ([1, 2, 3, 4],    2, [[1, 2], [3, 4]]),
            ([1, 2, 3, 4],    3, [[1, 2], [3], [4]]),
            ([1, 2, 3, 4, 5], 1, [[1, 2, 3, 4, 5]]),
            ([1, 2, 3, 4, 5], 2, [[1, 2, 3], [4, 5]]),
            ([1, 2, 3, 4, 5], 3, [[1, 2], [3, 4], [5]]),
            ]:
            result = partition(seq, num_partitions)
            self.assertEquals(expected, result)

    def test_deferred(self):
        i = iter([1, 2, 3, 4])
        d = Deferred(i)
        self.assertEquals(list(d), [1, 2, 3, 4])
        self.assertEquals(len(d), 4)
        self.assertEquals(d[2], 3)

def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)

if __name__ == "__main__":
    unittest.main()
