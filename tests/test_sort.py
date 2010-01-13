import unittest
from util.sort import *

class TestSort(unittest.TestCase):
    def test_bigsorted(self):
        result = list(bigsorted(['b', 'a', 'c']))
        self.assertEqual(['a', 'b', 'c'], result)

        result = list(bigsorted([]))
        self.assertEqual([], result)

        result = list(bigsorted([0], serialize=str, deserialize=int))
        self.assertEqual([0], result)

        for input in [
            ['a', 'b', 'c'],
            ['a', 'c', 'b'],
            ['b', 'a', 'c'],
            ['b', 'c', 'a'],
            ['c', 'a', 'b'],
            ['c', 'b', 'a']]:
            for max_memory in [0, 1, 2, 100]:
                result = list(bigsorted(input, max_memory=max_memory))
                assert result == ['a', 'b', 'c']

def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)

if __name__ == '__main__':
    unittest.main()
