import unittest
from util.iter import *

class TestIter(unittest.TestCase):
    def test_merge(self):
        for a, b in [
            ([],        []),
            ([],        [0]),
            ([0],       []),
            ([0],       [1]),
            ([0],       [0]),
            ([0, 0],    [0, 0]),
            ([1, 2, 3], [1, 2, 3]),
            ([1, 2, 3], [1, 2, 3]),
            ([1, 2, 3], [4, 5, 6]),
            ([4, 5, 6], [1, 2, 3]),
            ([1, 5, 6], [0, 3, 4])]:
            expected = sorted((a + b))
            result = list(merge(a, b))
            self.assertEqual(expected, result)

    def test_difference(self):
        for a, b in [
            ([],        []),
            ([],        [0]),
            ([0],       []),
            ([0],       [1]),
            ([0],       [0]),
            ([0, 0],    [0, 0]),
            ([1, 2, 3], [1, 2, 3]),
            ([1, 2, 3], [1, 2]),
            ([1, 2, 3], [1]),
            ([1, 2, 3], [1, 2, 3]),
            ([1, 2],    [1, 2, 3]),
            ([1],       [1, 2, 3])]:
            expected = list(sorted(set(a) - set(b)))
            result = list(difference(a, b))
            self.assertEqual(expected, result)

    def test_collate2(self):
        for a, b, expected in [
            ([],        [],        []),
            ([],        [0],       [(None, 0)]),
            ([0],       [],        [(0, None)]),
            ([0],       [0],       [(0, 0)]),
            ([0],       [1],       [(0, None), (None, 1)]),
            ([1],       [0],       [(None, 0), (1, None)]),
            ([0, 0],    [0, 0],    [(0, 0), (0, 0)]),
            ([1, 2, 3], [1, 2, 3], [(1, 1), (2, 2), (3, 3)]),
            ([1, 2, 3], [4, 5, 6], [(1, None), (2, None), (3, None),
                                    (None, 4), (None, 5), (None, 6)]),
            ([4, 5, 6], [1, 2, 3], [(None, 1), (None, 2), (None, 3),
                                    (4, None), (5, None), (6, None)]),
            ([1, 5, 6], [0, 3, 4], [(None, 0), (1, None), (None, 3),
                                    (None, 4), (5, None), (6, None)])
            ]:
            result = list(collate2(a, b))
            self.assertEqual(expected, result)

    def test_collate(self):
        for iters, expected in [
            # No iters
            ([],           []),
            # One iter
            ([[]],         []),
            ([[1, 2, 3]],
             [(1,), (2,), (3,)]),
            ([[], []], []),
            # Two iters
            ([[1, 2, 3], []],
             [(1, None), (2, None), (3, None)]),
            ([[1, 2], [1, 3]],
             [(1, 1), (2, None), (None, 3)]),
            # Three iters
            ([[1, 2], [1, 2], [1, 3]],
             [(1, 1, 1), (2, 2, None), (None, None, 3)]),
            ([[1, 2], [1, 3], [1, 2]],
             [(1, 1, 1), (2, None, 2), (None, 3, None)]),
            ([[1, 3], [1, 2], [1, 2]],
             [(1, 1, 1), (None, 2, 2), (3, None, None)]),
            ([[0, 2], [1, 2], [1, 2]],
             [(0, None, None), (None, 1, 1), (2, 2, 2)]),
            ([[1, 2], [0, 2], [1, 2]],
             [(None, 0, None), (1, None, 1), (2, 2, 2)]),
            ([[1, 2], [1, 2], [0, 2]],
             [(None, None, 0), (1, 1, None), (2, 2, 2)]),
            ([[1, 2], [1, 2], []],
             [(1, 1, None), (2, 2, None)]),
            ([[1, 2], [], [1, 2]],
             [(1, None, 1), (2, None, 2)]),
            ([[], [1, 2], [1, 2]],
             [(None, 1, 1), (None, 2, 2)]),
            # Four iters
            ([[1, 2], [1, 2], [1, 2], [1, 2]],
             [(1, 1, 1, 1), (2, 2, 2, 2)]),
            ([[1, 2], [1, 2], [1, 2], [1, 3]],
             [(1, 1, 1, 1), (2, 2, 2, None), (None, None, None, 3)]),
            ]:
            result = list(collate(*iters))
            self.assertEqual(expected, result)

        # Test with different cmp
        a = [(1, 'apple'), (2, 'bob')]
        b = [(2, 'banana'),   (3, 'carl')]
        c = [(3, 'carl'),  (4, 'donut')]
        expected = [
            ((1, 'apple'), None, None),
            ((2, 'bob'), (2, 'banana'), None),
            (None, (3, 'carl'), (3, 'carl')),
            (None, None, (4, 'donut'))]
        result = list(collate(a, b, c, cmp=lambda x,y: cmp(x[0], y[0])))
        self.assertEqual(expected, result)

    def test_izip_longest2(self):
        for a, b, expected in [
            ([],        [],        []),
            ([],        [0],       [(None, 0)]),
            ([0],       [],        [(0, None)]),
            ([0],       [0],       [(0, 0)]),
            ([0],       [1],       [(0, 1)]),
            ([1, 2, 3], [1, 2, 3], [(1, 1), (2, 2), (3, 3)]),
            ([1, 2, 3], [4, 5],    [(1, 4), (2, 5), (3, None)]),
            ([1, 2],    [4, 5, 6], [(1, 4), (2, 5), (None, 6)]),
            ]:
            result = list(izip_longest2(a, b))
            self.assertEqual(expected, result)

    def test_izip_longest(self):
        for iters, expected in [
            # No iters
            ([],           []),
            # One iter
            ([[]],         []),
            ([[1, 2, 3]],
             [(1,), (2,), (3,)]),
            ([[], []], []),
            # Two iters
            ([[1, 2, 3], []],
             [(1, None), (2, None), (3, None)]),
            ([[1, 2], [1, 3]],
             [(1, 1), (2, 3)]),
            # Three iters
            ([[1, 2], [1, 2], [1, 3]],
             [(1, 1, 1), (2, 2, 3)]),
            ([[1, 2], [1, 2], [1]],
             [(1, 1, 1), (2, 2, None)]),
            ([[1, 2], [1, None], [1, 3]],
             [(1, 1, 1), (2, None, 3)]),
            ([[1, None], [1, 2], [1, 3]],
             [(1, 1, 1), (None, 2, 3)]),
            ]:
            result = list(izip_longest(*iters))
            self.assertEqual(expected, result)

    def test_alternate(self):
        for iters, expected in [
            ([],                       []),
            ([[0]],                    [0]),
            ([[0], [1]],               [0, 1]),
            ([[0], [1], [2]],          [0, 1, 2]),
            ([[0, 3], [1], [2]],       [0, 1, 2, 3]),
            ([[0, 3], [1, 4], [2]],    [0, 1, 2, 3, 4]),
            ([[0, 3], [1], [2, 4]],    [0, 1, 2, 3, 4]),
            ([[0, 3], [1, 4], [2, 5]], [0, 1, 2, 3, 4, 5]),
            ]:
            result = list(alternate(*iters))
            self.assertEqual(expected, result)


    def test_peekiter(self):
        # empty, normal iteration
        i = peekiter([])
        self.assertEqual([], list(i))

        # empty, with peek
        i = peekiter([])
        self.assertEqual(None, i.peek())
        self.assertEqual([], list(i))

        # empty, with more
        i = peekiter([])
        self.assert_(not i.more())
        self.assertEqual(None, i.peek())
        self.assertEqual([], list(i))

        # elements, normal iteration
        a = [1, 2, 3]
        self.assertEqual([1, 2, 3], list(a))

        # elements, with peek
        a = [1, 2, 3]
        i = peekiter(a)
        self.assertEqual(1, i.peek())
        self.assertEqual(1, i.next())
        self.assertEqual(2, i.peek())
        self.assertEqual(2, i.next())
        self.assertEqual(3, i.peek())
        self.assertEqual(3, i.next())
        self.assertEqual(None, i.peek())
        self.assertRaises(StopIteration, i.next)

        # elements, with more
        a = [1, 2, 3]
        i = peekiter(a)
        self.assert_(i.more())
        self.assertEqual(1, i.peek())
        self.assertEqual(1, i.next())
        self.assert_(i.more())
        self.assertEqual(2, i.peek())
        self.assertEqual(2, i.next())
        self.assert_(i.more())
        self.assertEqual(3, i.peek())
        self.assertEqual(3, i.next())
        self.assert_(not i.more())
        self.assertEqual(None, i.peek())
        self.assertRaises(StopIteration, i.next)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)

if __name__ == '__main__':
    unittest.main()
