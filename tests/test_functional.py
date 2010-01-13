import unittest
import types
from util import *
from util.functional import *
from functools import partial

class TestMapreduce(unittest.TestCase):
    unit_mr = partial(mapreduce, lambda x: [(x, x)], lambda _, vals: vals)

    def test_simple(self):
        ten = self.unit_mr(xrange(10))

        self.assert_(isinstance(ten, types.GeneratorType))
        ten = map(None, ten)
        self.assertEquals(10, len(ten))
        self.assertEquals(map(lambda x: (x, [x]), range(10)), ten)

    def test_multimap(self):
        def mapper(val):
            yield 'a', val
            yield 'b', 2*val

        def reducer(key, vals):
            return reduce(lambda a, b: a + b, vals, 0)

        res = mapreduce(mapper, reducer, xrange(100))
        res = map(None, res)

        self.assertEquals(2, len(res))

        res = dict(res)

        self.assertEquals(99*50, res['a'])
        self.assertEquals(2*99*50, res['b'])

    def test_generator_input(self):
        def data():
            for i in xrange(100):
                yield i

        res = self.unit_mr(data())

        self.assert_(isinstance(res, types.GeneratorType))
        res = map(None, res)
        self.assertEquals(100, len(res))
        self.assertEquals(map(lambda x: (x, [x]), range(100)), res)


class TestMemoize(unittest.TestCase):
    def test_simple(self):
        ns = storage(calls=0)
        @memoize
        def ret(*args, **kwargs):
            ns.calls += 1
            return args, kwargs

        args = (
            (),
            (1, 2, 3),
            (1, 2, 3, 4),
            (3, 2, 1),
            (1, 1, 1, 1, 1, 1, 1))
        kwargs = (
            dict(),
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3, d=4),
            dict(a=3, b=2, c=1))

        for a in args:
            for kw in kwargs:
                times(10, lambda: self.assertEquals((a, kw), ret(*a, **kw)))

        self.assertEquals(len(args) * len(kwargs), ns.calls)

        for a in args:
            for kw in kwargs:
                self.assert_((a, frozenset(kw.iteritems()))
                             in ret.undecorated._cache)


class TestSafe(unittest.TestCase):
    def fail(self):
        raise Exception, "epic fail"

    def test_safe_execute_returns_successful_value(self):
        self.assertEquals("foobar", safe(lambda: "foobar", "blah"))

    def test_safe_execute_returns_failsure_when_exception(self):
        self.assertEquals("blah", safe(lambda: self.fail(), "blah"))


class TestFunctional(unittest.TestCase):
    def test_nonrepeated_sorted(self):
        self.assertEquals([], nonrepeated_sorted([]))
        self.assertEquals([1], nonrepeated_sorted([1]))
        self.assertEquals([1], nonrepeated_sorted([1, 1]))
        self.assertEquals([1, 2, 3], nonrepeated_sorted([1, 2, 3]))
        self.assertEquals([1, 2, 3], nonrepeated_sorted([1, 1, 2, 2, 3, 3]))

    def test_compose(self):
        def f1(a, b, c):
            return [a, b, c]
        def f2(x):
            return x*2
        def f3(x):
            return x + ['f3']
        def f4(x):
            return x + ['f4']

        self.assertEquals([1, 2, 3]*2, compose(f2, f1)(1, 2, 3))
        self.assertEquals([1, 2, 3, 'f3', 'f4'], compose(f4, f3, f1)(1, 2, 3))
        self.assertEquals(
            [1, 2, 3, 'f3', 1, 2, 3, 'f3', 'f4'],
            compose(f4, f2, f3, f1)(1, 2, 3))

    def test_compose_splat(self):
        def f1(a, b, c):
            return ([a, b, c], [2])
        def f2(x, _):
            return x*2
        def f3(x, y):
            return (x + y, y)

        self.assertEquals([1, 2, 3, 1, 2, 3], compose_(f2, f1)(1, 2, 3))
        self.assertEquals(
            [1, 2, 3, 2, 1, 2, 3, 2],
            compose_(f2, f3, f1)(1, 2, 3))

def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
