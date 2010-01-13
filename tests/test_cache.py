import time
import unittest
from itertools import count

import django.core.cache as dcache
from django.conf import settings

import util.cache as cache

def identity(*args, **kwargs):
    identity.runcount += 1
    return args, kwargs
identity.runcount = 0

@cache.cache_key('keyed', 60, lambda one, two: str(one))
def keyed(one, two):
    keyed.runcount += 1
    return one, two
keyed.runcount = 0

@cache.cache('decorated', 60)
def decorated(x):
    decorated.runcount += 1
    return x
decorated.runcount = 0

class TestCache(unittest.TestCase):
    def setUp(self):
        identity.runcount = 0
        keyed.runcount = 0
        # :-( Testing with django caching is a bit of a PITA.
        dcache.cache = dcache.get_cache('locmem:///')
        dcache.cache._cache.clear()

    def tearDown(self):
        dcache.cache = dcache.get_cache(settings.CACHE_BACKEND)

    def test_basics(self):
        self.assertEquals(0, identity.runcount)
        for _ in range(10):
            self.assertEquals(((), {}), cache.cache_('ns', 60, identity))
            self.assertEquals(1, identity.runcount)

        for _ in range(10):
            self.assertEquals((('hi',), {'ok': 1}),
                              cache.cache_('ns', 60, identity, 'hi', ok=1))
            self.assertEquals(2, identity.runcount)

    def test_cache_key(self):
        keyfun = lambda x,y:(x,y)
        res = cache.cache_key_('t', 1000, keyfun, keyed, 1, 2)
        self.assertEquals(1, keyed.runcount)
        res = cache.cache_key_('t', 1000, keyfun, keyed, 1, 2)
        self.assertEquals(1, keyed.runcount)
        res = cache.cache_key_('t', 1000, keyfun, keyed, 2, 1)
        self.assertEquals(2, keyed.runcount)

    def test_key(self):
        for x in range(100):
            self.assertEquals(x // 2, keyed(x // 2, x * 2)[0])
        self.assertEquals(50, keyed.runcount)

    def test_key_invalidate(self):
        for x in range(100):
            self.assertEquals(x // 2, keyed(x // 2, x * 2)[0])
            cache.invalidate_cache_key('keyed', str(x // 2))
        self.assertEquals(100, keyed.runcount)

    def test_decoration(self):
        for x in range(100):
            self.assertEquals(x, decorated(x))
        self.assertEquals(100, decorated.runcount)
        for x in range(100):
            self.assertEquals(x, decorated(x))
        self.assertEquals(100, decorated.runcount)

    def test_namespace(self):
        """Ensure different namespaces don't collide."""
        for i, ns in enumerate(['ns0', 'ns1', 'ns3', 'ns4']):
            for _ in range(5):
                self.assertEquals((('hi',), {}),
                                  cache.cache_(ns, 60, identity, 'hi'))
                self.assertEquals(i + 1, identity.runcount)

    def test_timeout(self):
        """Make sure timeouts work."""
        cache.cache_('ns', 2, identity, 'hi')
        self.assertEquals(1, identity.runcount)
        cache.cache_('ns', 2, identity, 'hi')
        self.assertEquals(1, identity.runcount)

        time.sleep(3)

        cache.cache_('ns', 2, identity, 'hi')
        self.assertEquals(2, identity.runcount)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
