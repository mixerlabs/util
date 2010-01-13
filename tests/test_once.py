import unittest
import util.once as once
import util.functional as functional
from util import *

class TestOnce(unittest.TestCase):
    def test_once(self):
        # Ugh, this sucks.
        ns = storage()
        ns.runs = 0

        @once.fun
        def myfun():
            ns.runs += 1
            return 'result'

        self.assertEquals(functional.times(10, myfun), ['result']*10)
        self.assertEquals(ns.runs, 1)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
