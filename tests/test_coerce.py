import unittest
import util.coerce as coerce

class TestCoerce(unittest.TestCase):
    def test_basic(self):
        l = [1, 2, 3, 4, 5]
        d = {1: 1.2, 2: 2.2}

        self.assertEquals(coerce.coerce(l, int, float),
                          map(float, l))

        self.assertEquals(coerce.coerce(d, int, str).keys(),
                          map(str, d.keys()))

        self.assertEquals(coerce.coerce(d, (int, float), str),
                          {'1': '1.2', '2': '2.2'})


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
