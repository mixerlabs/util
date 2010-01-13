import unittest
import random
from itertools import repeat, cycle

from util.io import GeneratorFile

class TestGeneratorFile(unittest.TestCase):
    @staticmethod
    def _read_all(vf):
        buf = ''
        while True:
            this = vf.read()
            buf += this
            if this == '':
                return buf

    def test_basic(self):
        """Ensure basic correctness."""
        data = map(str, range(10000))
        random.shuffle(data)
        data = ''.join(data)

        self.assertEquals(data, self._read_all(GeneratorFile(iter(data))))

    def test_default_read_size(self):
        data = repeat('x')

        self.assertEquals(
            1024, len(GeneratorFile(data, default_read_size=1024).read())
        )

        self.assertEquals(
            1, len(GeneratorFile(data, default_read_size=1).read())
        )

        self.assertEquals(
            123, len(GeneratorFile(data, default_read_size=10).read(123))
        )

    def test_readline(self):
        data = ['1', '234', '5\n', 'ttttt\n']

        gf = GeneratorFile(iter(data))
        self.assertEquals('12345\n', gf.readline())
        self.assertEquals('ttttt\n', gf.readline())
        self.assertEquals('', gf.readline())

        gf = GeneratorFile(iter(data))
        self.assertEquals('12345', gf.readline(5))
        self.assertEquals('\n', gf.readline())
        self.assertEquals('t', gf.readline(1))
        self.assertEquals('t', gf.readline(1))
        self.assertEquals('ttt\n', gf.readline())
        self.assertEquals('', gf.readline())
        self.assertEquals('', gf.readline(123))


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
