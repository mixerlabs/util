from util.str import *
import unittest

class TestStr(unittest.TestCase):
    def test_compress_hex_to_alphanumeric(self):
        self.assertEquals('D8WUY0UZPzKEpbLgfffg',
            compress_hex_to_alphanumeric('1d3c6eaaec34e871e395de5c0f7da18205fd05fe'))
        self.assertEquals('', compress_hex_to_alphanumeric(''))
        self.assertEquals('e', compress_hex_to_alphanumeric('42'))
        # Verify that odd-length strings choke
        self.assertRaises(TypeError, compress_hex_to_alphanumeric, '4')


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == "__main__":
    unittest.main()
