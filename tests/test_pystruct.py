import unittest

from util.pystruct import Struct

class TestPyStruct(unittest.TestCase):
    def test_basic(self):
        class MyStruct(Struct):
            a0 = 0
            a1 = 1

        self.assertEquals(0, MyStruct().a0)
        self.assertEquals(1, MyStruct().a1)

        s = MyStruct(a0='a0')
        self.assertEquals('a0', s.a0)
        self.assertEquals(1, s.a1)

    def test_defaults(self):
        class MyStruct(Struct):
            a0 = Struct.__arg__(0)
            a1 = Struct.__arg__(1)

            a2 = 2
            a3 = 3
            
        self.assertRaises(ValueError, MyStruct, 0)
        self.assertRaises(ValueError, MyStruct, 0, a2=2)

        s = MyStruct(0, 1)
        self.assertEquals(0, s.a0)
        self.assertEquals(1, s.a1)

        self.assertEquals('a3', MyStruct(0, 1, a3='a3').a3)

        
    @staticmethod
    def _invalid_struct_def_0():
        class MyStruct(Struct):
            a0 = Struct.__arg__(0)
            a1 = Struct.__arg__(2)

        return MyStruct

    @staticmethod
    def _invalid_struct_def_1():
        class MyStruct(Struct):
            a0 = Struct.__arg__(1)
            a1 = Struct.__arg__(2)

        return MyStruct

    def test_definition(self):
        self.assertRaises(TypeError, TestPyStruct._invalid_struct_def_0)
        self.assertRaises(TypeError, TestPyStruct._invalid_struct_def_1)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
