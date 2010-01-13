from util.attr_tree import *
import unittest

class TestAttrTree(unittest.TestCase):
    def test_attr_tree(self):
        tree = AttrTree()
        tree.set(('a', ), {'a_attr': 'a_val'})
        tree.set(('a', 'b', 'c'), {'c_attr': 'c_val'})
        tree.set(('a', 'b'), {'b_attr': 'b_val'})
        self.assertEquals(tree.get(('a', ))[1]['a_attr'], 'a_val')
        self.assertEquals(tree.get(('a', 'b'))[1]['b_attr'], 'b_val')
        self.assertEquals(tree.get(('a', 'b', 'c'))[1]['c_attr'], 'c_val')
        self.assertEquals(tree.serialize(), '{"a": [{"b": [{"c": [{}, {"c_attr": "c_val"}]}, {"b_attr": "b_val"}]}, {"a_attr": "a_val"}]}')
        tree = AttrTree(serialized='{"a": [{"b": [{"c": [{}, {"c_attr": "c_val"}]}, {"b_attr": "b_val"}]}, {"a_attr": "a_val"}]}')
        self.assertEquals(tree.serialize(), '{"a": [{"b": [{"c": [{}, {"c_attr": "c_val"}]}, {"b_attr": "b_val"}]}, {"a_attr": "a_val"}]}')


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == "__main__":
    unittest.main()
