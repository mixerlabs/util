"""structs with default values in python"""

from itertools import chain

from util.functional import mk_item_picker

class arg(object):
    def __init__(self, n):
        self.n = n

class _StructMeta(type):
    def __new__(mcl, classname, bases, classdict):
        def __init__(self, *args, **kwargs):
            if len(args) != len(self.__struct_args__):
                raise ValueError, 'Expected %d arguments' % len(self.__struct_args__)

            for idx, val in enumerate(args):
                setattr(self, self.__struct_args__[idx][0], val)

            for k, v in chain(self.__struct_defaults__, kwargs.iteritems()):
                setattr(self, k, v)

        def __repr__(self):
            v = lambda which: ','.join(['%s=%s' % (k, getattr(self, k)) for k, _ in which])
            return '<Struct %s (%s)>' % (
                v(self.__struct_args__), v(self.__struct_defaults__))

        newdict = dict(
            __slots__=[],
            __struct_args__=[],
            __struct_defaults__=[],
            __init__=__init__,
            __repr__=__repr__)

        for k, v in classdict.iteritems():
            if (k.startswith('__') and k.endswith('__')):
                newdict[k] = v
            else:
                newdict['__slots__'].append(k)

                if isinstance(v, arg):
                    newdict['__struct_args__'].append((k, v.n))
                else:
                    newdict['__struct_defaults__'].append((k, v))

        args = newdict['__struct_args__']
        args.sort(key=mk_item_picker(1))
        if map(mk_item_picker(1), args) != range(len(args)):
            raise TypeError, 'Incorrectly specified positional arguments.'

        return super(_StructMeta, mcl).__new__(mcl, classname, bases, newdict)

class Struct(object):
    """A `struct' like in C. Subclasses declare (with defaults)
    members, and they can be initialized through the __init__. For
    example:

    class Foo(Struct):
        one = 1
        two = {}
        three = []

    >>> f = Foo()
    >>> f
    <Struct three=[],two={},one=1>
    >>> f.three.append(1)
    >>> f
    <Struct three=[1],two={},one=1>
    >>> f = Foo(one=2, two=dict(hi='there'))
    >>> f
    <Struct three=[1],two={'hi': 'there'},one=2>

    We also support default arguments via the `__arg__' class:

    class Foo(Struct):
        d = Struct.__arg__(0)

        one = 1
        two = {}
        three = []

    >>> f = Foo(one=2, two=dict(hi='there'))
    ValueError: Expected 1 arguments
    >>> f = Foo('hello')
    <Struct d=hello (three=[],two={},one=1)>"""
    __arg__ = arg
    __metaclass__ = _StructMeta
