"""Robust symbols for Python. Go nuts.

    In [1]: from util import *
    In [2]: import util._sym
    In [3]: sym.foo == util._sym.foo
    Out[3]: True
    In [4]: sym.foo == sym.bar
    Out[4]: False
    In [5]: sym.foo is util._sym.foo
    Out[5]: True
    In [6]: sym.foo is sym.bar
    Out[6]: False
    In [7]: print sym.monkey
    monkey
    In [8]: print repr(sym.monkey)
    <Symbol monkey>
    In [9]: symbolize('monkey')
    Out[9]: <Symbol monkey>
    In [10]: symbolize('monkey') is sym.monkey
    Out[10]: True
"""

class Symbolize(object):
    __name__ = 'Symbolize'
    __doc__ = __doc__
    __file__ = __file__

    def __init__(self):
        self.symcache = {}

    class Symbol(object):
        def __init__(self, sym):
            symobj = self
            class exception(Exception):
                def __init__(self, data=''):
                    self.sym = symobj
                    self.data = data

                def __str__(self):
                    return self.__repr__()

                def __repr__(self):
                    desc = str(self.sym)
                    if self.data:
                        desc += '(%s)' % self.data

                    return '<Symbol exception: %s>' % desc

            self.sym = sym
            self.exc = exception

        def __repr__(self):
            return '<Symbol %s>' % self.sym

        def __str__(self):
            return self.sym

        def __cmp__(self, other):
            return cmp(id(self), id(other))

    # arg, 'intern' is taken
    def __symbolize__(self, key):
        return self.__getattr__(key)

    def __getattr__(self, key):
        if not key in self.symcache:
            self.symcache[key] = self.Symbol(key)

        return self.symcache[key]

import sys
import mod

# We always need to be the same place in the sys.modules, since that's
# where we store our instance.
if __name__ != 'util._sym':
    sys.modules[__name__] = mod.import_module('util._sym')
else:
    sys.modules[__name__] = Symbolize()
