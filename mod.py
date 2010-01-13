"""Dealing with modules."""
from __future__ import absolute_import

import sys
import imp
import os.path
import inspect

# from util.str import peel

# from .str import peel

__all__ = ['import_', 'import_module', 'resolve_object']

def import_module(mod):
    modobj = __import__(mod)
    for piece in mod.split('.')[1:]:
        modobj = getattr(modobj, piece)

    return modobj

def resolve_object(objname):
    """Resolves anything, including modules, objects, whatever."""
    modname = objname
    while True:
        try:
            mod = __import__(modname, {}, {}, [''])

            break
        except ImportError:
            if modname.find('.') < 0:
                raise

            dot = modname.rindex('.')
            modname = modname[:dot]
    else:
        raise ImportError, 'Failed to import object', obj

    obj = mod

    for a in filter(None, objname[len(modname):].split('.')):
        obj = getattr(obj, a)

    return obj


def import_(src, tgt, *args):
    srcmod = import_module(src)
    tgtmod = sys.modules[tgt]

    names = set(args)

    for k, v in srcmod.__dict__.iteritems():
        if not (k.startswith('__') and k.endswith('__')) and (set(['*', k]) & names):
            tgtmod.__dict__[k] = v

def path_of_module(mod, path=None):
    """Given a module `mod', return the path it resolves to, without
    importing it."""
    from util.str import peel
    a, b       = peel(mod, '.')
    _, path, _ = imp.find_module(a, path)

    if b:
        return path_of_module(b, [path])
    else:
        return path
