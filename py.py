"""Utilities for monkeying around with Python objects. Meta stuff."""

import inspect
import sys
import os
import signal
import traceback

from util.mod import resolve_object

def dir_(obj):
    return [getattr(obj, e) for e in dir(obj)]

class PyObject(object):
    """Encapsulate a resolvable python object."""
    def __init__(self, name):
        self.name = name
        self.obj()                      # Make sure we resolve.

    def obj(self):
        return resolve_object(self.name)

class MonkeyPatch(object):
    """monkey-patch the given object. Heh. Most likely you should have
    a _really_ good reason to use this."""
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        self.old_locals = dict(inspect.currentframe().f_back.f_locals)
        return self

    def __exit__(self, *_):
        l = inspect.currentframe().f_back.f_locals
        for k, v in l.items():
            if k not in self.old_locals or v is not self.old_locals[k]:
                setattr(self.obj, k, v)
                del l[k]

monkeypatch = MonkeyPatch

def with_attrs(**kwargs):
    """Returns a decorator that assigns the given attributes to the
    decorated object."""
    def decorator(fun):
        for k, v in kwargs.items():
            setattr(fun, k, v)

        return fun

    return decorator

class Proxy(object):
    """Just proxy an underlying object. You might want this when the
    underlying object is not writable and you need to override something."""

    def __init__(self, obj):
        self.__obj = obj

    def __getattr__(self, attr):
        try:
            return getattr(self.__obj, attr)
        except AttributeError:
            if attr in self.__obj.__dict__:
                return self.__obj.__dict__[attr]
            else:
                raise

def each_frame():
    """Returns ``(thread ident, frame)'' for each running thread."""
    return sys._current_frames().items()

def print_all_frames(to=sys.stderr):
    """Prints stackframes for all active threads (by default to
    stderr)."""
    for ident, frame in each_frame():
        print >>to, 'thread: %s' % ident
        print >>to, \
            ''.join('  ' + line for line in traceback.format_stack(frame))

def install_stackframe_inspector():
    """Install a SIGUSR1 handler that simply calls
    ``print_all_frames''."""
    signal.signal(signal.SIGUSR1, lambda *_: print_all_frames())
    def print_all_frames_and_exit(*_):
        print_all_frames()
        os._exit(1)

    signal.signal(signal.SIGINT, print_all_frames_and_exit)
    signal.signal(signal.SIGTERM, print_all_frames_and_exit)
