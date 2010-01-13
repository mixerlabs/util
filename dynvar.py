"""Implements thread-local dynamic variables in Python."""

import threading
from collections import deque

from util import *

class Stack(threading.local):
    def __init__(self):
        super(Stack, self).__init__()
        self._dq = deque()

    def append(self, item):
        self._dq.append(item)

    def pop(self):
        return self._dq.pop()

    def __iter__(self):
        return iter(self._dq)

    def __reversed__(self):
        return reversed(self._dq)

class Binding(object):
    _stack = Stack()

    def __init__(self, **kwargs):
        self._binding = kwargs

    def __enter__(self):
        self._stack.append(self._binding)

    def __exit__(self, type, value, x):
        self._stack.pop()

binding = Binding

class Variables(object):
    def get(self, key, orelse=None):
        try:
            return self[key]
        except KeyError:
            return orelse

    def __getattr__(self, key):
        # Search backwards
        for a in reversed(Binding._stack):
            if key in a:
                return a[key]

        raise AttributeError

    def __setattr__(self, key, value):
        raise AttributeError

    def __delattr__(self, key):
        raise AttributeError

    def __contains__(self, key):
        for a in reversed(Binding._stack):
            if key in a:
                return True
        else:
            return False

bindings = Variables()
