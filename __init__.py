# keep this very very clean!

from __future__ import absolute_import

import os

import util._sym as sym
from util.functional import memoize
from util.storage import Storage, storage

from . import logging

__all__ = ['Storage', 'storage', 'sym', 'symbolize',
           'environment_variable', 'DeadType',
           'mixlog']

mixlog = logging.mixlog

def symbolize(key):
    return sym.__symbolize__(key)

def environment_variable(name, default_value=''):
    if name in os.environ:
        return os.environ[name]
    else:
        return default_value

class DeadType(object):
    '''A "dead" type -- will raise an exception if you try to do
    anything with it. Nice to use to "deactivate" objects'''

    def __getattribute__(self, key):
        raise sym.obj_is_dead.exc()

