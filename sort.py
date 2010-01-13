#!/usr/bin/env python

from __future__ import with_statement

__all__ = ['bigsorted']

import os
import sys
from util.iter import merge
import struct
import tempfile

MAX_MEMORY = 100 * 1024 * 1024

def bigsorted(iterable, key=None, cmp=None, serialize=str, deserialize=str,
              max_memory=MAX_MEMORY, tmpdir=None):
    '''
    Sort iterable that is too large to fit in memory.
      key: Key lambda
      cmp: Compare lambda
      serialize: Element serializer lambda
      deserialize: Element deserializer lambda
      max_memory: Maximum number of serialized bytes to store in memory.
         The default is 100 MB. Note actual memory usage will be higher
         due to Python overhead.
    '''
    # Wrap around _bigsort to track tempfiles.  This ensure we don't
    # leave giant tempfiles on disk if something goes wrong.
    tmpfiles = []
    try:
        for element in _bigsorted(tmpfiles, iterable, key, cmp,
                                  serialize, deserialize, max_memory, tmpdir):
            yield element
    finally:
        for tmpfile in tmpfiles:
            try:
                os.unlink(tmpfile)
            except OSError:
                pass

def _bigsorted(tmpfiles, iterable, key=None, cmp=None,
               serialize=str, deserialize=str,
               max_memory=MAX_MEMORY, tmpdir=None):
    if key is None:
        key = lambda x: x
    if cmp is None:
        cmp = __builtins__['cmp']

    # Iterate over all elements.  Store in key_to_bytes.  If
    # key_to_bytes is too big, sort and write to disk.
    num_bytes    = 0
    key_to_bytes = {} # key -> list of bytes with key
    for element in iterable:
        k = key(element)
        bytes = serialize(element)
        key_to_bytes.setdefault(k, []).append(bytes)
        num_bytes += len(bytes)
        if num_bytes > max_memory:
            _write_key_to_bytes(tmpfiles, key_to_bytes, cmp, tmpdir)
            key_to_bytes.clear()
            num_bytes = 0

    if not tmpfiles:
        # Nothing was written to disk, yield what's in memory.
        for _, elem in _each_dict_key_element(key_to_bytes, cmp, deserialize):
            yield elem
        raise StopIteration

    # Merge sorted files until there's only one left
    while len(tmpfiles) > 1:
        filename1 = tmpfiles[0]
        with open(filename1) as file1:
            filename2 = tmpfiles[1]
            with open(filename2) as file2:
                fd, filename = tempfile.mkstemp('.bigsort', dir=tmpdir)
                tmpfiles.append(filename)
                with os.fdopen(fd, 'w') as file:
                    iter1 = _each_file_key_element(file1, key, deserialize)
                    iter2 = _each_file_key_element(file2, key, deserialize)
                    for _, elem in merge(iter1, iter2,
                                         lambda x,y: cmp(x[0], y[0])):
                        _write_element(file, elem, serialize)
            os.remove(filename2)
            assert tmpfiles[1] == filename2
            del tmpfiles[1]
        os.remove(filename1)
        tmpfiles.pop(0)

    # Merge last sorted file with key_to_bytes
    assert len(tmpfiles) == 1
    filename = tmpfiles[0]
    with open(filename) as file:
        iter1 = _each_file_key_element(file, key, deserialize)
        iter2 = _each_dict_key_element(key_to_bytes, cmp, deserialize)
        for _, element in merge(iter1, iter2, lambda x,y: cmp(x[0], y[0])):
            yield element
    os.remove(filename)
    tmpfiles.pop(0)

def _write_key_to_bytes(tmpfiles, key_to_bytes, cmp, tmpdir):
    fd, filename = tempfile.mkstemp('.bigsort', dir=tmpdir)
    tmpfiles.append(filename)
    with os.fdopen(fd, 'w') as file:
        for key in sorted(key_to_bytes.keys(), cmp=cmp):
            for bytes in key_to_bytes[key]:
                _write_bytes(file, bytes)

def _write_element(file, element, serialize):
    bytes = serialize(element)
    _write_bytes(file, bytes)

def _write_bytes(file, bytes):
    length_bytes = struct.pack('I', len(bytes))
    file.write(length_bytes)
    file.write(bytes)

def _each_dict_key_element(key_to_bytes, cmp, deserialize):
    for key in sorted(key_to_bytes.keys(), cmp=cmp):
        for bytes in key_to_bytes[key]:
            element = deserialize(bytes)
            yield key, element

def _each_file_key_element(file, key, deserialize):
    for element in _each_file_element(file, deserialize):
        k = key(element)
        yield k, element

def _each_file_element(file, deserialize):
    while True:
        try:
            element = _read_element(file, deserialize)
            yield element
        except EOFError:
            break

def _read_element(file, deserialize):
    bytes = file.read(4)
    if not bytes:
        raise EOFError
    (length,) = struct.unpack('I', bytes)
    bytes = file.read(length)
    if not bytes or len(bytes) != length:
        raise EOFError
    element = deserialize(bytes)
    return element
