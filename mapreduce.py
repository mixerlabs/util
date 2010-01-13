"""A slightly more complex mapreduce implementation (though it retains
the interface) than util.functional.mapreduce. This one will page to
disk if necessary, and can serve as an eventual interface to other MR
implementations (eg. Disco).

This one enforces string types on input and output. TODO(marius):
perhaps we can integrate JSON support so we get at least *some* python
type inference.

We also reads configuration parameters from the environment which are
overrideable by invocation parameters.

  mapreduce:
    mem_limit_mb - the number of megabytes of allowed memory usage"""

import sys
import os

import tempfile
import subprocess
import cPickle
import base64
from itertools import imap, groupby
from decorator import decorator

from environment import env
from util import *
from util.functional import mk_item_picker, switch

log = mixlog()

def chunkiter(f):
    while True:
        chunk = f.read(8192)
        if len(chunk) == 0:
            return
        else:
            yield chunk

def delim_reader(f, delim):
    content = ''
    for chunk in chunkiter(f):
        content += chunk

        while True:
            pos = content.find(delim)
            if pos < 0:
                break
            elif pos == 0:
                content = content[1:]
                continue

            yield content[:pos]
            content = content[pos:]

def make_mapper_reducer(mapper, reducer):
    """Processes `mapper' and `reducer', adding serialization to map
    output and reduce input. This is not required for in-memory
    mapreduces that allow passing non-string python values between the
    mapper & reducer."""
    if getattr(mapper, 'needs_serialization', False):
        assert reducer.needs_serialization
        mapper = lambda x, M=mapper: ((k, serialize_obj(v)) for k, v in M(x))
        reducer = lambda k, vs, R=reducer: R(k, map(deserialize_str, vs))
    return mapper, reducer

def mapreduce(mapper, reducer, data, mem_limit_mb=sym.env):
    mem_limit_mb = switch(mem_limit_mb,
                          env=env.mapreduce.mem_limit_mb,
                          _=mem_limit_mb)

    limit_bytes = mem_limit_mb << 20
    bytes = 0

    # TODO: for purely in-memory mapreduces, we could optimize away
    # the serialization/deserialization.
    mapper, reducer = make_mapper_reducer(mapper, reducer)

    mapped = []
    map_map = lambda key, val: mapped.append((key, val))
    map_sort = lambda: mapped.sort(key=lambda x: x[0])
    map_output = lambda: mapped
    def map_cleanup():
        del mapped[:]

    for item in data:
        for key, val in mapper(item):
            if bytes >= limit_bytes:
                tmpfile = tempfile.NamedTemporaryFile()
                outfile = tempfile.NamedTemporaryFile()
                def map_map(key, value):
                    key = str(key)
                    # This is a bit of an artifical limitation - it
                    # would be easy to devise an escaping scheme, but
                    # whoever runs into this first gets to fix it!!
                    assert '|' not in key, (
                        '| are not allowed in keys for disk sorting'
                    )
                    assert '\0' not in value, (
                        'null-bytes are not allowed in values '
                        'for disk sorting'
                    )
                    tmpfile.write("%s|%s\0" % (key, value))

                log.info('Switching to disk sorting... mapping %d bytes',
                         bytes)
                map(lambda kv: map_map(kv[0], kv[1]), map_output())
                map_cleanup()

                def map_sort():
                    tmpfile.flush()
                    p = subprocess.Popen(['sort',
                                          '-k', '1,1',   # key at pos 1
                                          '-z',          # nul-termination
                                          '-t', '|',     # |-separated
                                          '-o', outfile.name,
                                          tmpfile.name])

                    if p.wait():
                        raise sym.external_sort_failed

                def map_output():
                    f = open(outfile.name)
                    log.info('Reducing from %s', outfile.name)
                    return imap(lambda kv: kv.split('|', 1),
                                delim_reader(f, '\0'))

                def map_cleanup():
                    # Files get removed once their NamedTemporaryFile
                    # objects get GCd
                    pass

                # hack to disable doing this conversion more than
                # once.
                limit_bytes = sys.maxint

            map_map(key, val)
            bytes += len(val)

    map_sort()

    output = ((key, imap(mk_item_picker(1), vals))
              for key, vals in groupby(map_output(), lambda kv: kv[0]))

    return ((key, reducer(key, vals))
            for key, vals in output)

def serialize_obj(obj):
    return base64.b64encode(cPickle.dumps(obj))

def deserialize_str(str):
    return cPickle.loads(base64.b64decode(str))

def serialize(mapper):
    """Serialize the map output. This retains Python objects across
    maps and into reduces."""
    mapper.needs_serialization = True
    return mapper

def deserialize(reducer):
    """Deserialize a map serialization."""
    reducer.needs_serialization = True
    return reducer
