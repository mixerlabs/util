"""Utilities for dealing with ``etc'' directories and files within."""
from __future__ import absolute_import

import os
import posixpath as pp
import yaml as YAML

from util import mixlog
from util.functional import pick, memoize
from environment import env

log = mixlog()

def yamls(which):
    """Returns (yaml-parsed, path)-tuples for the specified filename in
    etcdirs."""
    files_ = files(which)
    return zip(map(YAML.load, map(open, files_)), files_)

def yaml(app, *name):
    """like `open_', but parses out a yaml dictionary from the named
    file."""
    return YAML.load(open_(app, *name))

def files(which):
    """Returns a list of paths for the given name in etcdirs."""
    return filter(pp.exists, [pp.join(d, which) for d in dirs()])

def path(app, *name):
    """Returns the path for the given app, & name."""
    return pp.join(env.directory, app, 'etc', *name)

def open_(app, *name):
    """Open the specified etcfile for the given app."""
    return open(path(app, *name))

@memoize
def dirs():
    """Returns all application etcdirs."""
    dirs = filter(lambda d: pp.basename(d) == 'etc',
                  pick(0, os.walk(env.directory)))
    # Skip ones that are python packages!
    dirs = filter(lambda d: not pp.exists(pp.join(d, '__init__.py')), dirs)

    return dirs

def appname(p):
    """Given an etcdir or etcfile, returns the inferred the appname."""
    if pp.isfile(p):
        p = pp.dirname(p)

    # Strip away 'etc'
    assert pp.basename(p) == 'etc', p
    p = pp.dirname(p)

    prefix = pp.commonprefix([env.directory, p])
    p = p[len(prefix):]

    if p[0] == '/':
        p = p[1:]

    return p
