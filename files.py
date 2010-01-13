"""Utilities to deal with files in the mixerlabs directory."""

import posixpath
from util import sym
from util.functional import switch, memoize
from environment import env
from util.iter import first
import tempfile
import mimetypes
import sys
import os
import hashlib
import base64
from mimetypes import guess_extension
import inspect
import shutil

__all__ = ['mixer_open', 'mixer_path']

def mixer_open(name, *args, **kwargs):
    return open(mixer_path(name, **kwargs), *args)

def mixer_system(name, *args, **kwargs):
    os.system(mixer_path(name, **kwargs), *args)
    
def mixer_system_py(name, *args, **kwargs):
    os.system('python %s ' % mixer_path(name, **kwargs), *args)

def mixer_path(*path, **kwargs):
    """Makes a path out of the *path arguments, as in
    posixpath.join. The path is made relative to the `relative' argument, which
    is either sym.src, or sym.root"""
    rel = kwargs.get('relative', sym.src)
    return posixpath.join(switch(rel, src=env.directory, root=env.root), *path)

def mixer_data_path(*path, **kwargs):
    return posixpath.join(env.data_repository, *path)

def mixer_static_data_path(*path, **kwargs):
    return posixpath.join(env.static_data_directory, *path)

def normalize_extension(filename):
    """ There are various alternate file extensions for common formats
    (like .jpe instead of .jpg)  This converts them to the most common one. """
    t = mimetypes.guess_type(filename)[0]
    if t is None:
        return

    exts = set(mimetypes.guess_all_extensions(t))

    # We have some preferences
    prefer = set(['.jpg', '.png', '.gif']) & exts
    if prefer:
        return first(prefer)
    else:
        return first(exts)

@memoize
def fingerprint_file(path, *args):
    hash = hashlib.sha1()
    file = open(path, 'rb')
    hash.update(file.read())
    file.close()
    # Only use the first 12 chars (just a cache hint, no need for security)
    return base64.urlsafe_b64encode(hash.digest())[:12]

def normalize_extension_from_mime_type(mime_type):
    """ Given a mime type like image/jpeg, return canonical extension like
    '.jpg', or empty string if extension is unknown."""
    # IE uses a nonstandard mime type when uploading jpg images (thans MS)
    ext = guess_extension(mime_type.replace('image/pjpeg', 'image/jpeg'))
    if not ext:
        return ''
    # Prepend an 'x' here because as of Python 2.6
    # mimetypes.guess_extension doesn't accept empty basenames any
    # longer.
    ext = normalize_extension('x' + ext)
    return ext or ''

def module_dir(name):
    """Returns the directory of the given module."""
    return posixpath.dirname(inspect.getsourcefile(sys.modules[name]))

def module_path(name, *components):
    """Returns the components joined to the path of the named module."""
    return posixpath.join(module_dir(name), *components)

def relative_path(path, *args):
    """Return a path relative to ``path''. Useful for getting paths
    relative to the current module (by invoking
    ``relative_path(__file__, ..)."""
    return os.path.normpath(os.path.join(os.path.dirname(path), *args))

class NamedTemporaryFile(object):
    """A NamedTemporaryFile that is a context manager."""
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __enter__(self):
        self._f = tempfile.NamedTemporaryFile(**self._kwargs)
        return self._f

    def __exit__(self, *_):
        self._f.close()

class TemporaryDirectory(object):
    """A temporary directory that gets wiped on exit."""
    def __enter__(self):
        self._d = tempfile.mkdtemp()
        return self._d

    def __exit__(self, *_):
        shutil.rmtree(self._d)

class TemporaryCWD(TemporaryDirectory):
    """Like ``TemporaryDirectory'', except also change chdir into it."""

    def __enter__(self):
        d = super(TemporaryCWD, self).__enter__()
        self._cwd = os.getcwd()
        os.chdir(d)
        return d

    def __exit__(self, *args):
        os.chdir(self._cwd)
        super(TemporaryCWD, self).__exit__(*args)

