try:
    import cPickle as pickle
except ImportError:
    import pickle

from base64 import b64encode, b64decode
import types
import zlib
import cjson

from django.db import models

class JSONField(models.TextField):
    __metaclass__ = models.SubfieldBase

    def to_python(self, value):
        if value == '':
            return None
        elif getattr(value, '_is_jsoned_type', False):
            return value
        elif isinstance(value, (str, unicode)):
            try:
                decoded = cjson.decode(value)
            except cjson.DecodeError:
                return value            # A bit of a hack.

            if decoded is None:
                return decoded

            # Necessary to identify JSON-decoded objects. We have to
            # create a subtype in case the supertype has slots
            # defined..
            class Jsoned(type(decoded)):
                _is_jsoned_type = True

            try:
                decoded.__class__ = Jsoned
            except TypeError:
                decoded = Jsoned(decoded)

            return decoded
        else:
            return value

    def get_db_prep_value(self, value):
        return cjson.encode(value)

class PickleField(models.TextField):
    """Store pickled versions of Python objects, unpickling them on
    request."""
    __metaclass__ = models.SubfieldBase

    def to_python(self, value):
        if not isinstance(value, types.StringTypes):
            return value

        if len(value) == 0:
            return None

        return pickle.loads(zlib.decompress(b64decode(value)))

    def get_db_prep_value(self, value):
        if isinstance(value, types.StringTypes):
            # We could add some magic to encoded strings, but in
            # reality we're not interested in pickling strings anyway.
            raise NotImplementedError, (
                'String values are not supported as they are '
                'indistinguishable from base-64 encoded pickles')

        # Compressing with zlib gives about 25% savings on widget pickles, which
        # are typically between 200 and 800 bytes. bz2 didn't do well on these.
        return b64encode(zlib.compress(pickle.dumps(value, 2)))
