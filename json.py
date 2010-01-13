import simplejson

class StringifyingJSONEncoder(simplejson.JSONEncoder):
    """A JSON encoder that attempts to ``stringify'' an object as a
    last resort for JSON encoding."""

    def default(self, obj):
        return str(obj)
