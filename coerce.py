from functional import dictmap


def coerce(obj, from_type, to_type, ignore_errors=False):
    if isinstance(obj, from_type):
        try:
            return to_type(obj)
        except ValueError:
            if ignore_errors:
                return obj
            else:
                raise
    elif isinstance(obj, dict):
        return dictmap(lambda k, v: (coerce(k, from_type, to_type),
                                     coerce(v, from_type, to_type)), obj)
    elif isinstance(obj, list):
        return [coerce(x, from_type, to_type) for x in obj]
    else:
        return obj
    
