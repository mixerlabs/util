import datetime
import time

def datetime_from_obj(value):
    """ If value is a datetime, or is a string that can be converted
    to a datetime, return the datetime.  Otherwise, return None. This
    function is stricter than django in that it expects the datetime
    format to be the same as what gets returned from datetime.now() """
    if isinstance(value, datetime.datetime):
        return value
    try:
        value, usecs = value.strip().split('.')
        if len(usecs) == 6:
            usecs = int(usecs)
            return datetime.datetime(
                  *time.strptime(value, '%Y-%m-%d %H:%M:%S')[:6],
                  **{'microsecond': usecs})
    except (ValueError, AttributeError):
        pass
    return None
