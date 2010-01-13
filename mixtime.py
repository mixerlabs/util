import re
import datetime
from dateutil.parser import parse

def local_utcoffset():
    """ Returns a timedelta rounded to the nearest minute of the ACTUAL
    difference between localtime and utc time.  For some crazy reason, there is
    no simple way to do this in python.  I've read dozens of articles.
    And time.daylight lies to me (it returns 1 in November) which also means
    time.timezone lies. """
    d = datetime.datetime.now() - datetime.datetime.utcnow()
    return datetime.timedelta(minutes=(d.seconds + 30) / 60 + d.days * 24 * 60)
   
LOCAL_UTC_OFFSET = local_utcoffset()
COLONS_TO_HYPHENS_IN_DATE_RE = re.compile(r"^(\d{4}):(\d\d):")

def localtime_from_string(s, assume_utc=False):
    """ Given a time string (such as Wed, 11 Nov 2009 18:47:58 -0800)
    convert that to a local time and strip off the time zone info.
    If time zone was ambiguous in the string, the local time zone is
    assumed unless assume_utc is True. """
    # Special case for 2009:11:25 06:30:12, swap date colons for hyphens
    s2 = COLONS_TO_HYPHENS_IN_DATE_RE.sub(r"\1-\2-", s)
    d = parse(s2)
    if d.tzinfo is not None:
        d = (d + LOCAL_UTC_OFFSET - d.utcoffset()).replace(tzinfo=None)
    elif assume_utc:
        d += LOCAL_UTC_OFFSET
    return d

def timestamp_from_datetime(dt):
    """ The opposite of datetime.fromtimestamp.  Basically, convert a datetime
    into the # of seconds since 1970, suitable for file timestamps. """
    return float(dt.strftime('%s'))