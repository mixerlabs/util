"""
This is a general purpose system for ratelimiting any type of event using
memcached and key (stringable identitifer).  The granularity is number of
events per consecutive group of minutes. It creates a memcached counter key
for each key+minute to provide a sliding window effect.

For example, the default is 20 events over the past 3 minutes.  Any time
the sum of events from key for the curent minute, minute-1, and minute-2 is
over 20, 'should_limit' returns False.

'clear_limit' can be used to zero out the limits for a given key if the user
answered a captcha.
"""

import datetime

from django.core.cache import cache

def _recent_keys(key, minutes, now):
    now = now or datetime.datetime.now()
    return ['util.ratelimit:%s:%s' % (str(key),
            (now - datetime.timedelta(minutes=m)).strftime('%Y%m%d%H%M')
            ) for m in range(minutes)]

def _incr_key(key, expire_after, howmuch=1):
    try:
        # Create the key if it doesn't exist (add fails silently if it exists)
        cache._cache.add(key, 0, time=expire_after)
        # incr is atomic (but would fail if key didn't exist)
        cache._cache.incr(key, delta=howmuch)
    except AttributeError:
        # Fallback to this method in case no memcached
        cache.set(key, cache.get(key, 0) + howmuch, expire_after)    
    
def should_limit(key, minutes=3, rate=20, now=None, howmuch=1):
    """ Increments the event count for the current key+minute, and returns True
    if 'key' has had more than 'rate' events in the past 'minutes' minutes. """
    keys = _recent_keys(key, minutes, now)
    _incr_key(keys[0], minutes * 60, howmuch=howmuch)
    return sum(cache.get_many(keys).values()) > rate
    
def clear_limit(key, minutes=3, now=None):
    """ Zeros all counters for 'key' over the past 'minutes' minutes. """
    for key in _recent_keys(key, minutes, now):
        cache.delete(key)
