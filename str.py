"""string/parsing utilities"""

from binascii import unhexlify
from string import ascii_letters, digits
import random

def peel(s, by):
    """like split(by, 1), except always return a 2-tuple, with the
    remainder of the string empty if it cannot be split any more."""
    ret = s.split(by, 1)
    if len(ret) == 1:
        return ret[0], ''
    else:
        return ret[0], ret[1]

def rpeel(s, by):
    """like rsplit(by, 1), except always return a 2-tuple, with the
    remainder of the string empty if it cannot be split any more."""
    ret = s.rsplit(by, 1)
    if len(ret) == 1:
        return ret[0], ''
    else:
        return ret[0], ret[1]

def compress_hex(hex, alphabet=('abcdefghijklmnopqrstuvwxyz'
                                'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')):
    return ''.join([alphabet[ord(b) % len(alphabet)] for b in unhexlify(hex)])

def compress_hex_to_alphanumeric(hex):
    """Compress 2n hex chars down to n alphanumeric chars.  This is lossy.
    A 160 bit sha-1 hash will keep 120 bits of precision, which is plenty. 
    It is intended for shortening long hash digest strings for pathnames."""
    # Don't be tempted to add other characters in here. 
    return compress_hex(hex)

# For some reason this seeding is not happening automatically
random.seed()

def random_key(length):
    """ Returns a string containing 'length' random letters and digits. """
    return ''.join([random.choice(ascii_letters + digits) for i in xrange(
            length)])

