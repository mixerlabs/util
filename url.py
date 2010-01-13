"""Provides ``open_safe'' -- a wrapper around urllib2.urlopen that
ensures the fetched URL is safe. See ``open_safe'' for details.

And merge_cgi_params to add and modify cgi parameters on urls

"""
from __future__ import absolute_import
from __future__ import with_statement

import cgi
import dns.resolver
import re
import socket
import struct
import urllib
import urllib2
import urlparse

from util.str import rpeel
from util.functional import dictmap

def aton(ip):
    return struct.unpack('>L', socket.inet_aton(ip))[0]

private_ip_networks = [
    # http://en.wikipedia.org/wiki/Private_network
    ('10.0.0.0'   , 8 ),
    ('172.16.0.0' , 12),
    ('192.168.0.0', 16),

    # http://en.wikipedia.org/wiki/Loopback
    ('127.0.0.0'  , 8 ),
]

private_ip_networks = [(aton(ip), mask) for ip, mask in private_ip_networks]
ip_re = re.compile(r'(\d+)\.(\d+)\.(\d+)\.(\d+)')

class NewDefaultSocketTimeout(object):
    """A terrible hack, with many caveats. But so is timeout handling
    in urllib2."""
    def __init__(self, timeout):
        self._new_timeout = timeout

    def __enter__(self):
        self._old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self._new_timeout)

    def __exit__(self, *_):
        socket.setdefaulttimeout(self._old_timeout)

class ProtocolError(ValueError)        : pass
class NetworkLocationError(ValueError) : pass
class ResolverError(ValueError)        : pass
class AccessError(ValueError)          : pass
class HttpError(ValueError)            : pass

def open_safe(url):
    """Provides safe URL opening for content. Specifically:

      - ensures only valid http URLs are fetched
      - ensures fetches aren't being done from private IP space
      - ensures only HTTP 200 responses are accepted
      - sets a socket timeout of 20 seconds

    It does not deal with, as it does not seem possible with urllib2:

      - limiting of the size of the response
      - HTTP streaming timeouts (ie. timeouts after connection establishment).

    Requests are fetched with user agent ``lil(bot)''."""
    with NewDefaultSocketTimeout(20):
        # First, make sure that the URL is safe.
        scheme, netloc, path, params, query, fragment = urlparse.urlparse(url)
        if scheme != 'http':
            raise ProtocolError, 'Only http allowed'
        if not netloc:
            raise NetworkLocationError, 'Network location required'

        host, port = rpeel(netloc, ':')

        if ip_re.match(host):
            resolveda = host
        else:
            # Now, resolve the host name into an IP address, and check
            # that it is sane. Note that `dns.resolver.query' resolves
            # CNAME chains for us.
            try:
                resolved = dns.resolver.query(host)
            except dns.resolver.NXDOMAIN:
                raise ResolverError, 'Network location does not exist!'

            # Pick the first valid address arbitrarily.
            resolveda = resolved[0].address

        resolvedn = aton(resolveda)

        # Make sure the resolved address isn't in one of the defined
        # private networks.
        for ip, mask in private_ip_networks:
            mask = 0xffffffffL << (32 - mask)
            if (resolvedn & mask) == ip:
                raise AccessError, 'Private network access disallowed!'

        if port:
            resolveda += ':' + port

        # Ok, we're fine, now make sure we actually use our *resolved*
        # IP address, instead of letting urllib2 resolve it for us (in
        # case it does something differently, or the address is
        # changed in between our resolution & subsequent fetching).
        url = urlparse.urlunparse(
            (scheme, resolveda, path, params, query, fragment)
        )
        headers = {
            'User-Agent' : ('Mozilla/5.0 (compatible; lilb(ot)/1.0; '
                            '+http://www.townme.com/)'),
            'Host'       : netloc,
            'Accept'     : 'text/xml,text/html;q=0.9,text/plain;q=0.8',
        }

        # Finally we're ready to make the request.
        request  = urllib2.Request(url, None, headers)
        response = urllib2.urlopen(request)
        if response.code != 200:
            raise HttpError, 'Invalid HTTP response code %d' % response.code

        return response


def merge_cgi_params(url, new_params=None):
    """ Returns a url with repeated cgi parameters replaced with the last
    occurring one or overridden by a value from a new_params dict. """
    parts = urlparse.urlsplit(url)
    params = cgi.parse_qs(parts[3], keep_blank_values=True)
    params = dictmap(lambda k, v: (k, v[-1]), params)
    if new_params:
        params.update(new_params)
    return urlparse.urlunsplit((parts[0], parts[1], parts[2],
            urllib.urlencode(params), parts[4]))
    

def strip_cgi_param(url, param):
    """ Returns a (stripped_url, value) with 'param' removed from the url and
    its value returned as value (or None if it wasn't present.)  If it was
    present but not assigned, value is ''.   If multiple instances of param are
    present, all are stripped but the value of the last one is returned. """
    parts = urlparse.urlsplit(url)
    params = cgi.parse_qsl(parts[3], keep_blank_values=True)
    value = [ None ]
    def is_match(item):
        if item[0] == param:
            value[0] = item[1]
            return False
        return True
    params = filter(is_match, params)
    return (urlparse.urlunsplit((parts[0], parts[1], parts[2],
            urllib.urlencode(params), parts[4])), value[0])
    
    
    
    
