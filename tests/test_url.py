import unittest
import socket
import urllib2

from util import url as URL

class TestOpenSafe(unittest.TestCase):
    """Test safe URL opening -- this unittest assumes the testing host
    has network access. Tests will fail otherwise."""

    def test_basic_ok_hosts(self):
        urls = ['http://www.google.com/',
                'http://www.google.com:80',
                'http://www.google.com']

        for url in urls:
            google = URL.open_safe(url).read()
            self.assert_(google.find('I\'m Feeling Lucky') >= 0)

    def test_evil_urls(self):
        tests = [
            (URL.ProtocolError        , 'ftp://ftp.kernel.org/'),
            (URL.ProtocolError        , '/foo/bar/baz'),
            (URL.NetworkLocationError , 'http:///foo/bar/baz'),
            (URL.ResolverError        , 'http://nonexistent.townme.com/'),
            (URL.AccessError          , 'http://10.0.0.1'),
            (URL.AccessError          , 'http://192.168.6.7'),
            (URL.AccessError          , 'http://172.16.9.10'),
            (URL.AccessError          , 'http://127.0.0.1'),
            (URL.AccessError          , 'http://127.1.2.3'),

            # ec2base.mixerlabs.com points to a 10/8 address.
            (URL.AccessError          , 'http://ec2base.mixerlabs.com/'),
            (URL.AccessError          , 'http://loopback.mixerlabs.com/'),

            (urllib2.HTTPError        , 'http://www.townme.com/nonexistant'),
        ]

        for exc, loc in tests:
            self.assertRaises(exc, URL.open_safe, loc)

    def test_timeout_urls(self):
        urls = [
            # this is a internal ip of citi.umich.edu -- it is blocked
            # by the umich firewall, and doesn't drop connections, so
            # it's nice for testing timeouts.
            'http://141.211.133.111/',
        ]

        for url in urls:
            try:
                URL.open_safe(url)
            except urllib2.URLError, e:
                a, = e.args
                self.assert_(isinstance(a, socket.timeout))
            else:
                self.assert_(False)

def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)

if __name__ == '__main__':
    unittest.main()
