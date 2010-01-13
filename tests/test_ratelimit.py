import unittest
import datetime
import uuid
import logging

if __name__ == '__main__':
    import conf
    conf.configure_django('www.settings')

from util.ratelimit import should_limit, clear_limit


class TestRatelimit(unittest.TestCase):
    def now_plus(self, minutes):
        return self.now + datetime.timedelta(minutes=minutes)
    
    def setUp(self):
        # Need an arbitrary starting minute (such as right now)
        self.now = datetime.datetime.now()

    def test_lots_of_events_in_same_minute(self):
        key = uuid.uuid1()
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertTrue(should_limit(key, 3, 5, self.now))        
        clear_limit(key, 3,  self.now)
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertTrue(should_limit(key, 3, 5, self.now))


    def test_multi_minutes(self):
        key = uuid.uuid1()
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(0)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(1)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(2)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(3)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(4)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(5)))
        # Should limit because sum of events over last 3 minutes is (1+1+2) > 3
        self.assertTrue(should_limit(key, 3, 3, self.now_plus(5)))
        # No limit because sum of events over last 3 minutes is (1+0+2) <= 3
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(7)))
        # Should limit because sum of events over last 3 minutes is (2+0+2) > 3
        self.assertTrue(should_limit(key, 3, 3, self.now_plus(7)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(10)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(11)))
        self.assertFalse(should_limit(key, 3, 3, self.now_plus(12)))

    def test_multi_increment(self):
        key = uuid.uuid1()
        self.assertFalse(should_limit(key, 3, 5, self.now, howmuch=3))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertFalse(should_limit(key, 3, 5, self.now))
        self.assertTrue(should_limit(key, 3, 5, self.now))        
        clear_limit(key, 3,  self.now)
        self.assertFalse(should_limit(key, 3, 5, self.now, howmuch=5))
        self.assertTrue(should_limit(key, 3, 5, self.now))


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)

if __name__ == '__main__':
    unittest.main()
