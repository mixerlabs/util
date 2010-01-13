import datetime
import unittest

from util._datetime import datetime_from_obj

class TestDatetime(unittest.TestCase):
    def test_datetime_from_obj(self):
        now = datetime.datetime.now()
        self.assertEquals(now, datetime_from_obj(now))
        self.assertEquals(now, datetime_from_obj(str(now)))
        self.assertEquals(now, datetime_from_obj(unicode(now)))
        self.assertEquals(None, datetime_from_obj('Duh'))
        self.assertEquals(None, datetime_from_obj(''))
        self.assertEquals(None, datetime_from_obj(None))
        self.assertEquals(None, datetime_from_obj(
                u'Ladies and gentlemen, Mr. Conway Twitty!'))
        self.assertEquals(None, datetime_from_obj(3.14159))
        self.assertEquals(None, datetime_from_obj(unittest.TestCase))


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == "__main__":
    unittest.main()
