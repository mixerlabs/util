"""the 'a' in adb.py is for testing."""

import unittest
import functools
import util.adb as adb
import types
from util import *

class TestAdb(unittest.TestCase):
    def setUp(self):
        adb.set_query_fun(functools.partial(TestAdb._query, self))
        self._q = ''
        self._a = {}
        self._r = []
        self._kwargs = {}
        self._stopped = False

    def _query(self, query, args, **kwargs):
        self._q = query
        self._a = args
        self._kwargs = kwargs
        for r in self._r:
            stop = yield r
            if stop is not None:
                break

        self._stopped = True

    @staticmethod
    @adb.query
    @adb.subs
    def _simple_subs_args_sql(args, subs):
        """SELECT * FROM $tablename WHERE type = %(key)s"""

    def test_simple_sub_args(self):
        TestAdb._simple_subs_args_sql(tablename='MYTABLE', key=123)
        self.assertEqual(self._q, 'SELECT * FROM MYTABLE WHERE type = %(key)s')
        self.assert_('key' in self._a)
        self.assertEqual(self._a['key'], 123)

    @staticmethod
    @adb.query
    @adb.subs
    def _none_args_are_deleted_sql(args, subs):
        """SELECT * FROM foo WHERE $clause"""
        args.self.assert_(not 'nonearg' in args)

    def test_none_args_are_deleted(self):
        TestAdb._none_args_are_deleted_sql(self=self, nonearg=None, clause=None)
        self.assertEqual(self._q, 'SELECT * FROM foo WHERE $clause')
        self.assert_(not 'nonearg' in self._a)
        self.assert_(not 'clause' in self._a)

    @staticmethod
    @adb.singular
    @adb.query
    def _singular_sql(args, subs):
        """"""

    def test_singular(self):
        self._r = [{'row': i} for i in xrange(10)]
        ret = TestAdb._singular_sql()
        self.assert_(isinstance(ret, adb.Storage))
        self.assertEquals(ret.row, 0)

    @staticmethod
    @adb.value
    @adb.query
    def _value_sql(args, subs):
        """"""

    def test_value(self):
        self._r = [{'row': i} for i in xrange(10)]
        ret = TestAdb._value_sql()
        self.assert_(isinstance(ret, int))
        self.assertEquals(ret, 0)

    @staticmethod
    @adb.query
    @adb.defaults(arg0='foo')
    @adb.subdefaults(sub0='bar')
    @adb.subs
    def _defaults_sql(args, subs):
        """SELECT * FROM $sub0 WHERE type=%(arg0)s"""

    def test_default_args(self):
        TestAdb._defaults_sql()
        self.assertEquals(self._q, 'SELECT * FROM bar WHERE type=%(arg0)s')
        self.assert_('arg0' in self._a)
        self.assertEquals(self._a['arg0'], 'foo')

    def test_default_args_replacement(self):
        TestAdb._defaults_sql(arg0='my_foo')
        self.assertEquals(self._q, 'SELECT * FROM bar WHERE type=%(arg0)s')
        self.assert_('arg0' in self._a)
        self.assertEquals(self._a['arg0'], 'my_foo')

    def test_default_subs_replacement(self):
        TestAdb._defaults_sql(sub0='my_bar')
        self.assertEquals(self._q, 'SELECT * FROM my_bar WHERE type=%(arg0)s')
        self.assert_('arg0' in self._a)
        self.assertEquals(self._a['arg0'], 'foo')

    @staticmethod
    @adb.query
    def _fun_manipulation_sql(args, subs):
        """SELECT * FROM $table WHERE type=%(key)s"""

        args.self.assert_('table' in args)
        subs.table = args.table + '_TESTING'

        args.self.assert_('key' in args)
        args.key = 'test_' + args.key

    def test_fun_manipulation(self):
        TestAdb._fun_manipulation_sql(self=self, table='table0', key='unittest')

        self.assertEquals(self._q, 'SELECT * FROM table0_TESTING WHERE type=%(key)s')
        self.assert_('key' in self._a)
        self.assertEquals(self._a['key'], 'test_unittest')

    @staticmethod
    @adb.query
    @adb.sscursor
    @adb.subs
    def _sscursor_sql(args, subs):
        """NULL"""

    def test_sscursor(self):
        rows = TestAdb._sscursor_sql()

        self.assert_(isinstance(rows, types.GeneratorType))
        rows = map(None, rows)
        self.assertEquals(0, len(rows))
        self.assert_('cursor_type' in self._kwargs)
        self.assert_(not 'streaming' in self._kwargs)
        self.assertEquals(self._kwargs['cursor_type'], sym.ss_cursor)

    @staticmethod
    @adb.query
    @adb.streaming
    def _streaming_sql(args, subs):
        """NULL"""

    def test_row_iter(self):
        self._r = [{'num': i} for i in xrange(10)]
        rows = TestAdb._streaming_sql()

        self.assert_(isinstance(rows, types.GeneratorType))
        rows = map(None, rows)
        self.assertEquals(self._r, rows)

    @staticmethod
    @adb.singular
    @adb.query
    @adb.streaming
    def _streaming_sql_singular(args, subs):
        """NULL"""

    def test_row_iter_singular(self):
        self._r = [{'num': i} for i in xrange(10)]

        self.assert_(not self._stopped)
        row = TestAdb._streaming_sql_singular()

        self.assertEquals(row.num, 0)
        self.assert_(self._stopped)


    @staticmethod
    @adb.query
    @adb.const_db
    def _const_db_sql(args, subs):
        """NULL"""

    def test_const_db(self):
        TestAdb._const_db_sql()
        self.assertEquals(self._kwargs['dbtype'], sym.const)

    @staticmethod
    @adb.query
    def _dynamic_db_sql(args, subs):
        """NULL"""

    def test_dynamic_db(self):
        TestAdb._dynamic_db_sql()
        self.assert_(not 'dbtype' in self._kwargs)


def test_suite():
    from util.django_layer import make_django_suite
    return make_django_suite(__name__)


if __name__ == '__main__':
    unittest.main()
