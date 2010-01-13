""" Create Mixerlabs django test environment for zope testrunner """

import sys
import unittest

from django.conf import settings
from django.core.management import call_command
from django.db import transaction
from django.test import utils

import mix

test_database_name = 'test_django'

class DjangoLayer(object):
    """Adds townme-specific test_django database arounf zope unit tests.
    This provides a clean test database before running all unit tests.

    For perf, this does not try to restore the database after each individual
    test.  Assume individual unit tests will clean any tables or rows that they
    require clean as part of their own initialization.
    
    If special cleaning is not easy to add, a unit test can use this in its
    intialization to rebuild the entire test_db:    
        from util.django_layer import DjangoLayer
        DjangoLayer.rebuild_test_db():    
    """

    @classmethod
    def rebuild_test_db(cls):
        mix.call('pg:dropdb', dbname=test_database_name, quiet=True)
        mix.call('pg:createdb', dbname=test_database_name, quiet=True)
        from django.db import connection        
        con = connection.creation.connection
        con.close()
        settings.DATABASE_NAME = test_database_name
        cursor = con.cursor()
        # These two commands should have been combined, except that
        # south has a bug when calling the migrate subcommand - it expects all
        # optonal args to be present, which is only true when using the parser.
        # To see south's bug, run src/www/migrate syncdb --migrate
        #
        # mute stdout
        old_stdout = sys.stdout
        try:
            sys.stdout = open('/dev/null', 'w')
            call_command('syncdb', verbosity=0, interactive=False,
                    migrate=False)
            call_command('migrate', verbosity=0, interactive=False,
                    all_apps=True, no_initial_data=False)
        finally:
            sys.stdout = old_stdout

    @classmethod
    def setUp(cls):
        sys.stdout.write(' (takes ~20 seconds) ')
        sys.stdout.flush()
        utils.setup_test_environment()
        cls.rebuild_test_db()        

    @classmethod
    def tearDown(cls):        
        mix.call('pg:dropdb', dbname=test_database_name, quiet=True)
        utils.teardown_test_environment()

    @classmethod
    def testSetUp(cls):                
        transaction.enter_transaction_management()
        transaction.managed(True)        
        
    @classmethod
    def testTearDown(cls):
        transaction.rollback()
        transaction.leave_transaction_management()
        
        try:
            transaction.leave_transaction_management()
        except transaction.TransactionManagementError:
            pass
        else:
            raise transaction.TransactionManagementError(
                    "Call to enter_transaction_management must "
                    "have a matching call to leave_transaction_management")


def make_django_suite(*argv):
    """ Returns a test suite with a DjangoLayer built from from classes passed
    as args.  To use, add something like this to the bottom of your unit test:

    def test_suite():
        from util.django_layer import make_django_suite
        return make_django_suite(__name__)

    or, if you want to name the test classes explicitly,
        return make_django_suite(TestClass1, TestClass2, ....)
        
    or you could do even more fine grained stuff by building your own
    TestSuite object and optionally adding the DjangoLayer
    
    TODO: Support docstring tests somehow.
    """
    if len(argv) == 1 and isinstance(argv[0], basestring):
        # If the args are a single string, assume it is the module name
        # and all its classes that inherit from unittest.TestCase are game.
        import inspect
        module = sys.modules[argv[0]]
        if not inspect.ismodule(module):
            raise Exception("%s is not a module name or class" & argv[0])
        def is_test_case(a):
            try:
                return issubclass(a[1], unittest.TestCase)
            except TypeError:
                return False
        argv = [c[1] for c in inspect.getmembers(module) if is_test_case(c)]
        
    suite = unittest.TestSuite([unittest.makeSuite(arg) for arg in argv])
    suite.layer = DjangoLayer
    return suite
 