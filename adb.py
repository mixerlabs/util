"""a better `db.py'?

adb.py allows you to decorate functions in order to process SQL
queries. The query itself is specified in the docstring of the
function, and the function is called with `args': arguments to the SQL
query, and `subs': subsitutions to be made in the SQL query (without
any escaping). Arguments follow the standard "%(keyword)s" syntax, and
substitutions are specifed in "shell-like" variables - "$subst".

For example, this defines a query:

@adb.query
def get_foo_bar(args, subs):
    '''SELECT * FROM localeze.baserecords WHERE pid=%(pid)s $limit'''

    if args.get('limit'):
        subs.limit = 'LIMIT %d' % args.limit
    else:
        subs.limit = ''

A query is then performed by:

  get_foo_bar(pid=7673, limit=10)

and a Storage object is returned with the row-values.

The query is performed after the function has been called, and will be
performed with any amendments that have been made to `args' and
`subs'.

There are other utility decorators, too:

  defaults,
  subdefaults,
  subs,
  value, and
  singular.

`defaults' and `subdefaults' provide default arguments to `args' and
`subs' respectively (eg. in the above example, instead of having the
else-clause to specify an empty sub for `limit', we could have specified

@adb.query
@adb.subdefaults(limit='')
def get_foo_ba..

`subs' will automatically perform subsitutions using the passed-in
kwargs. `value' returns the first column of the first row, and
`singular' returns only one row.

Furthermore, there are a number of options, also activated by
decorators.

They are:

  streaming    - streams results using a generator
  sscursor     - uses a server side cursor, implies streaming
  insert_id    - returns the insert id (LAST_INSERT_ID())
  const_db     - uses the const, or immutable database
  dynamic_db   - uses the dynamic, or mutable database (default)
"""

from __future__ import absolute_import
from __future__ import with_statement

import functools
import MySQLdb
import time
import logging
import types
from django.db import connection as django_connection
from copy import copy

from util import *
from util.functional import updated, deleted, switch
import conf.db

# So that we can access exceptions in a backend independent way.
db = MySQLdb

log = logging.getLogger(__name__)

# To get the webframe settings.
import webframe.settings

__all__ = ['query', 'singular', 'value', 'subs',
           'subdefaults', 'defaults', 'last_query_insert_id',
           'set_query_fun', 'sscursor']

def _mysql_connect(host=None, username=None, password=None):
    '''If username is None, this uses webframe.settings.DATABASE_USER
and webframe.settings.DATABASE_PASSWORD for credentials. If it is not
None, then the provided username and password will be used.'''
    if host is None:
        host = conf.db.MUTABLE_DATABASE_HOST
    if username is None:
        username = conf.db.MUTABLE_DATABASE_USER
        password = conf.db.MUTABLE_DATABASE_PASSWORD
    if password is None:
        password = ''

    return MySQLdb.connect(host,
                           username,
                           password,
                           charset='utf8',
                           use_unicode=True)

class connection(object):
    _connset = set()
    _const_connset = set()

    def __init__(self, dbtype=sym.dynamic):
        self.conn = None
        self.connset = switch(dbtype, dynamic=self._connset, const=self._const_connset)

    def __enter__(self):
        while len(self.connset) > 0:
            self.conn = self.connset.pop()
            # ping returns non-zero if connection is dead
            # should we do anything to clean up if there's 
            # an error?
            if self.conn.ping(True) == 0:
                break
            self.conn = None

        if self.conn is None:
            if self.connset is self._const_connset:
                self.conn = _mysql_connect(host=conf.db.DATABASE_HOST,
                                           username=conf.db.DATABASE_USER,
                                           password=conf.db.DATABASE_PASSWORD)
            else:
                self.conn = _mysql_connect()

            self.conn.autocommit(1)

        return self.conn

    def __exit__(self, type, value, x):
        self.connset.add(self.conn)
        return None

_debug = False
def debug(onoff):
    global _debug
    _debug = onoff

def adb_errorhandler(connection=None, cursor=None, errorclass=None, errorvalue=None):
    print "ERROR: conn: %s, cursor %s, class %s, val %s " % (connection, cursor, errorclass, errorvalue)
    raise cursor

def _do_query(query, args, cursor_type=sym.client_cursor, insert_id=False, dbtype=sym.dynamic):
    with connection(dbtype) as conn:
        c = conn.cursor(
            switch(cursor_type,
                   client_cursor=MySQLdb.cursors.DictCursor,
                   ss_cursor=MySQLdb.cursors.SSDictCursor))
        c.errorhandler = adb_errorhandler

    #    print 'executing query', query, 'with args', args
        global _debug
        if _debug:
            print "%s %s" % (query, args)

        before = time.time()

        # We're agnostic -- args can be regular args or kwargs.
        #print 'executing', query
        c.execute(query, args)

        if not insert_id:
            while True:
                row = c.fetchone()
                if row is None:
                    break
                stop = yield row
                if stop is not None:
                    break
        else:
            yield dict(insert_id=c.lastrowid)

        c.close()
        #print '~stopped', query

        try:
            django_connection.queries.append(
                {'time': '%.3f' % (time.time() - before),
                 'sql': query % (args or tuple())}
            )
        except TypeError:
            pass

def opt_or(method, opt, orelse):
    opts = getattr(method, 'opts', {})
    return opts.get(opt, orelse)

_query_fun = _do_query
def set_query_fun(fun):
    global _query_fun
    _query_fun = fun

# Query function decorators -- this is where the meat is.
def streamquery(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        query = method.__doc__
        subs = storage()
        kwargs = storage(kwargs)
        def strip_empty_keys():
            map(kwargs.pop, [k for k, v in kwargs.iteritems() if v is None])

        strip_empty_keys()
        method(kwargs, subs)
        strip_empty_keys()

        opts = getattr(method, 'opts', {})

        # XXX use templates
        for k, v in subs.iteritems():
            if not v is None:
                query = query.replace('$' + k, str(v))

        queryargs = args or dict(kwargs)
        rows = _query_fun(query, queryargs, **opts)
        stop = None
        while True:
            stop = yield storage(rows.send(stop))

    return wrapper

def mapquery(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        streaming = streamquery(method)
        return map(None, streaming(*args, **kwargs))

    return wrapper

def insertidquery(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        streaming = streamquery(method)
        res = map(None, streaming(*args, **kwargs))
        assert len(res) == 1, len(res)
        return res[0].insert_id

    return wrapper

def query(method):
    if opt_or(method, 'streaming', False):
        method.opts = deleted(method.opts, 'streaming')
        return streamquery(method)
    elif opt_or(method, 'insert_id', False):
        return insertidquery(method)
    else:
        return mapquery(method)

def singular(method):
    # This is kind of ugly. Perhaps the best way to actually do this
    # is to bake such options into the query decorator itself (and
    # make it a callable decorator..)

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        rows = method(*args, **kwargs)
        if isinstance(rows, types.GeneratorType):
            try:
                row = rows.next()
            except StopIteration:
                return storage()

            try:
                rows.send(True)
            except StopIteration:
                pass

            return row
        else:
            if rows:
                return rows[0]
            else:
                return storage()

    return wrapper

def value(method):
    method = singular(method)

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        return method(*args, **kwargs).values()[0]

    return wrapper

def subs(method):
    @functools.wraps(method)
    def wrapper(args, subs):
        method(args, subs)

        for k, v in args.iteritems():
            if not k in subs:
                subs[k] = v

    return wrapper

def _update_opt(method, **kwargs):
    method.opts = updated(getattr(method, 'opts', {}), **kwargs)
    return method

# server side cursor.
def sscursor(method):
    return _update_opt(method, cursor_type=sym.ss_cursor, streaming=True)

def streaming(method):
    return _update_opt(method, streaming=True)

def insert_id(method):
    return _update_opt(method, insert_id=True)

def const_db(method):
    return _update_opt(method, dbtype=sym.const)

def dynamic_db(method):
    return _update_opt(method, dbtype=sym.dynamic)

class subdefaults:
    def __init__(self, **kwargs):
        self.subdefaults = kwargs

    def __call__(self, method):
        @functools.wraps(method)
        def wrapper(args, subs):
            method(args, subs)

            for k, v in self.subdefaults.iteritems():
                if not k in subs or not subs[k]:
                    subs[k] = v

        return wrapper

class defaults:
    def __init__(self, **kwargs):
        self.defaults = kwargs

    def __call__(self, method):
        @functools.wraps(method)
        def wrapper(args, subs):
            method(args, subs)

            for k, v in self.defaults.iteritems():
                if not k in args:
                    args[k] = v

        return wrapper
