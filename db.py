from __future__ import with_statement
from __future__ import absolute_import

import sys
import os
import uuid
import signal
import traceback
import struct
import re
import weakref
from collections import defaultdict
from functools import partial, wraps
from decorator import decorator
from itertools import count, chain, izip, islice, repeat
from threading import Thread
import Queue
from time import time
from cStringIO import StringIO

from django.db import models, backend, connection, IntegrityError
from django.db import transaction as dtransaction
from django.db.models import sql
from django.db.models.query import QuerySet
from django.db.models.fields import AutoField
from django.conf import settings

try:
    from django.contrib.gis.geos.geometry import GEOSGeometry     # django 1.1
except ImportError:
    from django.contrib.gis.geos.geometries import GEOSGeometry   # django 1.0.2

from util.functional import (memoize, memoize_per_proc,
                             memoize_zap_cache, pick, singleton)
from util.str import compress_hex
from util.dict import getvalues
from util.seq import first, empty, nonempty
from util.io import GeneratorFile
from util.py import Proxy
from util import mixlog

log = mixlog()

def engine():
    return settings.DATABASE_ENGINE

class ConnectionWrapper(object):
    """A wrapper for connections so that we can have a weakref to
    them."""
    def __init__(self, connection):
        self.connection = connection

class CommitBlock(object):
    def __enter__(self):
        dtransaction.enter_transaction_management()
        dtransaction.managed(True)

    def __exit__(self, exc_type, exc_value, traceback):
        if dtransaction.is_dirty():
            if exc_value:
                dtransaction.rollback()
            else:
                dtransaction.commit()
        dtransaction.leave_transaction_management()

commit_block = CommitBlock

class CursorWrapper(object):
    """Captures SQL queries from the given cursor and hands it to a
    user-defined function, but otherwise stays out of the way. Modified
    from django/db/backends/util.py"""

    def __init__(self, cursor, logger):
        self.cursor = cursor
        self.logger = logger

    @staticmethod
    def wrap_connection(connection, logger):
        old = connection.cursor
        connection.cursor = lambda: CursorWrapper(old(), logger)
        connection.cursor._wrap_connection = (
            lambda conn: CursorWrapper.wrap_connection(conn, logger)
        )

        def unwrap():
            connection.cursor = old

        connection.cursor._unwrap = unwrap

    def execute(self, sql, params=()):
        start = time()
        try:
            return self.cursor.execute(sql, params)
        finally:
            self.logger({
                'sql': sql % params,
                'time': time() - start,
                'times': 1, 
            })

    def executemany(self, sql, param_list):
        start = time()
        try:
            return self.cursor.executemany(sql, param_list)
        finally:
            self.logger({
                'sql': sql,
                'time': time() - start,
                'times': len(param_list),
                })

    def copy_from(self, file, table, **kwargs):
        start = time()
        try:
            return self.cursor.copy_from(file, table, **kwargs)
        finally:
            self.logger({
                'sql'   : 'COPY "%s" FROM ?' % table,
                'time'  : time() - start,
                'times' : 1,
            })

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

class WithWrapper(object):
    def __enter__(self):
        CursorWrapper.wrap_connection(connection, self._logger)

    def __exit__(self, exc_type, exc_value, traceback):
        connection.cursor._unwrap()

class PrintQueries(WithWrapper):
    def __init__(self, separator=None):
        self.separator = separator

    def _logger(self, query):
        if self.separator is not None:
            print self.separator

        print >>sys.stderr, '%.3f: %s [%d]' % (
            query['time'], query['sql'], query['times'])

        #traceback.print_stack()

print_queries = PrintQueries

class SigInfoPrintQueries(PrintQueries):
    """The siginfo print queries debugger toggles printing of queries
    on SIGINFO (control-t). Doesn't work on Linux."""
    def __init__(self, *args, **kwargs):
        super(SigInfoPrintQueries, self).__init__(*args, **kwargs)
        self._is_on = False

        assert hasattr(signal, 'SIGINFO')

    def __enter__(self, *args, **kwargs):
        super(SigInfoPrintQueries, self).__enter__(*args, **kwargs)

        self._old_sighandler = signal.getsignal(signal.SIGINFO) or signal.SIG_DFL
        signal.signal(signal.SIGINFO, self._handler)

    def __exit__(self, *args, **kwargs):
        super(SigInfoPrintQueries, self).__exit__(*args, **kwargs)

        signal.signal(signal.SIGINFO, self._old_sighandler)

    def _handler(self, signum, frame):
        self._is_on = not self._is_on

    def _logger(self, *args, **kwargs):
        if self._is_on:
            super(SigInfoPrintQueries, self)._logger(*args, **kwargs)

siginfo_print_queries = SigInfoPrintQueries

def print_queries_for_fn(*pq_args, **pq_kwargs):
    @decorator
    def wrapper(fun, *args, **kwargs):
        with print_queries(*pq_args, **pq_kwargs):
            fun(*args, **kwargs)

    return wrapper

def print_queries_in_session():
    def logger(query):
        print >>sys.stderr, '%.3f: %s [%d]' % (
            query['time'], query['sql'], query['times'])

    CursorWrapper.wrap_connection(connection, logger)

def transaction(fun):
    """Do a 'transaction' -- commit & then defer to
    transaction.commit_on_success."""
    def _transaction(fun, *args, **kwargs):
        dtransaction.commit()
        return fun(*args, **kwargs)

    return dtransaction.commit_on_success(decorator(_transaction, fun))

class SuspendAndTransact(object):
    """Suspend the current connection, create a new connection, and
    commit the transaction if successful. Fully compatible with Django
    transaction management."""
    def __init__(self, connection=None):
        self.connection = connection

    def __enter__(self):
        self.was_dirty = dtransaction.is_dirty()
        self.old_connection = connection.connection

        connection.connection = self.connection

        dtransaction.enter_transaction_management()
        dtransaction.managed(True)
        dtransaction.set_clean()

    def __exit__(self, exc_type, exc_value, traceback):
        if dtransaction.is_dirty():
            if exc_type is None:
                dtransaction.commit()
            else:
                dtransaction.rollback()

        dtransaction.leave_transaction_management()
        if self.was_dirty:
            dtransaction.set_dirty()

        connection.connection = self.old_connection

suspend_and_transact = SuspendAndTransact

def with_own_connection(fun):
    """A decorator to that ensures the given function is given its own
    connection and transaction context. Upon return, the connection
    commits."""

    conn = list()
    def wrapper(fun, *args, **kwargs):
        if not conn:
            conn.append(mk_connection())
        with suspend_and_transact(conn[0]):
            return fun(*args, **kwargs)

    return decorator(wrapper, fun)

def lock_model(model, exclusive=True, row=False):
    """Lock the table for the given Django model. The lock is released
    at the end of the current transaction."""
    assert engine() == 'postgresql_psycopg2'

    mode = '%s %s' % (
        'ROW' if row else '',
        'EXCLUSIVE' if exclusive else 'SHARE'
    )

    connection.cursor().execute('''LOCK TABLE %s IN %s MODE''' % (
        model._meta.db_table, mode
    ))

def lock_rows(qs):
    """Ensures the resulting rows are exclusively locked for this
    transaction."""
    pks = [row.pk for row in qs]
    cursor = qs.query.connection.cursor()
    cursor.execute(
        '''SELECT id FROM %s WHERE id IN (%s) FOR UPDATE''' %(
            qs.query.model._meta.db_table, ', '.join(['%s']*len(pks))
        ),
        pks
    )
    cursor.fetchall()                   # is this necessary?

def page(qs, chunksize=10000, start_pk=0, use_temp_table=True):
    """Efficiently page a queryset (this is a generator) with the
    given chunksize."""
    # Warn here?

    if not use_temp_table:
        last = first(qs.order_by('pk').reverse())
        if last is None:
            return
        for i in count():
            start = start_pk + (i * chunksize)
            stop = start + chunksize
            chunk_qs = qs.filter(pk__gte=start, pk__lt=stop)
            for item in chunk_qs:
                yield item
            if stop > last.pk:
                return

    if start_pk != 0:
        qs = qs.filter(pk__gte=start_pk)

    # For MySQL we have fancy database paging, otherwise we just trust
    # the database to do the right thing?

    if engine() != 'mysql':
        for r in qs.all():
            yield r
    else:
        query = qs.query
        query.as_sql()  # triggers a bunch of stuff

        cols = query.get_default_columns(as_pairs=True)[0]

        get_columns = query.get_columns
        # This fixes a number of issues including representational
        # normalization within geodjango and so on. (It's also technically
        # the right thing to do! but the monkeypatching is a little ugly.)
        query.get_columns = lambda *a, **kw: ['`%s`.`%s`' % (d, c) for d, c in cols]
        query_sql, query_params = query.as_sql()
        from_ = query.get_from_clause()[0][0]
        query.get_columns = get_columns

        # First we ensure that the queryset isn't trying to do anything
        # too fancy that we couldn't deal with. We could actually make
        # these work, with some extra effort.
        if query.ordering_aliases:
            raise ValueError, 'Paging not supported for ordering aliases'
        if query.group_by:
            raise ValueError, 'Paging not supported for group_by clauses'
        if query.select_related:
            raise ValueError, 'Paging not supported with select_related'

        table = '`%s`' % compress_hex(uuid.uuid1().hex, alphabet='_abcdefghijklmnopqrstuvwxyz')

        cursor = query.connection.cursor()
        cursor.execute('CREATE TEMPORARY TABLE %s LIKE %s' % (table, from_))
        cursor.execute('ALTER TABLE %s modify `id` int(11)' % table)
        cursor.execute('ALTER TABLE %s DROP PRIMARY KEY' % table)
        cursor.execute(('ALTER TABLE %s ADD COLUMN '
                        '`__paging_pk` int(11) PRIMARY KEY AUTO_INCREMENT') % table)

        cols = pick(1, cols)
        sql = 'INSERT INTO %s (%s) %s' % (table, ','.join(cols), query_sql)

        cursor.execute(sql, query_params)

        index_start = len(query.extra_select.keys())

        # We need a different set of columns for the actual query because
        # backends like GeoDjango might have different select wrappers.
        qn = query.connection.ops.quote_name
        cols = []
        for f, _ in query.model._meta.get_fields_with_model():
            col = qn(f.column)

            if hasattr(query, 'get_select_format'):
                col = query.get_select_format(f) % col

            cols.append(col)

        # Now, begin the paging!
        for iteration in count():
            begin, end = iteration*chunksize, (iteration+1)*chunksize

            sql = ('SELECT %s FROM %s '
                   'WHERE `__paging_pk` >= %d '
                   'AND `__paging_pk` < %d') % (','.join(cols), table, begin, end)

            if cursor.execute(sql) == 0:
                break

            for row in cursor.fetchall():
                yield query.model(*row[index_start:])

        cursor.execute('DROP TABLE %s' % table)

def mk_connection():
    """Makes a new connection to the (Django) database."""
    # Ugh, this is a h.a.c.k.

    old = connection.connection
    connection.connection = None

    connection.cursor()                 # creates a new connection
    conn = connection.connection
    connection.connection = old

    if hasattr(connection.cursor, '_wrap_connection'):
        # psycopg2.connection is read-only, so proxy it.
        conn = Proxy(conn)
        connection.cursor._wrap_connection(conn)

    return conn

def get_obj(obj):
    """get-the-object!"""
    if isinstance(obj, tuple):
        return obj[0]
    else:
        return obj

def get_rels(obj):
    """get-the-rels!"""
    if isinstance(obj, tuple):
        return obj[1]
    else:
        return []

def naive_batch_insert(objects):
    O, R = get_obj, get_rels
    objects = iter(objects)

    for obj in objects:
        O(obj).save()
        pk = O(obj).pk

        for field, relobjects in R(obj):
            for relobj in relobjects:
                setattr(O(relobj), field, pk)

            naive_batch_insert(relobjects)

    dtransaction.commit_unless_managed()

def batch_insert_psycopg2(objects, chunk_size=10000000, stream=True):
    """Lightning-fast batch insertion via PSQL COPY via an in-memory
    file object. It's a little bit quirky, but I'm trying to follow best
    practices as per:

      http://wiki.postgresql.org/wiki/COPY and
      http://www.postgresql.org/docs/current/interactive/sql-copy.html

    Future optimizations:

      - separate out the rel-obj vs. non code paths, so we don't need
      to generate & waste a bunch of sequence ids for small batch inserts.

    Questions:

      - how does postgres deal with indexing during copy inserts?
      would it be beneficial to turn it off?
      - would a multiple values(...) query faster/better than the
      COPY? could we do the same sort of streaming?
      - do transactions hurt performance?

    We have to use our own database connection here because all object
    generation is propagated lazily, any of the generators may use the
    Django connection in the middle of the COPY, which would invalidate
    it.

    This also means that we don't follow Django's normal transaction
    semantics because its transaction system only deals with the
    ``system'' connection. We opt to always commit at the end of each
    insert."""

    if stream:
        conn = mk_connection()
    else:
        if not connection.connection:
            connection.cursor()         # force connect.
        conn = connection.connection

    rv = do_batch_insert_psycopg2(objects, chunk_size, conn, stream)

    if stream:
        conn.commit()

    return rv

class PgSequence(object):
    """Iterator for sequence numbers, attempts to be efficient by
    reserving blocks at a given time. Assumes the corresponding
    PostgreSQL sequence table exists."""

    _generators = {}

    @classmethod
    def get(cls, table, column):
        """Get a PgSequence instance for (table, column). For a given
        process, this instance will be shared, and several clients can
        have an iterator on the instance at any given time."""
        key = (os.getpid(), table, column)
        if key not in cls._generators:
            cls._generators[key] = cls(table, column)

        return cls._generators[key]

    @classmethod
    def kill(cls, table):
        """Kill all PgSequence instances for the given table. When
        `table' is None, cleanup state for all tables."""
        pid = os.getpid()
        for key, instance in cls._generators.items():
            pid_, table_, _ = key
            if pid_ == pid and (table is None or table_ == table):
                instance._kill()

    def __init__(self, table, column):
        self.table  = table
        self.column = column
        self._iter  = self.__iter()

    def __iter__(self):
        while True:
            yield self._iter.next()

    def _kill(self):
        self._iter.close()

    def __iter(self):
        howmany     = 2
        howmany_max = howmany * 10000
        cur         = mk_connection().cursor()
        qn          = connection.ops.quote_name

        while True:
            cur.execute(
                ('SELECT nextval(\'%s\') FROM generate_series(1, %d)') % (
                    qn('%s_%s_seq' % (self.table, self.column)), howmany
                )
            )

            for (seqid,) in cur.fetchall():
                yield seqid

            # Grow exponentially up to the chunk size.
            howmany = min(howmany * 2, howmany_max)

batch_insert_psycopg2_cleanup = PgSequence.kill

def do_batch_insert_psycopg2(objects, chunk_size, conn, do_stream):
    qn = connection.ops.quote_name

    objects = iter(objects)

    O, R = get_obj, get_rels

    try:
        object0 = objects.next()
    except StopIteration:
        return

    meta = O(object0)._meta
    has_rels = R(object0) != []

    # We just wanted the meta, so patch the object back in.
    objects = chain([object0], objects)

    def prep_field(o, f):
        # TODO: better way to check for unset?

        # We call pre_save *first* as some fields will auto populate
        # values here, and so it may change the results of the
        # subsequent getattr().
        pre_save = f.pre_save(o, True)
        attr = getattr(o, f.attname)
        if attr is None:
            return r'\N'                   # empty/null.
        elif isinstance(attr, GEOSGeometry):
            # GEOS doesn't support SRIDs/EWKB propertly, so we patch
            # it up.
            #
            # EWKB doesn't seem so well documented, so this is mostly
            # hearsay.

            end = '>'                   # big-endian

            b = attr.wkb
            srid_mask = 0x20000000
            srid = 4326

            byte, = struct.unpack('%sB' % end, b[0])
            end = '>' if byte == 0 else '<'

            gtype, = struct.unpack('%sI' % end, b[1:5])

            if not (gtype & srid_mask):
                gtype |= srid_mask
                b = (buffer(struct.pack('%sBII' % end, byte, gtype, 4326)) +
                     buffer(b, 5))

            return str(b).encode('hex')
        else:
            val = f.get_db_prep_save(pre_save)

            if isinstance(val, (str, unicode)):
                # http://www.postgresql.org/docs/current/interactive/sql-copy.html
                #
                # Backslash characters (\) can be used in the COPY data to
                # quote data characters that might otherwise be taken as
                # row or column delimiters. In particular, the following
                # characters must be preceded by a backslash if they
                # appear as part of a column value: backslash itself,
                # newline, carriage return, and the current delimiter
                # character.
                #
                # This finagling slows the insert down quite a bit. So
                # this might be a target for future optimization. In
                # reality, most strings won't need any substitution,
                # so perhaps just employing a regexp search for the
                # target characters would be faster.

                val = val.replace('\\', '\\\\')
                val = val.replace('\n', r'\n')
                val = val.replace('\r', r'\r')
                val = val.replace('\t', r'\t')

            return val

    # Gets all local fields except for non-primary-key auto fields.
    fields = filter(
        lambda f: (not isinstance(f, AutoField) or (has_rels and f == meta.pk)),
        meta.local_fields
    )
    field_names = [f.column for f in fields]

    if has_rels and isinstance(meta.pk, AutoField):
        sequence = PgSequence.get(meta.db_table, meta.pk.column)
    else:
        sequence = repeat(None)

    cur = conn.cursor()

    while True:
        relobjs = defaultdict(lambda: [])  # per-type related objects

        def sqlcopygen():
            num_bytes = 0
            for obj, pk in izip(objects, sequence):
                obj, rel = O(obj), R(obj)

                if pk is not None:
                    # In this case, we have rels, and we need to fix
                    # up the underlying object, and also populate our
                    # relobjs.

                    # an autofield, so set the pk on the object.
                    setattr(obj, meta.pk.attname, pk)

                    # Fix up the related objects & add them to the queue.
                    for field, objs in rel:
                        # TODO: lazify this bit. by using a db connection
                        # per related model, we could actually stream all
                        # of these simultaneously too.
                        objs = list(objs)
                        if nonempty(objs):
                            relobjs[type(O(objs[0]))].append((field, objs, pk))
                else:
                    assert rel == []

                values = map(partial(prep_field, obj), fields)

                # Convert to string.  Try to interpret values as unicode.
                # Sometimes this fails.
                try:
                    values = map(unicode, values)
                except UnicodeDecodeError, e:
                    log.warn('unicode decode failed %r', values)
                    values = map(str, values)
                s = '\t'.join(values) + '\n'
                try:
                    s = s.encode('utf-8')
                except UnicodeDecodeError, e:
                    log.warn('unicode decode failed %r', values)
                yield s

                num_bytes += len(s)
                if num_bytes > chunk_size:
                    break

        sqlcopygen = sqlcopygen()

        # First determine whether we're done.
        try:
            object0 = sqlcopygen.next()
        except StopIteration:
            break  # We're done.

        sqlcopygen = chain([object0], sqlcopygen)

        if do_stream:
            bytes = GeneratorFile(sqlcopygen)
        else:
            bytes = StringIO(''.join(sqlcopygen))

        try:
            cur.copy_from(bytes,
                          qn(meta.db_table),
                          sep='\t',
                          null=r'\N',
                          columns=field_names)
        except cur.connection.IntegrityError, ie:
            raise IntegrityError, ie.message

        # Could easily extend this to a DAG of related objects, since it's
        # so nicely inherently recursive.
        def relobjgen(relobjs):
            """Fixup all related objects."""

            for field, objs, pk in relobjs:
                for obj in objs:
                    setattr(O(obj), field, pk)
                    yield obj

        for r in relobjs.values():
            do_batch_insert_psycopg2(relobjgen(r), chunk_size, conn, do_stream)

def batch_insert(objects, **kwargs):
    """Do batch insertion of django model instances (objects being any
    iterable yielding those!). This dispatches to a database-specific
    implementation. There are some caveats:

      - We only support INSERTs, not UPDATEs
      - None of the django ORM courtesies are given to you
      - The list of objects MUST be homogenously typed, and we'll
        blindly assume it is. Your own damn fault if it isn't.
      - We don't send post save signals or any of that sort of
        nonsense.
      - We'll also assume that if the first object does not have any
        related objects, subsequent objects won't, either.

    batch_insert can also handle (batch-)inserting related objects
    that are created in parallel. This is done by specifying the
    attribute in the related object that stores the key of the "parent"
    object. This is specified via a tuple (in place of the naked
    object):

      (object, [('relattr', related_objects), ...])

    For example if you have two models:

    class M0(..):
        data = models.CharField()

    class M1(..):
        more_data = models.CharField()
        parent = models.ForeignKey(M0)

    And you want to save a bunch of `M0' objects along with a bunch of
    related `M1' objects, then where you'd normally do something like
    this:

      m0 = M0(data='foobar')
      m0.M1_set.add(more_data='hi')
      m0.M1_set.add(more_data='there')

    You instead do:

      objects = [
          (m0, [('parent_id', [M1(more_data='hi'), M1(more_data='there')])])
      ]

      batch_insert(objects)

    This also works recursively.
    
    Supported options (kwargs) are:

      `chunk_size' -- number of items in each chunk of inserts.
      `stream' -- stream the batch insertion all the way through. this requires
        the use of a separate connection, so beware of transaction semantics!
        (default=True)"""
    which = {
        'postgresql_psycopg2': batch_insert_psycopg2,
    }.get(engine(), naive_batch_insert)

    if which is naive_batch_insert:
        log.warn('Doing naive batch insertion! Go make some coffee, '
                 'this is gonna be a long one! Batch insertion not yet '
                 'implemented for %s.', engine())

    return which(objects, **kwargs)

def batch_insert_cleanup(table):
    """Clean up dangling state left by batch_insert() for the given
    table. batch_insert() will memoize data and create extra
    connections, this cleans this up. You must not call it during a
    running batch_insert(). If `table' is None, then cleanup state for
    all tables."""
    return {
        'postgresql_psycopg2': batch_insert_psycopg2_cleanup,
    }.get(engine(), lambda _: None)(table)

def delete_all_rows(model):
    """Deletes all rows of the given model, not trying to keep to
    constraints. This is *much* faster than the Django delete(), but can't
    be used in situations where related objects need to be GC'd."""
    cur = connection.cursor()
    qn = connection.ops.quote_name
    cur.execute('TRUNCATE TABLE %s' % qn(model._meta.db_table))

class BackgroundWriter(object):
    """Starts a background thread to write model instances that are
    queued up by the user."""

    def __init__(self):
        """This spawns the actual thread."""
        self.queue = Queue.Queue()
        self.thread = Thread(target=self._loop)
        self.thread.setDaemon(True)
        self.thread.start()

    @staticmethod
    @singleton
    def singleton():
        return BackgroundWriter()

    def save(self, instance):
        """Queues up `instance' to be saved in the background thread."""
        self.queue.put(instance)

    def join(self):
        self.queue.put(None)
        self.thread.join()

    def _loop(self):
        while True:
            instance = self.queue.get()
            if instance is None:
                return

            instance.save()
            self.queue.task_done()

def bgwrite(instance):
    """Save the given Django model instance in a background thread."""
    BackgroundWriter.singleton().save(instance)


def nestable_commit_on_success(func):
    """ Similar to commit_on_success decorator, except that it uses savepoints
    on the existing transaction rather than appending to it. On failure, the
    outer transaction is not rolled back, only the inner actions.  On success
    the inner changes will be commited when the outer transaction commits.
    If there is no outer transaction, this will create one. """
    def _nestable_commit_on_success(*args, **kw):
        was_managed = dtransaction.is_managed()
        try:
            if was_managed:
                sid = dtransaction.savepoint()
            else:
                dtransaction.enter_transaction_management()
                dtransaction.managed(True)
            try:
                res = func(*args, **kw)
            except:
                # All exceptions must be handled here (even string ones).
                if was_managed:
                    dtransaction.savepoint_rollback(sid)
                elif dtransaction.is_dirty():
                    dtransaction.rollback()
                raise
            else:
                if not was_managed and dtransaction.is_dirty():
                    dtransaction.commit()
            return res
        finally:
            if not was_managed:
                dtransaction.leave_transaction_management()
    return wraps(func)(_nestable_commit_on_success)


def nestable_rollback_on_exit(func):
    """ Similar to nestable_commit_on_success, except that it always rolls back.
    Useful for wrapping unit tests you never want to commit. """
    def _nestable_rollback_on_exit(*args, **kw):
        was_managed = dtransaction.is_managed()
        try:
            if was_managed:
                sid = dtransaction.savepoint()
            else:
                dtransaction.enter_transaction_management()
                dtransaction.managed(True)
            try:
                res = func(*args, **kw)
            finally:
                # All exceptions must be handled here (even string ones).
                if was_managed:
                    dtransaction.savepoint_rollback(sid)
                elif dtransaction.is_dirty():
                    dtransaction.rollback()
            return res
        finally:
            if not was_managed:
                dtransaction.leave_transaction_management()
    return wraps(func)(_nestable_rollback_on_exit)
