"""(per-thread) Event logging. These are just (type, data)-tuples
where `type' is a string, `data' is arbitrary, and the reader provides
presentation.

The `Log' class represents a linear event log. Every thread always has
a log, but it may not be active."""
from __future__ import absolute_import
from __future__ import with_statement

import time
import threading
import thread
import tempfile
import pstats
import cProfile

import traceback

from .db import CursorWrapper, connection

class ActiveLog(threading.local):
    """This is a container for the (per-thread) active log. It's used
    by the logging API at the bottom of this file."""
    def __init__(self):
        self._log = None

    def log_fget(self):
        if self._log is not None:
            return self._log
        else:
            return NOLOG

    def log_fset(self, log):
        self._log = log

    log = property(log_fget, log_fset)

class Log(list):
    """A log instance represents a thread of logging, and may be
    activated & deactivated (ie. be the current active log as per
    `_ACTIVE'). This is useful for tracing all log messages related to
    a given context, for example a request, or a user.

    This class also contains the full logging API, which are used by
    the wrappers on the bottom of this file."""
    def __init__(self, cpu_profile=False, log_sql_queries=False):
        self.log_sql_queries = log_sql_queries

        self.cpu_profile_enabled   = cpu_profile
        self.cpu_profile_of_thread = {}
        self.local                 = threading.local()
        self.lock                  = threading.Lock()

        self.restart()

    @property
    def cpu_profile(self):
        if not self.cpu_profile_enabled:
            return None

        ident = thread.get_ident()
        if ident not in self.cpu_profile_of_thread:
            self.cpu_profile_of_thread[ident] = cProfile.Profile()

        return self.cpu_profile_of_thread[ident]

    @property
    def merged_cpu_profile(self):
        """Produces a merged CPU profile (there is one CPU profile per
        thread)."""
        if not len(self.cpu_profile_of_thread):
            # Create an empty profile.
            self.cpu_profile.enable()
            self.cpu_profile.disable()
            len(self.cpu_profile_of_thread)

        files = []
        for profile in self.cpu_profile_of_thread.values():
            file = tempfile.NamedTemporaryFile()
            profile.dump_stats(file.name)
            files.append(file)

        stats = pstats.Stats(files[0].name)
        for file in files[1:]:
            stats.add(file.name)
            file.close()

        return stats

    @property
    def is_active(self):
        """Whether this log is active for the current thread."""
        return _ACTIVE.log is self

    def restart(self):
        """Restart the log. This clears all log entries & resets the
        timer."""
        self[:] = []
        self.begin_time = time.time()

    def time_ms(self):
        """Elapsed time (in milliseconds) from the last ``restart()''
        call."""
        return int(1000 * (time.time() - self.begin_time))

    def timed(self, type, data):
        """Returns a context manager usable for timed log
        entries. Entering the context manager also returns the data
        object (or None if it is not evaluated -- ie. we're not
        logging). Eg.:

          my_log = Log()
          with my_log.timed('some-type', lambda: {'count': '0'}) as D:
            while do_something():
              if D: D['counter'] += 1"""

        L = self
        class timer(object):
            def __init__(self):
                self.is_active = L.is_active

            def __enter__(self):
                if not self.is_active:
                    return

                data_ = data() if callable(data) else data

                self.idx   = L.append(type, data_)
                self.begin = time.time()

                return data_

            def __exit__(self, *_):
                if not self.is_active:
                    return

                e = L[self.idx]
                L[self.idx] = (e[0], L.time_ms(), e[2], e[3])

        return timer()

    # Context manager for keeping the log active.
    def __enter__(self):
        self.activate()

    def __exit__(self, *_):
        self.deactivate()

    def activate(self):
        """Activate this log for the current thread."""
        if self.is_active:
            return

        self.local.old_log = _ACTIVE.log
        _ACTIVE.log = self

        if self.log_sql_queries:
            CursorWrapper.wrap_connection(connection, self._sql_logger)

        if self.cpu_profile:
            self.cpu_profile.enable()

    def deactivate(self):
        """Deactivate this log for the current thread."""
        # It's ok to call deactivate twice. In fact, this should
        # happen when logs are "stolen"
        if not self.is_active:
            return

        if self.cpu_profile:
            self.cpu_profile.disable()

        _ACTIVE.log = self.local.old_log

        if self.log_sql_queries:
            connection.cursor._unwrap()

    def append(self, type, data):
        """Append a log entry of the given type & data. `type' is an
        arbitrary string, meaningful to whatever renders it. We return
        the index at which the entry was inserted."""
        entry = (self.time_ms(), None, type, data() if callable(data) else data)
        with self.lock:
            super(Log, self).append(entry)
            return len(self) - 1

    def append_(self, data):
        """Like `append', but with a none-type."""
        return self.append(None, data)

    def _sql_logger(self, query):
        elapsed = 1000 * query['time']
        begin = self.time_ms() - elapsed

        with self.lock:
            # We insertion-sort here since we're possibly backfilling.
            for i in xrange(len(self) - 1, -1, -1):
                if self[i][0] < begin:
                    break
            else:
                i = 0

            self.insert(i + 1, (
                begin, begin + elapsed, 'sql-query',
                {'query': query['sql']}))

class NoLog(Log):
    """A no-op `Log'. A `NoLog' instance is the active log when there
    aren't any legitimately active logs."""
    def __init__(self, *a, **kw):
        super(NoLog, self).__init__(*a, **kw)

    @property
    def is_active(self):
        return False

    def __enter__(self):
        pass
    def __exit__(self, *_):
        pass
    def activate(self):
        pass
    def deactivate(self):
        pass
    def append(*_):
        return 0

_ACTIVE = ActiveLog()
NOLOG   = NoLog()

# | The logging API. These functions are used to append to the current
# log. `append', `timed' and `is_active' are proxies to the active
# `Log'.

append    = lambda *a: _ACTIVE.log.append(*a)
timed     = lambda *a: _ACTIVE.log.timed(*a)
is_active = lambda *_: _ACTIVE.log.is_active

def steal():
    """``steal'' the active log: Deactivates & returns the current
    log."""
    log = _ACTIVE.log
    log.deactivate()
    return log

def get_active():
    """return the currently active log"""
    return _ACTIVE.log
