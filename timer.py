from __future__ import absolute_import
from __future__ import with_statement

import time

class timer(object):
    """A timer can time events as a context manager or decorator.

      eventtime = timer()
      with eventtime:
        a()

      @eventtime
      def b():
          c()

      something_else_expensive()

    ``eventtime.elapsed'' now contains the total amount of elapsed
    time of running `a' and `b'."""

    def __init__(self):
        self.elapsed = 0

    def __enter__(self):
        self.begin = time.time()

    def __exit__(self, *_):
        self.elapsed += 1000 * (time.time() - self.begin)

    def __call__(self, fun):
        def wrapper(*args, **kwargs):
            with self:
                return fun(*args, **kwargs)

        return wrapper

    def __str__(self):
        return '%dms' % self.elapsed

    __repr__ = __str__
