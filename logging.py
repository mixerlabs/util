"""Common logging setup for mixer code."""
from __future__ import absolute_import

import os
import sys
import types
import logging
import inspect
from datetime import datetime
import os.path

from environment import env

_configured = False

def mixlog(obj=None):
    try:
        if obj is None:
            name = inspect.getmodulename(
                inspect.stack()[1][0].f_code.co_filename
            )
        else:
            name = inspect.getmodule(obj).__name__
    except:
        # Yeah, bad, I know. But we have to be really careful here.
        name = '<unknown>'

    return logging.getLogger(name)

def configure(name=None,
              level=logging.INFO,
              relative_timestamps=False,
              log_to_file=True,
              log_to_console=False):
    """Configure logging for a mixer application:

      - `name' is the name of the ``application'', and informs the
      default logfile name.
      - `level' specifies the minimum log-level that is logged
      - `relative_timestamps' emits timestamps relative to the time of
      the call to ``configure()'' (typically at program invocation) instead
      of absolute time.
      - `log_to_file' turns on file logging. If a string is passed in,
      this is used as the path to log to.
      - `log_to_console' will print log messages to the console"""
    global _configured
    if _configured:
        print >>sys.stderr, \
            'Logging already configured! Ignoring reconfiguration.'
        return
    _configured = True

    if relative_timestamps:
        timefmt = '%(relativeCreated)d'
    else:
        timefmt = '%(asctime)s'

    formatter = logging.Formatter(
        timefmt + ' %(levelname)s %(name)s] %(message)s',
        '%Y%m%d-%H%M'
    )

    if log_to_file:
        if isinstance(log_to_file, types.StringTypes):
            path = log_to_file
        else:
            # Date today.

            if name is None:
                name = sys.argv[0]

            basename = '%s-%s_%d' % (
                name,
                datetime.now().strftime('%Y-%m-%d'),
                os.getpid()
            )
            path = os.path.join(env.log_directory, basename)

        f = logging.FileHandler(filename=path)
        f.setLevel(level)
        f.setFormatter(formatter)

        logging.root.addHandler(f)

    if log_to_console:
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(formatter)
        logging.root.addHandler(console)

        logging.root.setLevel(level)
