"""Utility for dealing with writing simple tools that have a number of
commands. Passes in arguments to any method suffixed with '_cmd' in
the module specified.

    import simpletool

    def foo_cmd(args):
        '''<bar> <baz>'''
        if len(args) != 2:
            raise simpletool.BadUsage()

        print args[0], args[1]

    def main(argv):
        simpletool.tool(sys.modules[__name__], argv)

We also provide a tool wrapper:

    TOOL = simpletool.SimpleTool(sys.modules[__name__], 'tool', 'description')"""

import sys

from util import *

BadUsage = sym.bad_usage.exc

def tool(module, argv):
    cmds = dict([(key[:-4], fun) for key, fun in module.__dict__.iteritems()
                 if key.endswith('_cmd')])

    def usage():
        print >>sys.stderr, 'usage: %s <command>' % argv[0]
        for c, fun in cmds.iteritems():
            print >>sys.stderr, '  %s %s' % (c, fun.__doc__)
        sys.exit(1)

    if len(argv) < 2:
        usage()

    cmd = argv[1]
    if not cmd in cmds:
        usage()
    else:
        try:
            cmds[cmd](argv[2:])
        except BadUsage:
            usage()
