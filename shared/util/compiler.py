#!/usr/bin/env python
"""Utility class to compile all of the code for a given product (as a
sanity check before a release).

:Status: $Id: //prod/main/_is/shared/python/util/compiler.py#5 $
:Authors: jwescott
"""

import compileall
import os
import sys

if __name__ == '__main__':
    # PRODROOT must be passed as an argument
    if len(sys.argv) < 2:
        print "Pass $PRODROOT as first argument."
        sys.exit(-1)

    prodroot = sys.argv[1]
    srcpath = os.path.join(prodroot, 'src', 'python')
    sys.path.append(srcpath)
    succeeded = compileall.compile_dir(dir=srcpath,
                                       maxlevels=99,
                                       ddir=srcpath,
                                       force=True,
                                       quiet=False)
    if succeeded:
        sys.exit(0)
    else:
        print '*** Errors occurred. Check the output above for details.'
        sys.exit(-1)

# EOF
