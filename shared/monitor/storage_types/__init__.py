"""This directory should contain counter_storage subclasses.  These are being
separated because a project might not want to have a package dependancy
(e.g. MySQL) that is never used.

:Status: $Id $
:Authors: bwhitela
"""

class StorageError(Exception):
    pass
