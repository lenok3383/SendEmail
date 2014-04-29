"""DB MulticastQueue static functions.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/utils.py#3 $
:Author: vburenin
"""

import time

def get_table_num(table_name):
    """Returns table number."""
    return int(table_name.rsplit('_', 1)[-1])


def mtime_to_ts(mtime):
    """Converts MySQL time format to unix timestamp."""
    return time.mktime(mtime.timetuple())


