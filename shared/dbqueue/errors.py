"""DB MulticastQueue exception errors.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/errors.py#3 $
:Author: ted, brianz, vburenin
"""

class OutOfRangeError(Exception):
    """Pointer is out of range"""
    pass


class EmptyTableError(Exception):
    """Data table is empty."""
    pass


class InvalidQueueStatusError(Exception):
    """Status data is inaccessible"""
    pass


class NoSuchTableError(Exception):
    """Generated if there is no data table with specific name"""
    pass
