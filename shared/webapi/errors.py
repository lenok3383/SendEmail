"""Module with exception classes for Web API server.

:Status: $Id: //prod/main/_is/shared/python/webapi/errors.py#14 $
:Authors: vburenin, rbodnarc
"""

from shared.auth import (AuthError,
                         InsufficientPrivilegesError,
                         LoginFailureError)

class WebAPIException(Exception):
    """WebAPI server exception"""


class NotFoundError(WebAPIException):
    """Resource not found"""


class DuplicationError(WebAPIException):
    """Duplicate resource"""


class InvalidData(WebAPIException):
    """Invalid data"""


class WebAPIInsufficientPrivilegesError(AuthError):
    """Redefine the class from auth"""

    def __init__(self, needed, has):
        """Create a new InsufficientPrivilegesError instance.

        :param needed: Needed privileges.
        :param has: The privileges a user actually has.
        """
        self.needed = needed
        self.has = has
        AuthError.__init__(self)

    def __str__(self):
        msg = 'Authorization Error: You need (%s), but you have ' % (
              ', '.join(self.needed))
        if self.has:
            msg += 'only (%s).' % (', '.join(self.has))
        else:
            msg += 'nothing.'
        return msg


class InconsistencyMappingError(Exception):
    """Inconsistency resource mapping"""
