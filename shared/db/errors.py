"""Database errors module.

:Status: $Id: //prod/main/_is/shared/python/db/errors.py#7 $
:Authors: jwescott, ohmelevs
"""


class DBError(Exception):
    """General database error."""

    def __init__(self, err):
        """
        Constructor.

        :Parameters:
          -`err`: the original exception instance
        """
        super(DBError, self).__init__(*err)
        self.__cause__ = err
        self.query = None
        self.query_args = None

        # Extract query information if it was
        # previously stored in the original exception.
        if hasattr(err, 'query'):
            self.query = err.query
        if hasattr(err, 'query_args'):
            self.query_args = err.query_args

    def __str__(self):
        if self.query is not None:
            return '%s\n'\
                   '\tquery=%s\n'\
                   '\targs=%s\n' % (super(DBError, self).__str__(),
                                    self.query, self.query_args)
        return super(DBError, self).__str__()


class DBConnectionError(DBError):
    """Exception class for when a low-level database connection error
    occurs.  If this error occurs, the underlying database or network
    link is most likely down and should be investigated immediately.
    This is usually a FATAL error.
    """
    pass


class DBRetryError(DBError):
    """Database operational error.  This exception will be retried in the
    retry decorator."""
    pass


class DBConfigurationError(DBError):
    """Database configuration error."""
    pass


class TimeoutError(Exception):
    """Exception class for when a timeout occurs trying to get a
    connection from the pool.
    """
    def __init__(self, timeout):
        Exception.__init__(self, timeout)
        self.timeout = timeout
