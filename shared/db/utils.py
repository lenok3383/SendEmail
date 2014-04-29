"""Database utils module.

#Retry decorator usage:

@shared.db.utils.retry
def delete_old_session_messages():
    query = '''
    DELETE FROM tic_session_messages
    WHERE start_timestamp < %s
    '''
    secs = config.Config()['corpus.classify_timeout_secs']
    cutoff_timestamp = time.time() - secs
    with shared.db.dbcp.get_rw_pool('corpus').transaction() as cursor:
        cursor.execute(query, (cutoff_timestamp,))

:Status: $Id: //prod/main/_is/shared/python/db/utils.py#12 $
:Authors: ohmelevs
"""

import logging
import time

from shared.db import errors
from shared.util.decoratorutils import allow_no_args_form


@allow_no_args_form
def retry(retry_count=None, retry_connection_count=3):
    """Returns a retry decorator.

    :Parameters:
    - retry_count: Number to retry Cursor errors.
                   0 - no retries, None - infinite retries.
                   Default: None.
    - retry_connection_count: Number to retry DBConnection errors.
                              0 - no retries, None - infinite retries.
                              Default: 3.

    :Returns:
    Original output of a decorated function.
    """
    return _RetryDecorator(retry_count, retry_connection_count)


RETRY_TIMEOUT_CAP = 600
STATIC_RETRY_TIMEOUT = 0.1

class _RetryDecorator(object):
    """Retry decorator class."""

    def __init__(self, retry_count=None, retry_conn_count=3):
        self._retry_count = retry_count
        self._retry_conn_count = retry_conn_count
        self._log = logging.getLogger('shared.db')

    def __call__(self, func):
        def _wrapper(*args, **kwargs):
            """Decorated original function with the retry logic."""
            conn_retries = 0
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except errors.DBConnectionError as error:
                    if not self._retry_more(self._retry_conn_count,
                                             conn_retries):
                        raise
                    self._sleep(conn_retries)
                    conn_retries += 1
                    self._log.warning('Error during connection to the'\
                                      ' database: %s', error)
                except errors.DBRetryError as error:
                    if not self._retry_more(self._retry_count, retries):
                        raise
                    time.sleep(STATIC_RETRY_TIMEOUT)
                    retries += 1
                    self._log.warning('Error during query execution: %s', error)
        return _wrapper

    def _retry_more(self, count_limit, current):
        """Checks if given counter reaches count limit."""
        if count_limit is None:
            return True
        else:
            return current < count_limit

    def _sleep(self, retry_count):
        """Sleeps 3**retry_count seconds."""
        timeout = 3 ** retry_count
        if timeout > RETRY_TIMEOUT_CAP:
            timeout = RETRY_TIMEOUT_CAP
        time.sleep(timeout)


