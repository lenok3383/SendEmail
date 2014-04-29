"""HTTPConnection retry and reader utility functions for Feeds Web API client
module.

:Status: $Id: //prod/main/_is/shared/python/feeds/utils.py#2 $
:Authors: migoldsb
"""

import cStringIO
import httplib
import os
import socket
import tempfile
import time
from contextlib import closing
from hashlib import md5

from shared.util.decoratorutils import allow_no_args_form

CHUNK_SIZE = 1024 * 16
READ_LIMIT = 1024 * 1024


@allow_no_args_form
def http_retry(max_retries=3):
    return _HTTPRetryDecorator(max_retries)


RETRY_TIMEOUT_CAP = 60

class _HTTPRetryDecorator(object):
    """HTTP Retry decorator class."""

    def __init__(self, max_retries=3):
        self._max_retries = max_retries

    def __call__(self, func):
        def _wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (httplib.HTTPException, InvalidMD5Error,
                        socket.timeout):
                    if not self._retry_more(self._max_retries, retries):
                        raise
                    self._sleep(retries)
                    retries += 1
        return _wrapper

    def _retry_more(self, limit, current):
        return limit is None or current < limit

    def _sleep(self, retry_count):
        timeout = min(3 ** retry_count, RETRY_TIMEOUT_CAP)
        time.sleep(timeout)


class InvalidMD5Error(Exception):
    """Raised when a file's MD5 doesn't check out."""
    def __init__(self, file_md5, check_md5):
        self.args = ['Calculated MD5 %s does not match indicated MD5 %s'
                     % (file_md5, check_md5)]


@http_retry
def http_request(netloc, path, method='GET', req_body='', req_headers={},
                 tmp_dir=None, timeout=None):
    """Send a single request to a web server and get the status, headers and
    body of the response.  Will retry the request up to three times (by
    default) if an HTTP error occurs.  Note that the returned body IS NOT a
    permanent file; assume it will disappear forever when it is closed.

    :param netloc: HTTP server host[:port].
    :param path: Requested path (e.g. '/foo/bar/baz.html').
    :param method: Request method (GET, HEAD, PUT, POST, etc).
    :param req_body: Request body, for PUT or POST requests.
    :param req_headers: Request headers dictionary.  No support for duplicate
        headers.
    :param tmp_dir: Where to write temporary files (defaults to '/tmp').
    :param timeout: Timeout (in seconds) for HTTP connection socket.
    :return: Response status code (int), headers (dict, lowercased keys), and
        body as a file-like object (StringIO or TemporaryFile).
    """

    with closing(httplib.HTTPConnection(netloc, timeout=timeout)) as conn:
        conn.request(method, path, req_body, req_headers)
        response = conn.getresponse()
        status = response.status
        resp_headers = response.getheaders()
        resp_headers = dict([(h[0].lower(), h[1]) for h in resp_headers])

        # Read chunks into buffer up to READ_LIMIT at a time, then dump to
        # a temporary file.
        check_md5 = resp_headers.get('content-md5', None)
        resp_md5 = md5()
        read = 0
        buf = cStringIO.StringIO()
        tf = None
        chunk = response.read(CHUNK_SIZE)
        while chunk:
            read += len(chunk)
            if read >= READ_LIMIT:
                if not tf:
                    tf = tempfile.TemporaryFile(dir=tmp_dir)
                    tf.write(buf.getvalue())
                tf.write(chunk)
            else:
                buf.write(chunk)
            if check_md5:
                resp_md5.update(chunk)
            chunk = response.read(CHUNK_SIZE)
        if check_md5:
            resp_md5 = resp_md5.hexdigest()
            if resp_md5 != check_md5:
                raise InvalidMD5Error(resp_md5, check_md5)
        if tf:
            resp_body = tf
        else:
            resp_body = buf
        resp_body.seek(0)

        return status, resp_headers, resp_body

