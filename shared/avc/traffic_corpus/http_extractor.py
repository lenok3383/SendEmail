"""
:Status: $Id: //prod/main/_is/shared/python/avc/traffic_corpus/http_extractor.py#2 $
:Author: $Author: kleshche $

The http extractor module extracts HTTPRequest/HTTPResponse Object pairs from
a TCPConnection object.
"""

import logging
import re

from shared.avc import traffic_corpus
from shared.avc.traffic_corpus.constants import *

HEADERS_RE = re.compile(r'(\r\n\r\n)|(\n\n)')
# added the .*? to the start to account for cruft which can occur at the start
# of a packet capture
REQUEST_RE = re.compile(r'.*?(?P<method>\w+)\s+(?P<path>\S+)\s+HTTP/(?P<version>\S+)$')
# the reason phrase is optional
RESPONSE_RE = re.compile(r'.*?HTTP/(?P<version>\S+)\s+(?P<code>\d+)\s*(?P<reason>.+)?$')

HTTP_RESPONSE = 0
HTTP_REQUEST = 1

# HTTP header names that can be present in a transaction only one time
# (according to RFC 2616)
SINGLE_HEADERS = ['age', 'content-length', 'content-location', 'content-md5',
                  'content-range', 'content-type', 'date', 'etag', 'expires',
                  'from', 'host', 'if-modified-since', 'if-range',
                  'if-unmodified-since', 'last-modified', 'location',
                  'max-forwards', 'referer', 'retry-after', 'server',
                  'user-agent',]

_LOG = logging.getLogger(__file__)


class InvalidHttpTransactionException(Exception):
    """Http transaction is invalid."""
    pass


class EmptyHttpTransactionException(Exception):
    """Http transaction is empty."""
    pass


class UnsupportedHttpMethodException(Exception):
    """Http method is not supported or unknown."""
    pass


def _parse_first_line(first_line):
    """Parse the first line of HTTP. There are two different formats (request
    response).

    :Parameters:
        - `first_line`: string of the first line ('HTTP/1.1 200 OK')

    :Return:
        - `first_line_dict`: dictionary of first line parts.

    :Exceptions:
        - `InvalidHttpTransactionException`: when http transaction is invalid
    """
    first_line = first_line.strip()

    if first_line.startswith('HTTP'):
        res_match = RESPONSE_RE.match(first_line)
        if not res_match:
            raise InvalidHttpTransactionException(
                'Could not parse response: %s' % (first_line))
        http_version = res_match.group(VER_KW)
        code = res_match.group(CODE_KW)
        reason = res_match.group(REASON_KW)
        first_line_dict = {TRANSACT_TYPE_KW: HTTP_RESPONSE,
                           HTTP_VER_KW: http_version,
                           CODE_KW: code,
                           REASON_KW: reason}
    else:
        req_match = REQUEST_RE.match(first_line)
        if not req_match:
            raise InvalidHttpTransactionException(
                'Could not parse request: %r' % (first_line,))
        http_version = req_match.group(VER_KW)
        method = req_match.group(METH_KW)
        path = req_match.group(PATH_KW)
        first_line_dict = {TRANSACT_TYPE_KW: HTTP_REQUEST,
                           HTTP_VER_KW: http_version,
                           METH_KW: method,
                           PATH_KW: path}
    return first_line_dict


def _parse_headers(raw_headers):
    """Parse HTTP headers into a dictionary with each header field a key.

    :Parameters:
        - `raw_headers`: string of complete HTTP headers

    :Return:
        - `first_line_dict`: dictionary of first line parts.
        - `headers_dict`: dictionary of headers with each header field as key

    :Exceptions:
        - `InvalidHttpTransactionException`: when http transaction is invalid
    """
    lines = raw_headers.split('\r\n')
    first_line_dict = _parse_first_line(lines[0])
    raw_header_lines = lines[1:]

    # Missing headers, raising exception.
    if len(raw_header_lines) == 0:
        raise InvalidHttpTransactionException(
            'Could not split transaction headers')

    headers_dict = {}
    header_lines = []
    for line in raw_header_lines:
        if line.startswith(' '):
            header_lines[-1] += line.strip()
            continue
        line = line.strip()
        if not line:
            break
        header_lines.append(line)

    for header_line in header_lines:
        header_parts = header_line.split(':', 1)
        if len(header_parts) == 1:
            raise InvalidHttpTransactionException(
                'HTTP Header without a colon is invalid')
        header_name = header_parts[0].strip().lower()
        if not header_name in headers_dict:
            headers_dict[header_name] = []
        elif header_name in SINGLE_HEADERS:
            # More than one instances of the header are found.
            raise InvalidHttpTransactionException(
                'More than one "%s" header detected in HTTP transaction' % \
                header_name)

        headers_dict[header_name].append(header_parts[1].strip())

    return first_line_dict, headers_dict


def _use_content_length(headers, raw_content):
    """Use Content-length header to determine the end of an HTTP transaction,
    and separate out the content. The content dictionary returned will only have
    values for the 'raw' key.

    :Parameters:
        - `headers`: dictionary representing the HTTP headers
        - `raw_content`: stream of bytes which contain the HTTP transactions

    :Return:
        - `content`: dictionary representing HTTP content (raw only)

    :Exceptions:
        - `InvalidHttpTransactionException`: when http transaction is invalid
    """
    content = {RAW_KW: None, RNDRD_KW: None}
    if len(raw_content) < int(headers[CONTENT_LEN_KW][0]):
        raise InvalidHttpTransactionException(
            'Content-length header [%s] does not match '\
            'actual content length [%s]' % (
            headers[CONTENT_LEN_KW][0], len(raw_content)))
    content[RAW_KW] = raw_content[:int(headers[CONTENT_LEN_KW][0])]
    return content


def _use_chunked_encoding(raw_content):
    """Content is chunked, so the Content-length header cannot be used. Use the
    chunked encoding scheme to determine the end of the transaction. The content
    dictionary returned will contain both 'raw' and 'rendered' values. Raw is
    the content still chunked, while rendered is the content assembled.

    :Parameters:
        - `raw_content`: stream of bytes which contain the HTTP transactions

    :Return:
        - `content`: dictionary representing HTTP content
    """
    def _get_chunk_size(data, data_length, position):
        """Returns the size of the http transaction chunk.

        :Parameters:
            - `data`: raw data whose chunk size needs to be calculated
            - `data_length`: total length of data
            - `position`: position to start from

        :Return:
            - `size`: size of the chunk
            - `position`: current position in the data

        :Exceptions:
            - `InvalidHttpTransactionException`: when http transaction is invalid
        """
        if data_length < position:
            raise InvalidHttpTransactionException(
                'Missing HTTP content found while decoding chunks')
        while data[position:position + 2] == '\r\n':
            position += 2
        i = data.find('\r\n', position, position + 8)
        if i == -1:
            raise InvalidHttpTransactionException(
                'Expected chunk size, but got: %r.' % (
                data[position:position + 8],))
        try:
            size = int(data[position:i], 16) # size is base 16
        except ValueError:
            raise InvalidHttpTransactionException(
                'Expected chunk size, but got: %r.' % (
                data[position:i],))
        position = i + 2 # advance past \r\n
        return size, position

    content = {RAW_KW: None, RNDRD_KW: None}
    rendered_content = []
    raw_content_length = len(raw_content)
    size, current_pos = _get_chunk_size(raw_content, raw_content_length, 0)
    # Stop when chunk size becomes 0.
    while size:
        rendered_content.append(raw_content[current_pos:current_pos + size])
        size, current_pos = _get_chunk_size(
            raw_content, raw_content_length, current_pos + size)

    if rendered_content:
        content[RNDRD_KW] = ''.join(rendered_content)
    content[RAW_KW] = raw_content
    return content


def _get_response_content(headers, raw_content):
    """Parse out response content from payload. The content dictionary returned
    will contain 'raw' and may contain 'rendered' values.

    :Parameters:
        - `headers`: dictionary representing the HTTP headers
        - `raw_content`: stream of bytes which contain the HTTP transactions

    :Return:
        - `content`: dictionary representing HTTP content
    """
    # if method may send content, there are three ways content length may be
    # determined
    if CONTENT_LEN_KW in headers:
        content = _use_content_length(headers, raw_content)
    elif TRNSFR_ENCDNG_KW in headers and \
         CHUNKD_KW in headers[TRNSFR_ENCDNG_KW]:
        content = _use_chunked_encoding(raw_content)
    else:
        content = {RAW_KW: None, RNDRD_KW: None}
        content[RAW_KW] = raw_content
    return content


def _get_request_content(first_line, headers, raw_content):
    """Parse out request content from payload. The content dictionary returned
    will contain 'raw' and may contain 'rendered' values.

    :Parameters:
        - `first_line`: dictionary representing the first line of the HTTP
                transaction
        - `headers`: dictionary representing the HTTP headers
        - `raw_content`: stream of bytes which contain the HTTP transactions

    :Return:
        - `content`: dictionary representing HTTP content

    :Exceptions:
        - `UnsupportedHttpMethodException`: when HTTP method is unkonwn or
                unsupported.
    """
    # if method may send content, there are three ways content length may be
    # determined
    if first_line[METH_KW].upper() in REQ_METH_DICT:
        if CONTENT_LEN_KW in headers:
            content = _use_content_length(headers, raw_content)
        elif TRNSFR_ENCDNG_KW in headers and \
                CHUNKD_KW in headers[TRNSFR_ENCDNG_KW]:
            content = _use_chunked_encoding(raw_content)
        else:
            content = {RAW_KW: None, RNDRD_KW: None}
            content[RAW_KW] = raw_content
    elif first_line[METH_KW].upper() in HTTP_METH_DICT:
        content = {RAW_KW: None, RNDRD_KW: None}
    else:
        raise UnsupportedHttpMethodException(
            'Unknown or unsupported HTTP method: %s' %
            (first_line[METH_KW].upper()))
    return content


def _parse_connection_transactions(tcp_transactions):
    """Create HTTPRequest/HTTPResponse tuples from TCPTransactions.

    :Parameters:
        - `tcp_transactions`: list of TCPTransaction objects

    :Return:
        - `request_responses`: a list of HTTP request/HTTP response pairs (tuples)

    :Exceptions:
        - `InvalidHttpTransactionException`: when http transaction is invalid
        - `EmptyHttpTransactionException`: when http transaction is empty.
    """
    request_responses = []
    last_request = None
    for tcp_transaction in tcp_transactions:
        try:
            hdrs_match = HEADERS_RE.search(tcp_transaction.transaction_data, 1)
            if not hdrs_match:
                raise InvalidHttpTransactionException(
                    'Could not separate headers from HTTP contents')

            raw_headers = tcp_transaction.transaction_data[:hdrs_match.start()]
            raw_content = tcp_transaction.transaction_data[hdrs_match.end():]

            if not raw_headers.strip():
                raise EmptyHttpTransactionException('No headers to parse')

            content = {RAW_KW: None, RNDRD_KW: None}
            first_line, headers = _parse_headers(raw_headers)
            if first_line[TRANSACT_TYPE_KW] == HTTP_REQUEST:
                content = _get_request_content(
                    first_line, headers, raw_content)
                last_request = traffic_corpus.HTTPRequest(
                    first_line[HTTP_VER_KW], first_line[METH_KW],
                    first_line[PATH_KW], headers, raw_headers,
                    content[RAW_KW], content[RNDRD_KW])
            else:
                content = _get_response_content(headers, raw_content)
                response = traffic_corpus.HTTPResponse(
                    first_line[HTTP_VER_KW], first_line[CODE_KW],
                    first_line[REASON_KW], headers, raw_headers,
                    content[RAW_KW], content[RNDRD_KW])
                if last_request is not None:
                    request_responses.append((last_request, response))
                else:
                    _LOG.debug('Response encountered without a matching request')
                    request_responses.append((last_request, response))
                last_request = None
        except EmptyHttpTransactionException as e:
            _LOG.info(e)
    # in the case that we only get a single request
    if last_request is not None:
        request_responses.append((last_request, None))
    return request_responses


def get_http_transactions(tcp_connection):
    """Gets HTTP request/response transactions in a tcp connection.

    :Parameters:
        - `tcp_connection`: tcp connection object from which to get transactions.

    :Return:
        - a list of HTTP request/HTTP response pairs (tuples)
    """
    return _parse_connection_transactions(tcp_connection.transactions)

