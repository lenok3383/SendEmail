"""Domain utility functions.

:Status: $Id: //prod/main/_is/shared/python/net/domainutils.py#6 $
:Author: ed, vscherb
"""

import re
import struct
import urlparse

from hashlib import md5
from shared.net import iputils
from shared.net import registrar
from urllib import quote, unquote, quote_plus, unquote_plus, url2pathname

def reverse_domain(dom_str):
    """Convert domain to reverse domain.

    E.g.: 'www.yahoo.com' -> 'com.yahoo.www', etc.

    :Parameters:
    - dom_str: String with domain in dotted-quad format.

    :Returns: String with reversed domain in dotted-quad format.
    """
    l = dom_str.split('.')
    l.reverse()
    return '.'.join(l)


def ascii_encode(plain_str):
    """
    Converts the given (possibly unicode) string to contain only ascii chars.

    Unicode strings are encoded with UTF-8, then output byte-wise using %xx
    for any bytes not representable with ascii chars.
    Non-unicode strings are output using %xx for non-ascii chars.
    Note: Unprintable ascii chars (including the NULL char '\x00') are left
    as their original values (the bytes remain unconverted).

    Calling this function multiple times on its own output does not change
    the output. That is, ascii_encode(ascii_encode(s)) == ascii_encode(s).

    :Parameters:
    - plain_str: String with plain string.

    :Returns: String that contain only ascii chars.

    :Raises: ValueError if unknown type of the plain string.
    """

    # Encode the string into a byte-string
    if isinstance(plain_str, unicode):
        plain_str = plain_str.encode('utf8')
    elif isinstance(plain_str, str):
        pass
    else:
        type_str = str(type(plain_str))
        raise ValueError('Unknown type of string: %s' % (type_str,))
    # Decode the byte-string as ascii, convert non-ascii bytes
    s = []
    # Encode non-ascii chars.
    for x in plain_str:
        if ord(x) > 0x7f:
            s.append('%' + hex(ord(x))[2:])
        else:
            s.append(x)
    return ''.join(s)


class InvalidDomainError(ValueError):
    """Exception class for domain string errors."""
    pass


class InvalidProtocolError(ValueError):
    """Exception class for protocol string errors."""
    pass


def _interpret_domain_as_ip(domain):
    """Try to interpret the domain string as ip.

    :Parameters:
    - ip_like_str: String with IP.

    :Returns: String with IP in canonical hexadecimal digits form.

    :Raises:
    - InvalidIPv6StrError if invalid IPv6 string
    - InvalidIpStrError if invalid IP string
    """
    like_ipv6 = False
    ip_like_str = domain
    # Remove brackets in case of IPv6 domain string, e.g.:
    #   http://[::ffff:192.0.2.128]/
    if ip_like_str.startswith('[') and ip_like_str.endswith(']'):
        ip_like_str = ip_like_str[1:-1]
        like_ipv6 = True
    try:
        return iputils.canonify_ip_str(ip_like_str)
    except iputils.InvalidIPv6StrError, e:
        raise InvalidDomainError(e)
    except iputils.InvalidIpStrError:
        if like_ipv6:
            raise InvalidDomainError('Invalid IPv6 string: %s' % (domain,))
        raise

def split_reversed_dom_str(reversed_dom_str):
    """Split a host/domain string into domain part and subdomain part.

    The purpose is to partition pruning and update
    into proper working unit. Empty string ('') will be used for
    missing parts of the tuple.

    E.g.: '1.2.3.4' ==> ('1.2.3.4', '')
          'com.foo' ==> ('com.foo', '')
          'com.foo.www' ==> ('com.foo', 'www')
          'cn.tianya.www' ==> ('cn.tianya', 'www')
          'cn.net.cnnic' ==> ('cn.net.cnnic', '')

    :Parameters:
    - reversed_dom_str: String with the host/domain.

    :Returns: A tuple of format (str, str).

    :Raises: InvalidDomainError if a domain/host has less than two syllables.
    """
    try:
        # Try to interpret the domain string as ip first
        return (_interpret_domain_as_ip(reversed_dom_str), '')
    except iputils.InvalidIpStrError:
        registrar_obj = registrar.get_registrar()
        subdomain, domain = registrar_obj.split_domain(reverse_domain(reversed_dom_str))
        if domain.find('.') < 0:
            raise InvalidDomainError('Domain should have at least two syllables: %s' % (reversed_dom_str,))
        return (reverse_domain(domain), reverse_domain(subdomain))


def split_normal_dom_str(normal_domain_str, reverse=True):
    """Split a host/domain string into domain part and  subdomain part.

    Same as split_reversed_dom_str, the input string is the domain string in the
    normal order.

    :Parameters:
    - normal_domain_str: String with the host/domain.
    - reverse: If True - reverse domain and subdomain.

    :Returns: A tuple of format (str, str).

    :Raises: InvalidIPv6StrError if domain id an invalid IPv6 string.
             InvalidDomainError if a domain/host has less than two syllables.
    """
    try:
        # Try to interpret the domain string as ip first
        return (_interpret_domain_as_ip(normal_domain_str), '')
    except iputils.InvalidIpStrError:
        registrar_obj = registrar.get_registrar()
        subdomain, domain = registrar_obj.split_domain(normal_domain_str)
        if domain.find('.') < 0:
            raise InvalidDomainError, 'Domain should have at least two syllables: %s' % normal_domain_str
        if reverse:
            return (reverse_domain(domain), reverse_domain(subdomain))
        else:
            return (domain, subdomain)


RE_ESCAPED_CHARS = re.compile('%[0-9a-fA-F]{2}')
RE_NO_SLASHES = re.compile('^(https?:)/{0,2}')
RE_URL_PROTOCOL = re.compile('^[-_a-zA-Z0-9]+://')
RE_SLASH_BEFORE_PARAMS = re.compile('^(https?://[^/?]+)\?(.*)')


def split_url(url,
        return_extra_parts=False,
        unquote_url=True,
        hash_path=False,
        lower_path=True,
        remove_tailing_slash=True,
        reverse=True,
        strict_port=True,
        add_missing_schema=True,
        encode_non_ascii=True):
    """Take a URL and split it into all of its parts (as strings).

    Empty string ('') will be used for missing parts of the tuple.

    :Parameters:
    - url: String with URL.
    - return_extra_parts: If True - return extra parts of URL:
                          query, fragment, user_passwd.
    - unquote_url: If True - unquote %xx.
    - hash_path: If True - convert hash to path.
    - lower_path: If True - lower case path part.
    - remove_tailing_slash: If True - remove tailing slash.
    - reverse: If True - reverse domain and subdomain.
    - strict_port: If True - raise ValueError if port is not numerical.
    - add_missing_schema: If True and URL's protocol is not set, then
                          set port's value either to 'http' (default)
                          or 'ftp' if URL starts with 'ftp.'.
    - encode_non_ascii: If True - converts the given (possibly unicode)
                        string to contain only ascii chars.

    :Returns: A tuple of format (domain, subdomain, path, protocol, port) or
              (domain, subdomain, path, protocol, port, query, fragment, user_passwd)
              if return_extra_parts is True;
    """

    # Cannoify the url first

    # Remove empty spaces
    url = url.strip()

    # Unquote %xx in URL (e.g. %20 => ' ')
    if unquote_url and RE_ESCAPED_CHARS.search(url):
        url = unquote_plus(url)

    original_url = url

    # http:www.foo.biz -> http://www.foo.biz
    if not url.startswith('http://') and not url.startswith('https://'):
        url = RE_NO_SLASHES.sub(lambda x: x and (x.groups()[0] + '//'), url)

    # Add schema:
    #     unschemed URIs: assume default of "http://"
    #     www.foo.com/bar -> http://www.foo.com/bar
    #     use ftp:// if the host name starts with ftp.
    #     ftp.foo.biz -> ftp://ftp.foo.biz
    if not RE_URL_PROTOCOL.match(url):
        if url.startswith('ftp.'):
            url = 'ftp://' + url
        else:
            # If looks like url
            url = 'http://' + url

    if encode_non_ascii:
        # But don't use any non-ascii chars! always %xx encode them.
        url = ascii_encode(url)

    # http://www.foo.biz?id=3 -> http://www.foo.biz/?id=3
    match = RE_SLASH_BEFORE_PARAMS.search(url)
    if match:
        url = '%s/?%s' % match.groups()

    # WSA use 'httpc' for special purpose. Unfortunately, python library
    # does not under stand
    if url.startswith('httpc:'):
        url = 'http' + url[5:]
        special_protocol = 'httpc'
    else:
        special_protocol = ''
    (protocol, host, path, query, fragment) = urlparse.urlsplit(url)
    if special_protocol:
        protocol = special_protocol
    if host == '' and path.startswith('//'):
        raise InvalidProtocolError('Invalid protocol: %s' % (protocol,))
    if not add_missing_schema:
        if not original_url.lower().startswith(protocol + ':'):
            protocol = ''

    # Remove username/password@ part
    host_tuple = host.split('@')

    domain_port = host_tuple[-1]
    if len(host_tuple) > 1:
        user_passwd = host_tuple[0]
    else:
        user_passwd = ''

    port = ''

    # Check there are IPv6 ip in domain name
    if ('[' in domain_port or ']' in domain_port):

        if (not domain_port.startswith('[') or
            ('[' in domain_port and ']' not in domain_port) or
            (']' in domain_port and '[' not in domain_port)):
            raise ValueError('Invalid IPv6 URL')

        right_bracket_pos = domain_port.rfind(']')
        last_colon_pos = domain_port.rfind(':')

        # Split host name and port
        domain = domain_port[0:right_bracket_pos+1]
        if last_colon_pos > right_bracket_pos:
            port = domain_port[last_colon_pos+1:]

    else:
        # Split host name and port
        domain_port_tuple = domain_port.split(':')

        if len(domain_port_tuple) > 2:
            raise InvalidDomainError('Too many colons in domain/port '
                                     'grouping: %s', domain_port)

        domain = domain_port_tuple[0]
        if len(domain_port_tuple) > 1:
            port = domain_port_tuple[-1]

    if port and (not port.isdigit()) and strict_port:
          raise ValueError('Port must be int: %s' % (port,))

    # Split domain into domain and sub-domain
    (domain, subdomain) = split_normal_dom_str(domain.lower(),
                                               reverse)
    if not domain:
        raise ValueError('Invalid URL: %s' % (url,))

    # Lower case path part
    if lower_path:
        path = path.lower()

    # Remove tailing slash
    if remove_tailing_slash:
        while path.endswith('/'):
            path = path[:-1]

    # Convert hash to path
    if hash_path:
        path = convert_path_to_hash(path)

    if return_extra_parts:
        return (domain, subdomain, path, protocol, port, query, fragment, user_passwd)
    else:
        return (domain, subdomain, path, protocol, port)


def unsplit_url(domain, subdomain, path, protocol, port, query, fragment, user_passwd, reverse=False):
    """Join 8 elements (can be tuple from split_url back) to an URL.

    Missed part can be replaced by NULL value, e.g. '', False, None.

    :Parameters:
    - domain: String with the domain.
    - subdomain: String with the subdomain.
    - path: String with the path string.
    - protocol: String with the protocol.
    - port: URL's port value.
    - query: String with the URL's query.
    - fragment: String with the URL's fragment.
    - user_passwd: String with the username and password.
    - reverse: If True - reverse domain and subdomain.

    :Returns: String with the unsplited URL.
    """
    if iputils.is_valid_ipv6(domain):
        # Add square brackets when unsplit URL with IPv6 as domain
        domain = '[%s]' % domain
    if reverse:
        (domain, subdomain) = (reverse_domain(domain), reverse_domain(subdomain))
    host = domain
    if subdomain:
        host = '%s.%s' % (subdomain, domain)
    if user_passwd:
        host = '%s@%s' % (user_passwd, host)
    if port:
        host = '%s:%s' % (host, port)
    url = host
    if protocol:
        url = '%s://%s' % (protocol, host)
    if path:
        url = '%s%s' % (url, path)
    if query:
        url = '%s?%s' % (url, query)
    if fragment:
        url = '%s#%s' % (url, fragment)

    return url


def convert_str_to_uint(s):
    """Convert plain string to hashed integer value.

    Performs conversions between Python values and C structs.
    Unpack the string according to the format '>I'.

    :Parameters:
    - s: String with plain string.

    :Returns: Unsigned integer value of plain string hashed value
              (unpacked string).
    """
    return struct.unpack('>I', md5(s).digest()[:4])[0]


def url_to_keys(url):
    """Convert hashed url to dom_hash_key and url_hash_key.

    :Parameters:
    - url: String with the URL.

    :Returns: Tuple in format (dom_hash_key, url_hash_key).
    """
    (domain, subdomain, path, protocol, port) = split_url(url, reverse = False)

    return (convert_str_to_uint(domain), convert_str_to_uint(url))



class PathSegmentFormatError(ValueError):
    """Exception class for path string errors."""
    pass


def calc_path_seg_hash(path_segment):
    """Calculate hash value for the path segment.

    :Parameters:
    - path_segment: String with the path segment.

    :Returns: String with the hashed value (first 12 chars).
    """
    return md5(path_segment).hexdigest()[:12]


def convert_path_to_hash(path_str):
    """Convert '/' delimited path to '/' delimited hashes.

    :Parameters:
    - path_str: String with the path.

    :Returns: String with the hashes.

    :Raises: PathSegmentFormatError if the input string is invalid.
    """
    if not path_str:
        return ''

    if not path_str.startswith('/'):
        raise PathSegmentFormatError('Invalid path_str "%s"' % (path_str,))

    segs = path_str.split('/')
    # Keep the first /
    new_segs = [segs[0]]
    length = len(segs)
    for i in range (1, length):
        if segs[i]:
            new_segs.append(calc_path_seg_hash(segs[i]))
        elif i == length -1:
            # Keep the ending /
            new_segs.append(segs[i])

    return '/'.join(new_segs)


def looks_like_ip(domain):
    """Check if `domain` is an IP address v4 or v6.

    Supported IP formats: 192.168.1.1, 213.172.0x1f.13, 0x7f000001, 1113343453,
    2001:67:ac:150::250, [2001:67:ac:150::250].

    :Parameters:
        - `domain`: A domain string to check is it an IP.

    :Return:
        True if it is either a v4 or v6 address, otherwise False.
    """
    try:
        _interpret_domain_as_ip(domain)
        return True
    except ValueError:
        return False
