"""IP utility functions.

During the porting to Python 2.6 we've made the following changes
which break compatibility with existing IronPort products:

    - ipToInt             renamed to ip_to_signed_int,
    - intToIp             renamed to signed_int_to_ip,
    - ipToUnsignedInt     renamed to ip_to_unsigned_int,
    - unsignedIntToIp     renamed to unsigned_int_to_ip,
    - isIpInRange         renamed to is_ip_in_range,
    - isValidIp           renamed to is_valid_ip,
    - isValidCidr         renamed to is_valid_cidr,
    - isRoutableIp        renamed to is_routable_ip,
    - isIllegalSourceIp   renamed to is_illegal_source_ip,
    - cidrToRange         renamed to cidr_to_range and returns unsigned ints
                          as result (if you still want signed ints, you need
                          to pass 'signed=True' parameter),
    - makeMask            renamed to make_mask.

:Status: $Id: //prod/main/_is/shared/python/net/ip.py#12 $
:Authors: michaelo, aflury, ted, kylev
"""

import re
import socket
import struct
import types
import warnings


warnings.warn(
    "'shared.net.ip' module is deprecated. "
    "Use 'shared.net.iputils' instead.", DeprecationWarning
    )


def ip_to_signed_int(ip):
    """Convert IP address from dotted-quad string to network order int.

    Consider this deprecated, and only use it for working with old DBs
    that store IPs as signed 32-bit integers.

    :param ip: String with ip in dotted-quad format.
    :return: Signed integer value of ip.
    """
    warnings.warn(
        "Storing IPs as signed integers is deprecated.", DeprecationWarning
        )
    return struct.unpack(">i", socket.inet_aton(ip))[0]


def signed_int_to_ip(iip):
    """Convert IP address in 32-bit int to dotted-quad string.

    Consider this deprecated, and only use it for working with old DBs
    that store IPs as signed 32-bit integers.

    :param iip: Signed integer value of ip.
    :return: String with ip in dotted-quad format.
    """
    warnings.warn(
        "Storing IPs as signed integers is deprecated.", DeprecationWarning
        )
    return socket.inet_ntoa(struct.pack('>i', int(iip)))


def ip_to_unsigned_int(ip):
    """Convert IP address from dotted-quad string to *NOT Network* order int.

    :param ip: String with ip in dotted-quad format.
    :return: Long integer value of ip.
    """
    return struct.unpack(">L", socket.inet_aton(ip))[0]


def unsigned_int_to_ip(iip):
    """Convert IP address in 32-bit unsigned int to dotted-quad string.

    :param iip: Unsigned integer value of ip.
    :return: String with ip in dotted-quad format.
    """
    return socket.inet_ntoa(struct.pack('>L', long(iip)))


# the regular expression version is much faster
_NON_ROUTABLE_RE = re.compile(r'^(0\.|10\.|127\.|169\.254\.|'
                               '172\.(1[6-9]|2\d|3[0-1])\.|'
                               '192\.0\.2\.|192\.168\.|198\.(1[8-9])\.|'
                               '(22[4-9]|23\d)\.|255\.)')

# the regular expression version is much faster
_ILLEGAL_SOURCE_RE = re.compile(r'^([0257]\.|14\.|169\.254\.|191\.255\.)')

_VALID_CIDR_RE = re.compile(r'^([0-9]{1,3}\.){0,3}[0-9]{1,3}(/[0-9]{1,2})?$')


def is_ip_in_range(ip, ip_range):
    """Check whether the given IP is in the list of networks or not.

    Networks are either single or "partial" IPs (such as '234' or '153.106').
    CIDR format like '153.106.0.0/16' is not supported yet.

    :param ip: String with ip in dotted-quad format.
    :param ip_range: Iterable with valid ips range.
    :return: True if ip in range, False otherwise.
    """

    for x in range(4):
        if ip in ip_range:
            return True
        ip = '.'.join(ip.split('.')[:-1])
    return False


def is_valid_ip(ip):
    """Determine if the given IP is a valid IP address.

    This function determines if the given IP is a valid IP address in canonical
    dotted-quad form.

    :param ip: String with ip to be checked.
    :return: True if ip is valid, False otherwise.
    """
    try:
        packed = socket.inet_aton(ip)
        unpacked = socket.inet_ntoa(packed)
        if ip == unpacked:
            return True
        else:
            return False
    except (socket.error, UnicodeEncodeError):
        return False


def is_valid_cidr(cidr):
    """Verifies valid CIDR format. Note: Assumes IPv4 32 bit addressing.

    :param cidr: String with cidr to be checked.
    :return: True if cidr is valid, False otherwise.
    """
    if not _VALID_CIDR_RE.match(cidr):
        return False

    try:
        prefix, subnet = cidr.split('/')
        subnet = int(subnet)
    except ValueError:
        prefix = cidr
        subnet = 32

    return is_valid_ip(prefix) and 1 <= subnet <= 32


def is_routable_ip(ip):
    """Determine if the given IP is one which is publicly routable.

    This is based on the list of non-routable networks in RFC 3330
    (which includes RFC 1918).
    Also see http://www.faqs.org/rfcs/rfc3330.html.

    :param ip: String with ip to be checked.
    :return: True if ip is routable, False otherwise.
    """
    return is_valid_ip(ip) and not _NON_ROUTABLE_RE.match(ip)


def is_illegal_source_ip(ip):
    """Check if email can't be sent from this ip.

    :param ip: String with ip to be checked.
    :return: True if ip is illegal, False otherwise.
    """
    return _ILLEGAL_SOURCE_RE.match(ip)


def _unsigned_int_to_signed_int(unsigned):
    """Helper for converting unsigned int to signed int.

    :param unsigned: An unsigned integer value.
    :return: A signed integer value.
    """
    return struct.unpack('i', struct.pack('I', unsigned & 0xFFFFFFFF))[0]


def cidr_to_range(ip, signed=False):
    """Convert a IP number (dotted quad) to a range limited by two ints.

    A result tuple containing a range (unsigned-low, unsigned-high) pair.
    It will accept a string or a tuple/list in a variety of formats.

    :param ip: An IP or subnet as a string, tuple, or list.  Supported
               formats include:
               - '153.106.4.1' - string IP address
               - '153.106' - partial string IP, classful subnet
               - '53.106.*.*' - string subnet with "*" placeholders
               - [153, 106] - list of integers, partial or complete
               - (127, '0', '0', 1) - tuples, partial or complete
               - '153.106.0.0/16' - string CIDR-slash notation
    :param signed: If True - signed ints will be returned, unsigned otherwise.
    :return: A tuple with format (int, int).
    """

    net_bits = 32
    explicit_netbits = None

    if type(ip) in types.StringTypes:
        if ip.find('/') != -1:
            # Looks like '/' CIDR notation
            ip, new_mask = ip.split('/')
            explicit_netbits = int(new_mask)
        ip_parts = ip.split('.')
    elif type(ip) == types.ListType:
        # We're done
        ip_parts = ip[:]
    elif type(ip) == types.TupleType:
        # We need this to be mutable
        ip_parts = list(ip)
    else:
        raise TypeError('I don\'t know how to handle an IP as a %s' %
                        (type(ip),))

    # Drop *'s to make them look like "incomplete" IP's, handled below
    while ip_parts[-1] == '*':
        ip_parts.pop()

    # Do we have an incomplete IP, and thus some classful subnet?
    while len(ip_parts) < 4:
        ip_parts.append(0)
        net_bits -= 8

    if explicit_netbits is not None:
        net_bits = explicit_netbits

    # Unpack to one big signed long
    text_ip = '.'.join([str(x) for x in ip_parts])
    mask = make_mask(net_bits)
    ip_int = ip_to_unsigned_int(text_ip)
    ip_low = ip_int & mask
    ip_high = ip_int | (~mask & 0xFFFFFFFF)
    if signed:
        return (_unsigned_int_to_signed_int(ip_low),
                _unsigned_int_to_signed_int(ip_high))
    else:
        return ip_low, ip_high


def make_mask(bits):
    """Make a mask with netbits suitable for the results of 'ip_to_int'.

    :param bits: An integer number of bits for a mask (0 <= bits <= 32).
    :return: A 32-bit integer mask with the highest *bits* bits set to 1.
    """

    if bits < 0 or bits > 32:
        raise ValueError('Number of bits must be'
                         ' between 0 and 32 (inclusive)')
    return (0xFFFFFFFF << (32 - bits)) & 0xFFFFFFFF


def subtract_cidr_list(source_cidr, cidrs_list):
    """Transform cidr by removing another specified cidrs.

    Assumes all entries are valid.

    :param source_cidr: Cidr to be transformed.
    :param cidrs_list: Cidrs to be removed.
    :return: Trasformed cidr.
    """
    src_start, src_end = cidr_to_range(source_cidr)

    cidr_ranges_list = [cidr_to_range(cidr) for cidr in cidrs_list]
    cidr_ranges_list.sort()

    for cidr_start, cidr_end in cidr_ranges_list:
        if cidr_end < src_start:
            continue
        elif cidr_start > src_end:
            break
        elif cidr_start <= src_start:
            src_start = cidr_end + 1
        else:
            for prefix, subnet in ip_range_to_cidr(src_start, cidr_start - 1):
                yield '%s/%s' % (unsigned_int_to_ip(prefix), subnet)
            src_start = cidr_end + 1
    if src_start <= src_end:
        for prefix, subnet in ip_range_to_cidr(src_start, src_end):
            yield '%s/%s' % (unsigned_int_to_ip(prefix), subnet)


def ip_range_to_cidr(start, end):
    """Convert an ip range to a cidr.

    Note: Assumes IPv4 32 bit addressing (Jay Chan's magic).

    :param start: An unigned int as starting ip of a range.
    :param end: An unsigned int as ending ip of a range.
    :return: Cidr in format (prefix, subnet).
    """
    stack = list()
    while True:
        h = 1
        p = 0
        # Find the different bits
        x = start ^ end
        while x:
            # Find the largest CIDR that envelopes this range.
            x >>= 1
            h <<= 1
            p += 1
        if end - start + 1 == h:  # this CIDR is complete
            yield (start, 32 - p)
            if stack:
                # there's more to do
                start, end = stack.pop()
            else:
                break
        else:
            # Recursively do ip_range_to_cidr to the two ranges
            # divided by the largest CIDR boundary
            d = start | ((h >> 1) - 1)
            stack.append((d + 1, end))
            end = d


def star_to_cidr(ip):
    """Converts ip with wildcards to cidr.

    E.g.: a.b.c.* -> a.b.c.0/24, a.b.* -> a.b.0.0/16, etc.

    :param ip: String with ip to be converted.
    :return: String with cidr.
    """
    ip = ip.strip()
    if not ip.endswith('*'):
        return '%s/36' % (ip,)

    octets = 0
    parts = ip.split('.')
    new_parts = []
    for part in parts:
        if part.isdigit():
            new_parts.append(part)
            octets += 1
        elif part == '*':
            for i in xrange(4 - octets):
                new_parts.append('0')
            break
    return '%s/%d' % ('.'.join(new_parts), octets * 8)


def blank_to_cidr(ip):
    """Converts ip with blanks to cidr.

    E.g.: a.b.c. -> a.b.c.0/24, a.b.c -> a.b.c.0/24, etc.

    :param ip: String with ip to be converted.
    :return: String with cidr.
    """
    ip = ip.strip('.')
    if ip:
        octets = ip.count('.') + 1
    else:
        return '0.0.0.0/0'
    if octets == 4:
        return ip
    for x in xrange(4 - octets):
        ip += '.0'
    return '%s/%d' % (ip, octets * 8)


def reverse_ip(ip):
    """Reverse the ip.

    E.g.: a.b.c.d -> d.c.b.a

    :param ip: String with ip to be reversed.
    :return: String with reversed ip.
    """
    parts = ip.split('.')
    parts.reverse()
    return '.'.join(parts)
