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

:Status: $Id: //prod/main/_is/shared/python/net/iputils.py#16 $
:Authors: michaelo, aflury, ted, kylev, apelkmann
"""

import re
import socket
import struct
import types
import warnings

IPV4_MAX = 2 ** 32 - 1  # 0xFFFFFFFF
IPV6_MAX = 2 ** 128 - 1 # 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

class InvalidIpStrError(ValueError):
    """Exception class for invalid IP string errors."""
    pass

class InvalidIPv6StrError(InvalidIpStrError):
    """Exception class for invalid IPv6 string errors."""
    pass


def bin_to_ipv6long(ipv6_binary):
    """Convert IPv6 address from binary format to long integer.

    :param binary: 16 bytes string representing IPv6 address in big-endian
                   byte order.
    :return: IPv6 address as unsigned long integer.
    """
    hb, lb = struct.unpack('!QQ', ipv6_binary)
    return (hb << 64) + lb


def ipv6long_to_bin(ipv6_long):
    """Convert IPv6 address from long integer format to binary format.

    :param ipv6_long: IPv6 address as unsigned long integer.
    :return: 16 bytes string of IPv6 address in big-endian byte order format.
    """
    return struct.pack('!QQ', ipv6_long >> 64, ipv6_long & 0xffffffffffffffff)


def ip_to_signed_int(ip):
    """Convert IP address from dotted-quad string to network order int.

    Consider this deprecated, and only use it for working with old DBs
    that store IPs as signed 32-bit integers.

    :param ip: String with ip in dotted-quad format.
    :return: Signed integer value of ip.
    """
    warnings.warn(
        'Storing IPs as signed integers is deprecated.', DeprecationWarning
        )
    return struct.unpack('>i', socket.inet_aton(ip))[0]


def ip_to_unsigned_int(ip):
    """Convert IP address from dotted-quad string to an unsigned int.

    :param ip: String with ip in dotted-quad format.
    :return: Long integer value of ip.
    """
    return struct.unpack('!L', socket.inet_aton(ip))[0]


def ipv6_to_unsigned_int(ip):
    """Convert the given IPv6 from string into unsigned long integer.

    :param ip:  String containing an IPv6 network address.
    :return: Unsigned long integer value of the given IPv6 string.
    :Exceptions: ValueError if the given IP is invalid.
    """
    return bin_to_ipv6long(socket.inet_pton(socket.AF_INET6, ip))


def any_ip_to_unsigned_int(ip, af=None):
    """Convert the given IP from string into unsigned long integer.

    Supports Ipv4 and IPv6 addresses.

    :param ip: String representing IPv4 or IPv6 address.
    :param af: Unix address family of the given IP.
    :Return: Unsigned long integer value of the given IP string.
    :Exceptions: ValueError if the given IP or socket family is invalid.
    """
    try:
        if af is None:
            if ':' in ip:
                af = socket.AF_INET6
            else:
                af = socket.AF_INET

        if af == socket.AF_INET:
            return ip_to_unsigned_int(ip)
        elif af == socket.AF_INET6:
            return ipv6_to_unsigned_int(ip)
        else:
            raise ValueError, 'Invalid address family'
    except (socket.error, UnicodeEncodeError):
        raise ValueError, 'Invalid IP address value'


def signed_int_to_ip(iip):
    """Convert IP address in 32-bit int to dotted-quad string.

    Consider this deprecated, and only use it for working with old DBs
    that store IPs as signed 32-bit integers.

    :param iip: Signed integer value of ip.
    :return: String with ip in dotted-quad format.
    """
    warnings.warn(
        'Storing IPs as signed integers is deprecated.', DeprecationWarning)
    return socket.inet_ntoa(struct.pack('>i', int(iip)))


def unsigned_int_to_signed_int(unsigned):
    """Helper for converting unsigned int to signed int.

    :param unsigned: An unsigned integer value.
    :return: A signed integer value.
    """
    signed = unsigned if unsigned <= 0x7FFFFFFF else unsigned - 0x100000000
    return signed


def signed_int_to_unsigned_int(signed):
    """Helper for converting signed int to unsigned int.

    :param signed: An signed integer value.
    :return: A unsigned integer value.
    """
    unsigned = signed if signed >= 0 else 0x100000000 + signed
    return unsigned


def unsigned_int_to_ip(iip):
    """Convert IP address in 32-bit unsigned int to dotted-quad string.

    :param iip: Unsigned integer value of ip.
    :return: String with ip in dotted-quad format.
    """
    return socket.inet_ntoa(struct.pack('>L', long(iip)))


def unsigned_int_to_ipv6(int_val):
    """Convert 128-bit unsigned int to IPv6 string.

    :param int_val: Unsigned integer value of IPv6.
    :return: String representation of IPv6 address.
    """
    return socket.inet_ntop(socket.AF_INET6, ipv6long_to_bin(int_val))


def any_unsigned_int_to_ip(iip):
    """Transform 32-bit or 128-bit decimal numbers to IPv4 or v6 string.

    :param iip:  Unsigned integer representation of an IPv4 or IPv6 address.
    :Return: String representation of IPv4 or IPv6.
    """
    if 0 <= iip <= IPV4_MAX:
        return unsigned_int_to_ip(iip)
    else:
        return unsigned_int_to_ipv6(iip)


def get_address_family(ip):
    """Get socket address family of the given IP address.

    :param ip: String containing an IPv network address.
    :Return: Unix address family of the given IP address.
    :Exceptions: ValueError if the given IP is invalid.
    """
    if is_valid_ip(ip):
        return socket.AF_INET
    elif is_valid_ipv6(ip):
        return socket.AF_INET6
    else:
        raise ValueError, 'Invalid IP address value'


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


def is_any_ip(ip):
    """Determine if an address is an IP address v4 or v6.

    :param address: The IP address string to check.
    :Return: Returns True if it is either a v4 or v6 address, otherwise False.
    """
    if ':' in ip:
        return is_valid_ipv6(ip)
    else:
        return is_valid_ip(ip)


def is_valid_ip(ip):
    """Validate IPv4 address.

    This function supports IPv4 addresses.

    :param ip: String containing an IPv4 network address in dotted-decimal
               format.
    :Return: True if the given IP is a valid IPv4 address; False otherwise.
    """
    try:
        packed = socket.inet_aton(ip)
        unpacked = socket.inet_ntoa(packed)
        if ip == unpacked:
            return True
        else:
            return False
    except (socket.error, UnicodeEncodeError, TypeError):
        return False


def is_valid_ipv6(ip):
    """Validate IPv6 address.

    This function supports IPv6 addresses.

    :param ip: String containing an IPv6 network address.
    :Return: True if the given IP is a valid IPv6 address; False otherwise.
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
        return True
    except (socket.error, UnicodeEncodeError, TypeError):
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


def is_valid_ipv6_cidr(cidr):
    """Verifies valid CIDR format. Note: Assumes IPv6 128 bit addressing.

    :param cidr: String with cidr to be checked. Example: abcd:1234::/32
    :return: True if cidr is valid, False otherwise.
    """
    try:
        prefix, subnet = cidr.split('/')
        subnet = int(subnet)
    except ValueError:
        prefix = cidr
        subnet = 128

    return is_valid_ipv6(prefix) and 1 <= subnet <= 128


_NON_ROUTABLE_RE = re.compile(r'^(0\.|'
                               '10\.|'
                               '127\.|'
                               '169\.254\.|'
                               '172\.(1[6-9]|2\d|3[0-1])\.|'
                               '192\.0\.2\.|'
                               '198\.51\.100\.|'
                               '203\.0\.113\.|'
                               '192\.168\.|'
                               '198\.(1[8-9])\.|'
                               '(22[4-9]|23\d)\.|'
                               '100\.(6[4-9]|[7-9]\d|1[0-1]\d|12[0-7])\.|'
                               '255\.)')

_NON_ROUTABLE_IPV6_RE = re.compile(r'^(\:\:1$|'             # ::1/128
                                    '\:\:$|'                # ::/128
                                    '\:\:ffff|'             # ::ffff:0:0/96
                                    '\:\:[0-9]+\.|'         # ::<ipv4-address>/96
                                    'fe[89ab][0-9a-f]|'     # fe80::/10
                                    'f[cd][0-9a-f][0-9a-f]|'# fc00::/7
                                    '2001\:db8\:|'          # 2001:db8::/32
                                    '2001\:1[0-9a-f]\:|'    # 2001:10::/28
                                    '2001\:2\:[0\:])')      # 2001:2::/48

def is_routable_ip(ip):
    """Determine if the given IP is one which is publicly routable.

    This is based on the list of non-routable networks in RFC 3330
    (which includes RFC 1918).
    Also see http://www.faqs.org/rfcs/rfc3330.html.

    :param ip: String with ip to be checked.
    :return: True if ip is routable, False otherwise.
    """
    return is_valid_ip(ip) and not _NON_ROUTABLE_RE.match(ip)


def is_routable_ipv6(ip):
    """Determine if a given IPv6 is publicly routable.

    This is based on the list of non-routable networks from RFC 5156
    (http://www.faqs.org/rfcs/rfc5156.html).  One more block was added
    according to
    http://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xml.

    :param ip: IPv6 address as string.
    :return: True if IP is routable, False otherwise.
    """
    if not is_valid_ipv6(ip):
        return False

    ip = canonify_ipv6_str(ip)
    return not _NON_ROUTABLE_IPV6_RE.match(ip)


def is_routable_any_ip(ip):
    """Determine if a given IPv4 or IPv6 is publicly routable.

    :param ip: IPv4 or IPv6 address as string.
    :return: True if IP is routable, False otherwise.
    """
    if ':' in ip:
        return is_routable_ipv6(ip)
    else:
        return is_routable_ip(ip)


def is_illegal_source_ip(ip):
    """Check if email can't be sent from this ip.

    :param ip: String with ip to be checked.
    :return: True if ip is illegal, False otherwise.
    """
    return _ILLEGAL_SOURCE_RE.match(ip)


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
    ip_high = ip_int | (mask ^ IPV4_MAX)
    if signed:
        return (unsigned_int_to_signed_int(ip_low),
                unsigned_int_to_signed_int(ip_high))
    else:
        return ip_low, ip_high


def ipv6_cidr_to_range(ip):
    """Convert IPv6 CIDR to a range limited by two integers.

    :param ip: IPv6 address or CIDR as string.  Supported formats:
               - '20ab:0db8:85a3:0000:0000:8a2e:0370:7334'
               - '20ab:0db8:85a3:0000:0000:8a2e:0370:7334/16'
    :return: tuple of two unsigned integers:
             (low ip, high ip)
    :raise: ValueError if invalid IP address or CIDR value.
    """
    try:
        if not '/' in ip:
            ip_low = ip_high = ipv6_to_unsigned_int(ip)
            return ip_low, ip_high

        ip, bits = ip.split('/')
        ip = ipv6_to_unsigned_int(ip)
        mask = make_mask_ipv6(int(bits))
    except (socket.error, UnicodeEncodeError, ValueError):
        raise ValueError('Invalid IP address or CIDR value')

    ip_low = ip & mask
    ip_high = ip | (mask ^ IPV6_MAX)
    return ip_low, ip_high


def any_cidr_to_range(ip):
    """Convert IPv4 or IPv6 CIDR to a range limited by two integers.

    :param ip: IP or IPv6 address or CIDR.
    :return: tuple of two unsigned integers:
             (low ip, high ip)
    """
    if ':' in ip:
        return ipv6_cidr_to_range(ip)
    else:
        return cidr_to_range(ip)


def cidr_to_ip_range_hex(cidr):
    """For a given CIDR block, return a tuple of the starting IP and
    ending IP for that block.  IP addresses will be hexadecimal
    strings for easy sortability.

    :param cidr: the IP / CIDR block.

    :return: tuple of the form ('ce5dea00', 'ce5deaff').
    """
    return tuple(map(
        lambda i: '%08x' % (i,),
        cidr_to_range(cidr)))


def make_mask(bits):
    """Make a mask with netbits suitable for the results of 'ip_to_int'.

    :param bits: An integer number of bits for a mask (0 <= bits <= 32).
    :return: A 32-bit integer mask with the highest *bits* bits set to 1.
    """
    if bits < 0 or bits > 32:
        raise ValueError('Number of bits must be'
                         ' between 0 and 32 (inclusive)')
    return (IPV4_MAX << (32 - bits)) & IPV4_MAX


def make_mask_ipv6(bits):
    """Make a mask with a given number of bits suitable for IPv6 addresses.

    :param bits: number of bits (0 <= integer <= 128).
    :return: 128-bit integer mask.
    :raise: ValueError if invalid bits value.
    """
    if bits < 0 or bits > 128:
        raise ValueError('Number of bits must be between 0 and 128')
    return (IPV6_MAX << (128 - bits)) & IPV6_MAX


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


def canonify_ipv6_str(ip):
    """Bring IPv6 string into a canonical form.

    E.g.: '0000:0000:0000::1234' -> '::1234', '::FFFF' -> '::ffff'
    :param ip: IPv6 address as string.
    :return: canonical IPv6 string.
    :raise: ValueError if invalid IP address value.
    """
    try:
        return socket.inet_ntop(socket.AF_INET6,
                                socket.inet_pton(socket.AF_INET6, ip))
    except (socket.error, UnicodeEncodeError):
        raise ValueError, 'Invalid IP address value'

# "http://0x7f000001/"
IP_LIKE_OCT_NUM_RE = re.compile('0x[0-9a-f]+$')
# "http://1113343453/"
IP_LIKE_DEC_NUM_RE = re.compile('[0-9]+$')

def canonify_ip_str(ip_str):
    """Format strange ip strings to canonical hexadecimal digits form.

    E.g.: 213.172.0x1f.13, 0x7f000001, 1113343453

    :param ip_str: String with IP.
    :return: String with IP in canonical hexadecimal digits form.
    :raise: InvalidIPv6StrError if invalid IPv6 string
            InvalidIpStrError if invalid IP string
    """
    like_ipv6 = False

    def is_numeric(s):
        return IP_LIKE_OCT_NUM_RE.match(s) or IP_LIKE_DEC_NUM_RE.match(s)

    def segment_to_int(segment):
        try:
            if segment.startswith('0x'):
                return '%s' % (int(segment, 16),)
            elif segment.startswith('0'):
                return '%s' % (int(segment, 8),)
            else:
                return segment
        except ValueError:
            if like_ipv6:
                raise InvalidIPv6StrError('Invalid IPv6 string: %s' % (ip_str,))
            else:
                raise InvalidIpStrError('Invalid IP string: %s' % (ip_str,))

    ip_like_str = ip_str.lower()
    like_ipv6 = ':' in ip_like_str

    if is_numeric(ip_like_str):
        # Format strange ip strings to int's, e.g.:
        #   '0x7f000001' -> 2130706433
        #   '1113343453' -> 1113343453
        ip_like_str = any_unsigned_int_to_ip(int(ip_like_str, 0))

    if is_any_ip(ip_like_str):
        return ip_like_str
    else:
        # Try to format ip's like '213.172.0x1f.13' to canonical form
        canonical_ip = []

        if like_ipv6:
            # Segment an IPv6 [part of] address
            segments = ip_like_str.split(':')
            for segment in segments[:-1]:
                canonical_ip.append(segment_to_int(segment))
                canonical_ip.append(':')
            if '.' in segments[-1]:
                # If this is an IPv4-mapped IPv6 address having the last 32 bits
                # written in the customary dot-decimal notation of IPv4, e.g.:
                # '::ffff:192.0.2.128'
                ip_like_str = segments[-1]
            else:
                canonical_ip.append(segment_to_int(segments[-1]))
                ip_like_str = ''

        if ip_like_str:
            # Segment an IPv4 [part of] address
            segments = ip_like_str.split('.')
            for segment in segments[:-1]:
                canonical_ip.append(segment_to_int(segment))
                canonical_ip.append('.')
            # Add the last segment
            canonical_ip.append(segment_to_int(segments[-1]))

        canonical_ip = ''.join(canonical_ip)

        if is_any_ip(canonical_ip):
            return canonical_ip

    if like_ipv6:
        raise InvalidIPv6StrError('Invalid IPv6 string: %s' % (ip_str,))
    else:
        raise InvalidIpStrError('Invalid IP string: %s' % (ip_str,))


def apply_cidr_iip(iip, cidr):
    """Convert a CIDR to a mask and apply it to an IPv4 as an unsigned int.

    NOTE: This does no sanity checking.

    :param ip: int, an IP to apply the CIDR to
    :param cidr: uint, the CIDR to use as a mask
    :return: uint, an IP with the mask applied
    """
    return make_mask(cidr) & iip


def apply_cidr_iip6(iip, cidr):
    """Convert a CIDR to a mask and apply it to an IPv6 as an unsigned int.

    NOTE: This does no sanity checking.

    :param ip: int, an IP to apply the CIDR to
    :param cidr: uint, the CIDR to use as a mask
    :return: uint, an IP with the mask applied
    """
    return make_mask_ipv6(cidr) & iip


def apply_cidr_any_sip(sip, cidr):
    """Convert a CIDR to a mask and apply it to an IP as a string.

    NOTE: This does no sanity checking.

    :param ip: str, an IP to apply the CIDR to
    :param cidr: int, the CIDR to use as a mask
    :return: str, an IP with the mask applied
    """
    iip = any_ip_to_unsigned_int(sip)
    if ':' in sip:
        masked_iip = apply_cidr_iip6(iip, cidr)
        masked_sip = unsigned_int_to_ipv6(masked_iip)
    else:
        masked_iip = apply_cidr_iip(iip, cidr)
        masked_sip = unsigned_int_to_ip(masked_iip)
    return masked_sip


def canonify_ipv4_str_lite(ip_str):
    """Convert an IPv4 like string into canonical form. Light/Fast version.

    :param ip: IPv4 address as string.
    :return: canonical IPv4 string.
    :raise: InvalidIpStrError if invalid IPv4 string.
    """
    def segment_to_int(segment):
        try:
            if segment.startswith('0x'):
                return '%s' % (int(segment, 16),)
            elif segment.startswith('0'):
                return '%s' % (int(segment, 8),)
            else:
                return segment
        except ValueError:
            raise InvalidIpStrError('Invalid IP string: %s' % (ip_str,))

    if is_valid_ip(ip_str):
        return ip_str

    if IP_LIKE_OCT_NUM_RE.match(ip_str) or IP_LIKE_DEC_NUM_RE.match(ip_str):
        # Format strange ip strings to int's, e.g.:
        #   '0x7f000001' -> 2130706433
        #   '1113343453' -> 1113343453
        return any_unsigned_int_to_ip(int(ip_str, 0))

    # Try to format ip's like '213.172.0x1f.13' to canonical form
    canonical_ip = []

    if ip_str:
        # Segment an IPv4 [part of] address
        segments = ip_str.split('.')
        for segment in segments[:-1]:
            canonical_ip.append(segment_to_int(segment))
            canonical_ip.append('.')
        # Add the last segment
        canonical_ip.append(segment_to_int(segments[-1]))

    canonical_ip = ''.join(canonical_ip)

    if is_any_ip(canonical_ip):
        return canonical_ip

    raise InvalidIpStrError('Invalid IP string: %s' % (ip_str,))


# Output from SBNP Vector is a full IPv6 address without colons.
VECTOR_IPV6_RE = re.compile('^[0-9a-f]+$')
FULL_IPV4_RE = re.compile('^([0-9]{1,3}\.){3}[0-9]{1,3}$')

def investigate_ip(ip, check_routable=True):
    """Return what can be determined about the given candidate IP.

    :param ip: str or unsigned int of IP to investigate
    :param check_routable: boolean, should check if IP is routable
    :return: dict with findings on the IP:
        {'verdict': int,      # 4 if IPv4, 6 if IPv6, 0 if cannot determine
         'routable': boolean, # True if routable
         'sip': str,          # canonicalized string of IP
         'iip': int,          # IP as integer
         'cidr': int,         # CIDR or None if malformed CIDR
        }
    """
    verdict, routable, sip, iip, cidr = (0, False, '', 0, -1)

    def split_cidr(sip):
        sip, cidr = sip.split('/', 1)
        try:
            cidr = int(cidr)
        except ValueError:
            cidr = None
        return sip, cidr

    if ip and isinstance(ip, str):
        sip = ip.lower()
        if '/' in sip:
            sip, cidr = split_cidr(sip)

        if VECTOR_IPV6_RE.match(sip):
            iip = int(sip, 16)

        elif ':' not in sip and not FULL_IPV4_RE.match(sip):
            if '*' in sip:
                sip = star_to_cidr(sip)
            else:
                sip = blank_to_cidr(sip)
            if '/' in sip:
                if cidr >= 0:
                    sip = sip.split('/')[0]
                else:
                    sip, cidr = split_cidr(sip)

    elif ip and isinstance(ip, (int, long)):
        iip = ip

    if iip:
        try:
            sip = any_unsigned_int_to_ip(iip)
        except struct.error:
            sip = ''
            iip = 0

    if sip:
        try:
            if ':' in sip:
                sip = canonify_ipv6_str(sip)
                if check_routable:
                    routable = not _NON_ROUTABLE_IPV6_RE.match(sip)
                verdict = 6
                if not iip:
                    iip = ipv6_to_unsigned_int(sip)
            else:
                sip = canonify_ipv4_str_lite(sip)
                if check_routable:
                    routable = not _NON_ROUTABLE_RE.match(sip)
                verdict = 4
                if not iip:
                    iip = ip_to_unsigned_int(sip)
        except (ValueError, InvalidIpStrError, TypeError):
            pass

    if cidr == -1:
        if verdict == 6:
            cidr = 128
        elif verdict == 4:
            cidr = 32
        else:
            cidr = None
            verdict = 0
    elif cidr is None:
        verdict = 0

    # Check CIDRS.
    if verdict == 4 and sip and cidr and (cidr > 32 or cidr < 0):
        verdict = 0
        cidr = None
    elif verdict == 6 and sip and cidr and (cidr > 128 or cidr < 0):
        verdict = 0
        cidr = None

    # Clean up CIDRS.
    if cidr and verdict == 4:
        iip = apply_cidr_iip(iip, cidr)
        sip = unsigned_int_to_ip(iip)
    elif cidr and verdict == 6:
        iip = apply_cidr_iip6(iip, cidr)
        sip = unsigned_int_to_ipv6(iip)

    return {'verdict': verdict, 'routable': routable, 'sip': sip,
        'iip': iip, 'cidr': cidr}


def reverse_dns(ip):
    """Convert the IPv4 or IPv6 into a string suitable for a PTR lookup in DNS.

    This reverses the IP address and converts it to a string appropriate
    for PTR lookups in DNS.  For IPv4 it uses .in-addr.arpa suffix,
    and for IPv6 -- .ip6.arpa.

    :param ip: String with IP.
    :return: String of the IP address for a PTR lookup.
    """
    if ':' in ip:
        return reverse_dns_pieces_ipv6(ip, 'ip6.arpa')
    else:
        return reverse_dns_pieces_ipv4(ip, 'in-addr.arpa')


def reverse_dns_pieces_ipv4(ip, to_append=''):
    """Convert the IPv4 into a string that looks a lot like a PTR lookup.

    This reverses the IP address and converts it to a string appropriate
    for DNS Blacklist queries. This is basically the same as reverse_dns()
    but without the .in-addr.arpa suffix.

    For example::

        reverse_dns_pieces_ipv4('1.2.3.4') -> '4.3.2.1'
        reverse_dns_pieces_ipv4('1.2.3.4', 'suffix') -> '4.3.2.1.suffix'

    :param ip: String with IPv4 in dotted-decimal format.
    :param to_append: A string to append to the end of the reversed IP.
    :return: String of the reversed IP.
    """
    if to_append:
        to_append = '.' + to_append

    return reverse_ip(ip) + to_append


def reverse_dns_pieces_ipv6(ip, to_append=''):
    """Convert the IPv6 into a string that looks a lot like a PTR lookup.

    This reverses the IP address and converts it to a string appropriate
    for DNS Blacklist queries. This is basically the same as reverse_dns()
    but without the .ip6.arpa suffix.

    For example::

        reverse_dns_pieces_ipv6('2001:db8::567:89ab') ->
            'b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2'
        reverse_dns_pieces_ipv6('2001:db8::567:89ab', 'suffix') ->
            'b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.suffix'

    :param ip: String with IPv6.
    :param to_append: A string to append to the end of the reversed IP.
    :return: String of the reversed IP.
    """
    result = [None] * 32
    if to_append:
        result.append(to_append)

    value = ipv6_to_unsigned_int(ip)

    for index in xrange(32):
        result[index] = '%x' % (value & 0xf, )
        value >>= 4

    return '.'.join(result)


def reverse_dns_pieces(ip, to_append=''):
    """Convert the IPv4 or IPv6 into a string that looks a lot like a
    PTR lookup.

    This reverses the IP address and converts it to a string appropriate
    for DNS Blacklist queries. This is basically the same as reverse_dns()
    but without the .ip6.arpa or .in-addr.arpa suffix.

    For example::

        reverse_dns_pieces('1.2.3.4') -> '4.3.2.1'
        reverse_dns_pieces('1.2.3.4', 'suffix') -> '4.3.2.1.suffix'
        reverse_dns_pieces('2001:db8::567:89ab') ->
            'b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2'
        reverse_dns_pieces('2001:db8::567:89ab', 'suffix') ->
            'b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.suffix'

    :param ip: String with IP.
    :param to_append: A string to append to the end of the reversed IP.
    :return: String of the reversed IP.
    """
    if ':' in ip:
        return reverse_dns_pieces_ipv6(ip, to_append)
    else:
        return reverse_dns_pieces_ipv4(ip, to_append)
