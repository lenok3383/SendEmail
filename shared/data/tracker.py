"""IPAS Header parsers.

:Author: gperry, rbodnarc
:Version: $Id: //prod/main/_is/shared/python/data/tracker.py#8 $
"""

import base64
import socket
import struct
import time

from shared.net import ip


PACKAGES_VERSION_FORMAT = '%Y%m%d_%H%M%S'

HEADER_FORMAT = {'version': 'B',
                 'spam_score': 'H',
                 'vof_score': 'B',
                 'rules_ver': 'L',
                 'ip': '4s',
                 'sbrs': 'B',
                 'flags': 'B', }


class IpasHeaderError(Exception):
    """Exception class for TrackerHeader errors."""
    pass


class TrackerHeader(object):

    """Object representing a X-IronPort-Anti-Spam-Result header."""

    def __init__(self, header):
        """Initialize TrackerHeader instance.

        :param header: Value of X-IronPort-Anti-Spam-Result header.
        """
        self._header = header
        try:
            self._parts = self._decode(header)
        except (struct.error, TypeError) as err:
            raise IpasHeaderError('Invalid header format. Original header:'
                                  ' "%s". Original error: "%s"' % \
                                  (self._header, str(err)))

    def __call__(self):
        """Call TrackerHeader instance.

        :return: Tuple with IPAS rule indices.
        """
        return self._parts['rule_indices']

    def _decode(self, header):
        """Helper for decoding header value.

        :param header: Value of X-IronPort-Anti-Spam-Result header.
        :return: Dictionary with decoded information about IPAS result.
        """

        tracker_header_string = header + '=' * (4 - len(header) % 4)
        bits = base64.b64decode(tracker_header_string)

        # Grab header version.
        version = struct.unpack(HEADER_FORMAT['version'],
                                bits[0])[0]

        # Depend on header version we expect different header format.
        if version in (0, 1):
            handle = V1HeaderProcessor()
        elif version == 2:
            handle = V2HeaderProcessor()
        elif version == 3:
            handle = V3HeaderProcessor()
        else:
            raise IpasHeaderError('Unknown header version')

        result = handle.process(bits)
        return result

    @property
    def header(self):
        """The raw base64 encoded header."""
        return self._header

    @property
    def header_version(self):
        """The IPAS header version."""
        return self._parts['version']

    @property
    def spam_score(self):
        """The spam scores of this email."""
        return self._parts['spam_score'] / 655.35

    @property
    def vof_score(self):
        """"The VOF score of this email."""
        return self._parts['vof_score'] / 51.0

    @property
    def packages_version(self):
        """The package version string."""
        return time.strftime(PACKAGES_VERSION_FORMAT,
                             time.gmtime(self._parts['rules_ver']))

    @property
    def packages_version_int(self):
        """The package version as an integer."""
        return long(self._parts['rules_ver'])

    @property
    def profile(self):
        """The IPAS Profile."""
        return self._parts['region']

    @property
    def ip(self):
        """The sender's IPv4 address as signed integer."""
        if self._parts.get('ip'):
            return ip.ip_to_signed_int(socket.inet_ntoa(self._parts.get('ip')))
        else:
            return None

    @property
    def ipv6(self):
        """The sender's IPv6 address as long."""
        return self._parts.get('ipv6')

    @property
    def sbrs(self):
        """The sender's SBRS score."""
        raw = self._parts['sbrs']
        if raw == None or raw == 255:
            return None
        else:
            return (raw / 10.0) - 10

    @property
    def ip_version(self):
        """IP version.

        Might be:
          32 - ipv4;
          64 - top bits of ipv6, missed bits will be complemented with nulls;
          128 - full ipv6.
        """
        if 'flags' in self._parts:
            return self._parts.get('flags').get('ip_version')
        else:
            return None

    @property
    def is_outbound(self):
        """Outbound flag."""
        if 'flags' in self._parts:
            return self._parts.get('flags').get('is_outbound')
        else:
            return None

    @property
    def is_internal_relay(self):
        """Internal relay flag."""
        if 'flags' in self._parts:
            return self._parts.get('flags').get('is_internal_relay')
        else:
            return None

    def __getitem__(self, item):
        return self._parts['rule_indices'][item]

    def __iter__(self):
        for ind in self._parts['rule_indices']:
            yield ind

    def __len__(self):
        return len(self._parts['rule_indices'])

    def __contains__(self, item):
        return item in self._parts['rule_indices']

    def __eq__(self, item):
        return isinstance(item, self.__class__) and item.header == self.header

    def __hash__(self):
        return hash(self._header)

    def __repr__(self):
        return '<%s: %s>' % (self.packages_version,
                             str(self._parts['rule_indices']),)


class HeaderProcessor(object):

    """Main helper class for header processing."""

    def __init__(self, parts):
        """Initialize HeaderProcessor instance.

        :param parts: List with header parts.
        """
        self._parts = parts

    def process(self, bits):
        """Process IPAS header.

        :param bits: Decoded header value.
        :return: Structured dictionary with header information.  E.g.:
                {'spam_score': 2621,
                 'rules_ver': 0,
                 'ip': '\xac\x15\x91=',
                 'region': 'NorthAmerica',
                 'vof_score': 0,
                 'rule_indices': (509, 553, 812, 886),
                 'version': 2,
                 'sbrs': 255}
        """
        result = dict()
        struct_format = '=%s' % (''.join(
                            HEADER_FORMAT[part] for part in self._parts),)

        base_size = struct.calcsize(struct_format)

        data_parts = struct.unpack(struct_format, bits[:base_size])
        for i in range(len(data_parts)):
            result[self._parts[i]] = data_parts[i]

        self._process_version(bits[base_size:], result)

        return result

    def _process_version(self, bits, result):
        """Helper for processing header parts, which depend on version.

        MUST be redefined in child class!

        :param bits: Decoded part of header to be processed.
        :param result: Dictionary to save obtained information.
        """
        pass

    def _grab_ber_integers(self, bits, num=None):
        """Helper for unpacking BER compressed integers.

        :param bits: Data to be unpacked.
        :param num: Number of elements to be unpacked.  If None, try
                    to unpack all data as BER integers.
        :return: Pair (offset, vals) where offset is an index of first
                 unprocessed byte, vals - list with integers.
        """
        vals = list()
        tmp_buffer = 0
        i = 0

        chars = struct.unpack('B' * len(bits), bits)

        for i, val in enumerate(chars):
            tmp_buffer <<= 7
            tmp_buffer += val & 127

            if val & 128 == 0:
                vals.append(tmp_buffer)
                tmp_buffer = 0
                if num and len(vals) == num:
                    break

        return i + 1, vals

    def _grab_indices(self, bits):
        """Helper for unpacking rules indices.

        :param bits: Decoded part of header to be processed.
        :return: Tuple with unpacked values.
        """
        if not bits:
            return tuple()

        _, vals = self._grab_ber_integers(bits)
        return tuple(sum(vals[:i + 1]) for i in range(0, len(vals)))

class V1HeaderProcessor(HeaderProcessor):

    """Helper class for processing 'v0' or 'v1' header versions."""

    def __init__(self):
        """Initialize parent HeaderProcessor instance.

        Uses just a portion of header parts.
        """
        HeaderProcessor.__init__(self, ['version', 'spam_score',
                                        'vof_score', 'rules_ver'])

    def _process_version(self, bits, result):
        """"Helper for defining header fields.

        Defines region, ip, sbrs and rule indices for 'v0' or 'v1' header
        versions.

        :param bits: Decoded part of header to be processed.
        :param result: Dictionary to save obtained information.
        """
        # We need to set this values manually
        # since old headers doesn't have such information.
        result['ip'] = None
        result['sbrs'] = None
        result['flags'] = dict()
        result['region'] = 'global'

        result['rule_indices'] = self._grab_indices(bits)


class V2HeaderProcessor(HeaderProcessor):

    """Helper class for processing 'v2' header versions."""

    def __init__(self):
        """Initialize parent HeaderProcessor instance.

        Uses full list of header parts.
        """
        HeaderProcessor.__init__(self, ['version', 'spam_score',
                                        'vof_score', 'rules_ver',
                                        'ip', 'sbrs'])

    def _process_version(self, bits, result):
        """"Helper for defining header fields.

        Defines region, ip, sbrs and rule indices for 'v2' header version.

        :param bits: Decoded part of header to be processed.
        :param result: Dictionary to save obtained information.
        """
        result['flags'] = dict()
        # Extract the null-terminated region profile string
        end_of_region_profile = 0
        for i in range(len(bits)):
            if bits[i] == '\0':
                end_of_region_profile = i
                break
        result['region'] = bits[:end_of_region_profile]
        result['rule_indices'] = self._grab_indices(
                                        bits[end_of_region_profile + 1:])


class V3HeaderProcessor(HeaderProcessor):

    """Helper class for processing 'v3' header versions.

    See format specification here:
    http://eng.ironport.com/docs/is/case-engine/trackerheaderv3.rst
    """

    REGIONS = {0: 'global',
               2: 'zh'}

    IP_TYPES = {1: 32,
                2: 64,
                3: 128}


    def __init__(self):
        """Initialize parent HeaderProcessor instance.

        Uses full list of header parts.
        """
        HeaderProcessor.__init__(self, ['version', 'flags', 'spam_score',
                                        'vof_score', 'rules_ver',
                                        'sbrs'])

    def _process_version(self, bits, result):
        """"Helper for defining header fields.

        Defines region, ip, sbrs, rule indices and flags 'v3' header versions.

        :param bits: Decoded part of a header to be processed.
        :param result: Dictionary to save obtained information.
        """
        encoded_flags = result['flags']
        result['flags'] = dict()

        # Flags format: XXOIPP**, where
        # XX - ip version, O - outbound flag, I - internal relay flag,
        # PP - profile, * - reserved/unused.

        result['region'] = V3HeaderProcessor.REGIONS.get(
                                            encoded_flags >> 2 & 3, 'unknown')
        result['flags']['ip_version'] = V3HeaderProcessor.IP_TYPES.get(
                                            encoded_flags >> 6)

        if result['flags']['ip_version'] is None:
            raise IpasHeaderError('Unknown IP version')

        result['flags']['is_outbound'] = bool(encoded_flags >> 5 & 1)
        result['flags']['is_internal_relay'] = bool(encoded_flags >> 4 & 1)

        # Add ip information to 'result' dictionary.
        # If we have ipv6 then 'ip' key in the result dictionary will be
        # omitted.
        bits = self._grab_ip(result, bits)

        result['rule_indices'] = self._grab_indices(bits)

    def _grab_ip(self, result, bits):
        """Grab ip information.

        Basing on ip_version value we expect:
          - 32 bits of ipv4;
          - top 64 bits of ipv6;
          - all 128 bits of ipv6.

        :param result: Result dictionary to be extended.
        :param bits: Data to be processed/unpacked.
        :return: Rest of bits with indices values.
        """
        if result['flags']['ip_version'] == 32:
            top_ip_format = '=4s'
            top_ip = struct.unpack(top_ip_format, bits[:4])[0]
            result['ip'] = top_ip
            return bits[4:]

        top_ip_format = '=L'
        top_ip_int = struct.unpack(top_ip_format, bits[:4])[0]
        bits = bits[4:]

        if result['flags']['ip_version'] == 64:
            (offset, ip_parts) = self._grab_ber_integers(bits, num=1)
            result['ipv6'] = ((top_ip_int << 32) |
                              (ip_parts[0])) << 64
        else:
            (offset, ip_parts) = self._grab_ber_integers(bits, num=3)
            result['ipv6'] = ((top_ip_int << 96) |
                              (ip_parts[0] << 64) |
                              (ip_parts[1] << 32) |
                              (ip_parts[2]))

        return bits[offset:]
