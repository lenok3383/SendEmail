"""Registrar utility routines.

:Status: $Id: //prod/main/_is/shared/python/net/registrar.py#5 $
:Authors: vburenin, yifenliu


Registrar utility is a helpful tool to determine top level domains and
host name based on full host name. Registrar takes all the data from external
configuration file containing existing domain lists and
case specific patterns. There are three different type of data sections in that
file for that:

1. Domain list. Contains set of domains separated by space
   or new line. Domain section definition look like:
   <Domains: domain_level data_name>
2. Data list. Set of specific data that is usually a part of domain and
   is used inside of patterns to match a part of hostname. Data is
   separated by space or by new line. Data section definition look like:
   <Data: data_name>.
3. Pattern list. A list of patterns separated by new line.
   Pattern section definition look like.
   <Patterns: domain_level pat_name>

Any other types will be ignored and may be used by other products to have
custom configuration domain data.

Domain list:
    Domain level refers to the domain level list. It means, if there is the
    first level file content should be like this: 'com gov info biz ua'
    Second level domain list will contain data like this: 'com.ua, rv.ua'
    <Name> can be used to refer domain name in the patterns.

Data list:
    Usually, it is a list of parts of domains. It can be the list of US states.
    Since, US have the specific domain rules it can be referred by name
    to build complex patterns like 'pvt.k12.(us_states).us'.

Pattern list:
    List of patterns to match special cases to build domains.
    Number of parts of pattern should be the same as specified domain level.
    The following syntax parts are used:
        - '/' separator between pattern parts.
        - '%in(<referal_name>)' - check content of data set or domain set.
        - '%re(<regexp>)' - Runs regular expression against part of domain.
        - '$' - Substitution for any type of value.
    For example: pvt/k12/%in(us_states)/us - will hit any host name that
                 starts from 'pvt.k12' and finishes with 'us', third
                 part of domain can be any US state. In order to refer
                 specific US states, data file should be created with the list
                 of US states and the following name: us_states.data.
                 $/%re(^k[abc]$)/%in(us_states)/us will hit the following
                 host names: bla-bla.ka.ro.us, ko-ko.kb.ca.us, le-le.kc.ca.us
                 Use '$' instead empty regex, since it works faster.
    Note: All patterns processed in order as they listed, it finishes
    check on first hit.
"""

import logging
import os
import re
import threading
import time
from collections import deque

from shared.conf import env

_DNS_VALID = re.compile(r'^[a-z0-9]+[a-z0-9\-\.]+$', re.I)
_DOT_TLD = re.compile(r'\.([^.]+)$')
_RE_MULTI_DOT = re.compile(r'\.+')

PREFIX_DOMAINS = 'domains:'
PREFIX_PATTERNS = 'patterns:'
PREFIX_DATA = 'data:'
_DOMAIN_TRAILINGS = ' .\t\n\r\v\f'


def normalize_hostname(domain):
    """Removes trailing spaces/dots and dots doubles in the domain name.

    :param domain: Domain name.
    :returns: Normalized domain name.
    """

    if domain:
        return _RE_MULTI_DOT.sub('.', domain.strip(_DOMAIN_TRAILINGS).lower())
    return domain


class InitError(RuntimeError):

    """Raised if Registrar is not initialized."""

    pass


_registrar_object = None


def get_registrar():
    """Get currently initialized registrar instance"""
    if not _registrar_object:
        raise InitError('Registrar object is not initialized')
    return _registrar_object


REGISTRAR_DATA_FILE_NAME = 'registrar_data.dat'


def init_registrar(config_path=None, refresh_interval=86400):
    """Init registrar instance.

    :param config_path: Path to custom configuration file.
    :param refres_interval: Time interval to reload config.
    """
    global _registrar_object

    # If there is no user path, try current prod confroot first.
    # If there is no data in confroot, load default from 'shared'.
    if config_path is None:
        config_path = os.path.join(env.get_conf_root(),
                                 REGISTRAR_DATA_FILE_NAME)
        if not os.path.isfile(config_path):
            config_path = os.path.join(os.path.dirname(__file__),
                                       REGISTRAR_DATA_FILE_NAME)

    _registrar_object = _Registrar(config_path, refresh_interval)


class _Registrar(object):

    def __init__(self, config_path, refresh_interval):
        """Constructor.

        :param config_path: Path to custom configuration files.
        :param refres_interval: Time interval to reload config.
        """
        self.__patterns = {}
        self.__data = {}
        self.__custom_domains = {}
        self.__domains = {}
        self.__refresh_interval = refresh_interval
        self.__config_path = config_path
        self.__next_read_ts = 0
        self.__log = logging.getLogger('shared.net.registrar._Registrar')
        self.__lock = threading.RLock()

    def split_domain(self, domain):
        """Split domain name to the host and the domain name.

        Examples:

          "www.foo.com" => ("www", "foo.com")
          "www.foo.co.uk" => ("www", "foo.co.uk")
        """

        self.__read_data()
        hostname, domain = self.__find_host_and_domain(domain)
        return '.'.join(hostname), '.'.join(domain)

    def trim_domain(self, domain):
        """Determine host domain.

        :param domain: FQDN host name.
        :return: host domain name.
        """

        self.__read_data()
        return '.'.join(self.__find_host_and_domain(domain)[1])

    def iterate_domains(self):
        """Iterate over all available domains. Patterns are not iterated."""

        self.__read_data()
        all_domains = self.__domains.copy()

        for domains in all_domains.itervalues():
            for domain_name in domains:
                yield domain_name

    def is_tld(self, domain):
        """Check if it is pure domain name.

        It goes through the all rules and matches domain by existing names
        or domain names.
        """

        self.__read_data()
        domparts = normalize_hostname(domain).split('.')
        return bool(self.__match_domain_in_set(domparts) or \
                self.__match_domain_pattern(domparts))

    def is_domain_valid(self, domain):
        """Basic smoke test to see if a domain/hostname is valid."""

        self.__read_data()
        if not _DNS_VALID.search(domain):
            # Only alpha-numeric with dots and hyphens.
            return False
        dotmatch = _DOT_TLD.search(domain)
        if (not dotmatch) or (dotmatch.group(1) not in
            self._Registrar__data['top_level_domains']):
            # Ensure it ends in a known-valid TLD, and has at least 1 dot.
            return False
        return True

    def __find_host_and_domain(self, domain):
        """Split domain name to the host name and the domain name.

        Examples:

          "www.foo.com" => (['www'], ['foo', 'com'])
          "www.foo.co.uk" => (['www'], ['foo', 'co', 'uk'])
        """

        if not domain:
            return ('','')

        domparts = deque(normalize_hostname(domain).split('.'))
        hostname = deque()

        while len(domparts) > 1:
            if self.__match_domain_in_set(domparts):
                break
            if self.__match_domain_pattern(domparts):
                break
            hostname.append(domparts.popleft())

        if hostname:
            # Add the "next part" to the TLD to make the domain.
            domparts.appendleft(hostname.pop())

        return hostname, domparts

    def __match_domain_in_set(self, domain_blocks):
        """Determines if current domain name is a real domain."""
        level = len(domain_blocks)
        return (level in self.__domains) and \
            ('.'.join(domain_blocks) in self.__domains[level])

    def __match_domain_pattern(self, domain_blocks):
        """Match domain name against parent."""
        level = len(domain_blocks)
        if level in self.__patterns:
            for pattern_matchers in self.__patterns[level]:
                matches = 0
                for matcher, block in zip(pattern_matchers, domain_blocks):
                    if matcher(block):
                        matches += 1
                    else:
                        break
                if matches == level:
                    return True

    def __read_data(self):
        """Read registrar configuration data."""
        if self.__next_read_ts > time.time():
            return
        self.__lock.acquire()
        try:
            if self.__next_read_ts <= time.time():
                # There is a rare condition that two thread both get there in a
                # short period. If one reloads the data already then there is
                # no need for another to do it again.
                # Reset all containers.
                self.__log.debug('Load registrar data from: %s',
                    self.__config_path)
                self.__patterns = {}
                self.__data = {}
                self.__domains = self.__custom_domains.copy()

                with open(self.__config_path) as fh:
                    reg_data = fh.read().split('<')

                # Clean possible empty parts.
                reg_data = [data for data in reg_data if data]

                for reg_data_chunk in reg_data:
                    header, data = reg_data_chunk.split('\n', 1)
                    header = header.strip('<>')
                    data_type, data_info = header.split(' ', 1)
                    data_type = data_type.lower()
                    if data_type.startswith(PREFIX_DOMAINS):
                        dom_level, data_name = data_info.split()
                        self.__load_domain(data, int(dom_level),
                            data_name.strip())
                    elif data_type.startswith(PREFIX_DATA):
                        self.__load_data(data, data_info.strip())
                    elif data_type.startswith(PREFIX_PATTERNS):
                        dom_level = data_info.split()[0]
                        self.__load_pattern(data, int(dom_level))

                self.__log.debug('Compiling patterns')
                self.__compile_patterns()
                self.__log.debug('Data have been read')
                self.__next_read_ts = time.time() + self.__refresh_interval

        finally:
            self.__lock.release()

    def __compile_expression(self, expression):
        """Compile registrar patter expression"""
        # Dataset checker.
        if expression.startswith('%in('):
            data_name = expression[4:-1]
            return lambda x: x in self.__data[data_name]

        if expression == '$':
            return lambda x: True

        # Regex checker.
        if expression.startswith('%re('):
            re_exp = re.compile(expression[4:-1])
            return lambda x: bool(re_exp.match(x))

        # By default we think it is just string pattern.
        return lambda x: x == expression

    def __compile_patterns(self):
        """Compile all registrar patterns."""
        for level, patterns in self.__patterns.iteritems():
            compiled_patterns = []

            for pattern in patterns:
                c_pattern = []
                for p_expression in pattern:
                    c_pattern.append(self.__compile_expression(p_expression))

                compiled_patterns.append(c_pattern)

            self.__patterns[level] = compiled_patterns

    def __load_pattern(self, data, level):
        """Load all registrar patterns from file."""
        pattern_list = self.__patterns.setdefault(level, [])
        for pattern in data.split('\n'):
            pattern = pattern.strip()
            if pattern and not pattern.startswith('#'):
                pattern = pattern.split('/')
                if level != len(pattern):
                    raise ValueError(
                        'Incorrect pattern data for level %s: %s' %
                                     (level, pattern))
                pattern_list.append(pattern)

    def __load_data(self, data, data_name):
        """Load set data"""
        all_data = []
        for data_line in data.split('\n'):
            data_line = data_line.strip()
            if data_line and not data_line.startswith('#'):
                all_data.extend(data_line.split())
        data = set(all_data)
        self.__data[data_name] = data
        return data

    def set_custom_domains(self, domains):
        """Allow manual extension of registrar data from outside

        It requires list of TLDs on input. All of them will be parsed and
        stored by domain levels in__custom_domains dict.
        """
        assert isinstance(domains, list)
        self.__custom_domains = {}
        for domain in domains:
            level = len(domain.split('.'))
            self.__custom_domains.setdefault(level, set()).update(domains)

    def __load_domain(self, data, level, data_name):
        """Load domain data."""
        data = self.__load_data(data, data_name)
        self.__domains.setdefault(level, set()).update(data)
