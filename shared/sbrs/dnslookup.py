"""Use DNS to lookup SBRS scores.

During the porting to Python 2.6 we've made the following changes
which break compatibility with existing IronPort products:

    - parseSbrsScoreAndRules renamed to parse_sbrs_score_and_rules,
    - Agent renamed to LookupAgent, its method names changed too.

:Status: $Id: //prod/main/_is/shared/python/sbrs/dnslookup.py#8 $
:Authors: jwescott
"""

import DNS

from shared.net import iputils


def __compat_check():
    """There seems to be a non-standard set of DNS module
    installations, this is to ensure that we don't nuke
    old systems with the updated module.
    """
    resolvers = DNS.Base.defaults['server']
    if not resolvers:
        DNS.DiscoverNameServers()

__compat_check()


class SbrsDnsLookupError(DNS.DNSError):
    """Main exception class for SBRS DNS lookup errors."""
    pass


DEFAULT_QUERY_DOMAIN = 'rf-internal.senderbase.org'
DEFAULT_QUERY_VERSION = 'v1x2s'

SBRS_SCORE_INDEX = '0'
SBRS_RULES_INDEX = '7'


class LookupAgent(object):

    """Class for performing DNS lookup for SBRS results."""

    def __init__(self, query_domain=DEFAULT_QUERY_DOMAIN,
                 timeout=5, server=None,
                 query_version=DEFAULT_QUERY_VERSION):
        """Initialize Agent instance.

        :param query_domain: SenderBase query domain.
        :param timeout: Timeout for DNS requests.
        :param server: Server or list of DNS servers.
                       If None, default system servers will be used.
        :param query_version: SenderBase query version.
        """
        self.__timeout = timeout
        self.__server = server
        self.__query_domain = query_domain
        self.__query_version = query_version

    def __dns_request(self, record):
        """Helper for performing DNS request.

        :param record: DNS-query for request.
        :return: DnsResult object with response.
        """
        kwargs = {'name': record,
                  'qtype': DNS.Type.TXT,
                  'timeout': self.__timeout, }

        if self.__server:
            kwargs['server'] = self.__server

        return DNS.Request().req(**kwargs)

    def get_sbrs(self, ip, query_domain=None, query_version=None,
                 silent=True, score_only=False):
        """Get raw SBRS record for IP.

        :param ip: String with IP in dotted quad format.
        :param query_domain: Domain for rdns query.
        :param query_version: Version for rdns query.
        :param silent: If True, return None in case of any error.
        :param score_only: Get only SBRS.
        :raise SbrsDnsLookupError: If error happened and 'silent' is False.
        :return: String with SBRS record.
        """
        query_domain = query_domain or self.__query_domain
        query_version = query_version or self.__query_version

        if ':' in ip:
            ip_part = '%x' % iputils.ipv6_to_unsigned_int(ip)
        else:
            ip_part = '.'.join(reversed(ip.split('.')))

        if not score_only and query_version:
            query_domain = '%s.%s' % (query_version, query_domain)

        record = '%s.%s' % (ip_part, query_domain)

        try:
            request = self.__dns_request(record)
            if request.answers:
                return request.answers[0]['data'][0]
        except DNS.DNSError, err:
            if not silent:
                raise SbrsDnsLookupError(str(err))

        return None

    def get_score(self, ip, query_domain=None, query_version=None,
                   silent=True):
        """Get SBRS for a single IP.

        :param ip: String with IP in dotted quad format.
        :param query_domain: Domain for rdns query.
        :param query_version: Version for rdns query.
        :param silent: If True, return None in case of any error.
        :raise SbrsDnsLookupError: If error happened and 'silent' is False.
        :return: Float with score value.
        """
        score = self.get_sbrs(ip,
                              query_domain=query_domain,
                              query_version=query_version,
                              silent=silent,
                              score_only=True)

        if score:
            return float(score)

        return score

    def get_scores(self, ips, query_domain=None, query_version=None,
                   silent=True):
        """Get SBRS for set of IPs.

        :param ips: List with strings of IPs in dotted quad format.
        :param query_domain: Domain for rdns query.
        :param query_version: Version for rdns query.
        :param silent: If True, return None in case of any error.
        :raise SbrsDnsLookupError: If error happened and 'silent' is False.
        :return: Dictionary with IPs as keys and scores as values.
        """
        sbrs_scores = dict()
        for ip in ips:
            sbrs_scores[ip] = self.get_score(ip,
                                             query_domain=query_domain,
                                             query_version=query_version,
                                             silent=silent)
        return sbrs_scores


def __parse_sbrs(answer):
    """Helper for parsing SBRS answer.

    :param answer: String with raw SBRS answer.
    :return: Dictionary with SBRS field names as keys
             and field values as values.
    """
    fields_list = answer.split('|')
    sbrs_data = dict()

    for field in fields_list:
        if field.find('=') != -1:
            label, value = field.split('=')
            sbrs_data[label] = value

    return sbrs_data


def __get_sbrs_rules(sbrs_data):
    """Helper for grabbing SBRS rule names.

    :param sbrs_data: Dictionary with SBRS field names and values.
    :return: List with rule names.
    """
    rules_string = sbrs_data.get(SBRS_RULES_INDEX)
    if not rules_string:
        return None

    rules_string_len = len(rules_string)
    rules_list = list()

    if rules_string_len % 3 == 0:
        for i in range(0, rules_string_len, 3):
            rules_list.append(rules_string[i:i + 3])
    else:
        raise ValueError('Misconfigured rule string')

    return rules_list


def parse_sbrs_score_and_rules(answer, rule_filter=None):
    """Parse raw SBRS answer to get spam score and rule names.

    :param answer: String with SBRS DNS response.
    :param rule_filter: List of rule names (return only rules from this list).
    :returns: Dictionary with 'spam_score' (float) and 'rules' (list).
    :raise ValueError: If rule field has invalid format.
    """
    if not answer:
        return None
    sbrs_data = __parse_sbrs(answer)
    result = dict()

    spam_score = sbrs_data.get(SBRS_SCORE_INDEX)
    if spam_score:
        spam_score = float(spam_score)
    result['spam_score'] = spam_score

    rules = __get_sbrs_rules(sbrs_data)
    if rule_filter:
        rules = filter(lambda rule: rule in rule_filter, rules)
    result['rules'] = rules

    return result

# EOF
