import DNS
import unittest2 as unittest

from shared.sbrs import dnslookup


SBRS_DATA_MOCKUP = {
    '208.111.169.146' :
        {'raw' : '|0=4.1|1=0.0|2=0.2892|3=0.5|4=-797988462|'\
                 '5=613052499343050752|6=NA|7=AvNCmLDhLIaNTrLWh2|',
         'score' : '4.1',
         'reversed_ip' : '146.169.111.208'},
    '184.154.56.42' :
        {'raw' : '|0=-9.9|1=0.0|2=0.9986|3=0.5|4=-1197852630|'\
                 '5=2310944745582755856|6=NA|7=CmMCu1DhLIaMIvmIvnPsbTrH|',
         'score' : '-9.9',
         'reversed_ip' : '42.56.154.184'},
    '88.196.174.199' :
        {'raw' : '|0=2.9|1=0.0|2=0.3779|3=0.5|4=1489284807|'\
                 '5=33619968|6=NA|7=CmNTrN|',
         'score' : '2.9',
         'reversed_ip' : '199.174.196.88'},
    '2002::1' :
        {'raw' : '|0=2.9|1=0.0|2=0.3779|3=0.5|4=1489284807|'\
                 '5=33619968|6=NA|7=CmNTrN|',
         'score' : '2.9',
         'reversed_ip' : '20020000000000000000000000000001'},
    '2002:1111:2222:3333:4444:5555:6666:ffff' :
        {'raw' : '|0=2.9|1=0.0|2=0.3779|3=0.5|4=1489284807|'\
                 '5=33619968|6=NA|7=CmNTrN|',
         'score' : '2.9',
         'reversed_ip' : '2002111122223333444455556666ffff'},
     '74.125.43.105' :
        {'raw' : None,
         'score' : None,
         'reversed_ip' : '105.43.125.74'}
}

SBRS_QUERY_MOCKUP = {'%s.%s' % (dnslookup.DEFAULT_QUERY_VERSION,
                                dnslookup.DEFAULT_QUERY_DOMAIN) : 'raw',
                     'v1x2v.rf-internal' : 'raw',
                     dnslookup.DEFAULT_QUERY_DOMAIN : 'score',
                     'rf-internal' : 'score'}


SBRS_ANSWER_MOCKUP = {'|0=1.8|1=0.0|2=0.42|3=0.5|4=-797988462|'\
                      '5=649644246315436032|6=NA|7=AvLCmLDhMIaNTrLWh2|' :
                            {'spam_score' : 1.8, 'rules' : ['AvL', 'CmL',
                                                            'DhM', 'IaN',
                                                            'TrL', 'Wh2']},
                      '|0=-0.4|1=0.0|2=0.6651|3=0.5|4=-935626811|'\
                      '5=4648277765433393152|6=NA|7=AvNCmNDhLIaHTrN|' :
                            {'spam_score' :-0.4, 'rules' : ['AvN', 'CmN',
                                                            'DhL', 'IaH',
                                                            'TrN']}
                     }


class DNSRequestMockup(object):

    def __init__(self):
        self.defaults = {'servers' : None}
        self.TXT = ''

    def req(self, name=None, server=None, **kwargs):

        # Just a way for modeling exception raising.
        if server:
            if server.find('.') == -1:
                raise DNS.DNSError('Timeout')

        # Return answer based on query content
        for mock in SBRS_DATA_MOCKUP.itervalues():
            if mock['reversed_ip'] in name:
                for query, query_type in SBRS_QUERY_MOCKUP.iteritems():
                    if name == '%s.%s' % (mock['reversed_ip'], query):
                        return DNSResultMockup(mock[query_type])

        # Return empty answer
        return DNSResultMockup()


class DNSResultMockup(object):

    def __init__(self, value=None):
        if value:
            self.answers = [{'data' : [value, ]}, ]
        else:
            self.answers = []


class DNSMockup(object):

    def __init__(self):
        self.Base = DNSRequestMockup()
        self.Type = DNSRequestMockup()
        self.DNSError = DNS.DNSError
        self.Request = DNSRequestMockup

    def DiscoverNameServers(self):
        pass


class TestDNSLookup(unittest.TestCase):

    def setUp(self):
        self.__orig_dns = dnslookup.DNS
        dnslookup.DNS = DNSMockup()
        self.__agent = dnslookup.LookupAgent()

    def tearDown(self):
        dnslookup.DNS = self.__orig_dns

    def test_get_sbrs_default(self):
        # Test get_sbrs with default domain and version
        for ip, mock in SBRS_DATA_MOCKUP.iteritems():
            self.assertEquals(mock['raw'], self.__agent.get_sbrs(ip))

    def test_get_sbrs_with_params(self):
        ip = '184.154.56.42'
        mock = SBRS_DATA_MOCKUP[ip]

        # Test get_sbrs with specified domain and version
        self.assertEquals(mock['raw'], self.__agent.get_sbrs(ip,
                                        query_domain='rf-internal',
                                        query_version='v1x2v'))

        # Test get_sbrs with domain and version, passed to __init__ method
        agent = dnslookup.LookupAgent(query_domain='rf-internal',
                                      query_version=None)
        self.assertEquals(mock['score'], agent.get_sbrs(ip))

        # Test get_sbrs with specified domain and version, also passed to
        # __init__ method
        self.assertEquals(mock['raw'], agent.get_sbrs(ip,
                                        query_domain='rf-internal',
                                        query_version='v1x2v'))

    def test_get_sbrs_error(self):
        # Test error raising
        agent = dnslookup.LookupAgent(server='ababa')
        self.assertRaises(dnslookup.SbrsDnsLookupError,
                          agent.get_sbrs, '', silent=False)

    def test_get_score_default(self):
        # Test get_score with default domain and version
        for ip, mock in SBRS_DATA_MOCKUP.iteritems():
            if mock['score']:
                self.assertEquals(float(mock['score']),
                                  self.__agent.get_score(ip))
            else:
                self.assertEquals(None,
                                  self.__agent.get_score(ip))

    def test_get_score_with_params(self):
        ip = '208.111.169.146'
        mock = SBRS_DATA_MOCKUP[ip]

        # Test get_score with specified domain and version
        # (although it must ignore version in any case)
        self.assertEquals(float(mock['score']),
                          self.__agent.get_score(ip,
                                                query_domain='rf-internal',
                                                query_version='v1x2v'))

    def test_get_ipv6_score_with_params_1(self):
        ip = '2002::1'
        mock = SBRS_DATA_MOCKUP[ip]

        # Test get_score with specified domain and version
        # (although it must ignore version in any case)
        self.assertEquals(float(mock['score']),
                          self.__agent.get_score(ip,
                                                query_domain='rf-internal',
                                                query_version='v1x2v'))

    def test_get_ipv6_score_with_params_2(self):
        ip = '2002:1111:2222:3333:4444:5555:6666:ffff'
        mock = SBRS_DATA_MOCKUP[ip]

        # Test get_score with specified domain and version
        # (although it must ignore version in any case)
        self.assertEquals(float(mock['score']),
                          self.__agent.get_score(ip,
                                                query_domain='rf-internal',
                                                query_version='v1x2v'))

    def test_get_scores_default(self):
        # Test get_scores with default domain and version
        answer = self.__agent.get_scores(SBRS_DATA_MOCKUP.keys())
        for ip, score in answer.iteritems():
            if score:
                self.assertEquals(score, float(SBRS_DATA_MOCKUP[ip]['score']))
            else:
                self.assertEquals(score, SBRS_DATA_MOCKUP[ip]['score'])

    def test_get_scores_with_params(self):
        # Test get_scores with specified domain and version
        answer = self.__agent.get_scores(SBRS_DATA_MOCKUP.keys(),
                                         query_domain='rf-internal',
                                         query_version='v1x2v')
        for ip, score in answer.iteritems():
            if score:
                self.assertEquals(score, float(SBRS_DATA_MOCKUP[ip]['score']))
            else:
                self.assertEquals(score, SBRS_DATA_MOCKUP[ip]['score'])

    def test_parse_sbrs_score_and_rules(self):
        # Test parse_sbrs_score_and_rules with correct answer
        for answer, parse_answer in SBRS_ANSWER_MOCKUP.iteritems():
            score_and_rules = dnslookup.parse_sbrs_score_and_rules(answer)
            self.assertEquals(score_and_rules['spam_score'],
                              parse_answer['spam_score'])
            self.assertEquals(score_and_rules['rules'],
                              parse_answer['rules'])

    def test_parse_sbrs_score_and_rules_filter(self):
        # Test parse_sbrs_score_and_rules with 'rule_filter' parameter
        rule_filter = ['AvL', 'AvN']
        for answer in SBRS_ANSWER_MOCKUP:
            score_and_rules = dnslookup.parse_sbrs_score_and_rules(answer,
                                                    rule_filter=rule_filter)
            for rule in score_and_rules['rules']:
                self.assertTrue(rule in rule_filter)

    def test_parse_sbrs_score_and_rules_error(self):
        # Test parse_score_and_rules with incorrect answer
        self.assertRaises(ValueError,
                          dnslookup.parse_sbrs_score_and_rules,
                          '0=1|7=AsdAsdAs')

        # Test parse_sbrs_score_and_rules with no score field
        self.assertEquals(dnslookup.parse_sbrs_score_and_rules(
                                        '1=2|7=AddSdd')['spam_score'], None)

        # Test parse_sbrs_score_and_rules with no rules field
        self.assertEquals(dnslookup.parse_sbrs_score_and_rules(
                                        '0=2|2=AddSdd')['rules'], None)


if __name__ == '__main__':
    unittest.main()
