"""Unittests for IPAS Header parsers.

:Author: rbodnarc
:Version: $Id: //prod/main/_is/shared/python/data/test/test_tracker.py#6 $
"""

import unittest2 as unittest

from shared.data.tracker import TrackerHeader, IpasHeaderError


HEADERS = {'v1' : {'header' : 'AKLwAH+dkkOBM4JTJ4J',
                   'header_version' : 0,
                   'ip' : None,
                   'sbrs' : None,
                   'profile' : 'global',
                   'vof_score' : 0.0,
                   'spam_score' : 94.0,
                   'packages_version' : '20051204_074047',
                   'packages_version_int' : 1133682047,
                   '__call__' : (179, 518, 557)},

           'v2' : {'header' : 'Aj0KAAAAAACsFZE9/05vcn' \
                              'RoQW1lcmljYQCDfSyCA0o',
                   'header_version' : 2,
                   'ip' :-1407872707,
                   'sbrs' : None,
                   'profile' : 'NorthAmerica',
                   'vof_score' : 0.0,
                   'spam_score' : 4.0,
                   'packages_version' : '19700101_000000',
                   'packages_version_int' : 0,
                   '__call__' : (509, 553, 812, 886)},
           'v3' : [{'header' : 'A8j///9NST1LJrgNASCIrYyAAIKULpvB5jQegjaDA4'
                               'tiAY0GATUBAQEBAQERCgoIEAaQDpYwhyqFcQQ',
                    'header_version' : 3,
                    'ip' : None,
                    'ipv6' : 42540766452641154071740215577757643572L,
                    'sbrs' :-6.2,
                    'profile' : 'zh',
                    'vof_score' : 5,
                    'spam_score' : 100,
                    'packages_version' : '20100101_010101',
                    'packages_version_int' : 1262307661,
                    'is_outbound' : False,
                    'is_internal_relay' : False,
                    '__call__' : (30, 340, 727, 2233, 2234, 3904, 3905, 3958,
                                  3959, 3960, 3961, 3962, 3963, 3964, 3981,
                                  3991, 4001, 4009, 4025, 4031, 6093, 8957,
                                  9895, 10648, 10652)},
                   {'header' : 'A2D///9NST1LJgEBAQGCVIMDi2IBjQYBNQEBAQEBAREKC'
                               'ggQBpAOljCHKoVxBA',
                    'header_version' : 3,
                    'ip' : 16843009,
                    'ipv6' : None,
                    'sbrs' :-6.2,
                    'profile' : 'global',
                    'vof_score' : 5,
                    'spam_score' : 100,
                    'packages_version' : '20100101_010101',
                    'packages_version_int' : 1262307661,
                    'is_outbound' : True,
                    'is_internal_relay' : False,
                    '__call__' : (340, 727, 2233, 2234, 3904, 3905, 3958, 3959,
                                  3960, 3961, 3962, 3963, 3964, 3981, 3991,
                                  4001, 4009, 4025, 4031, 6093, 8957, 9895,
                                  10648, 10652)},
                   {'header' : 'A7D///9NST1LirgNASCIrYyAAIJUjmUBjQYBPwEBAQoK'
                               'CggQBpAOljCHKoVxBA',
                    'header_version' : 3,
                    'ip' : None,
                    'ipv6' : 42540766452641154071740063647526813696L,
                    'sbrs' : 3.8,
                    'profile' : 'global',
                    'vof_score' : 5,
                    'spam_score' : 100,
                    'packages_version' : '20100101_010101',
                    'packages_version_int' : 1262307661,
                    'is_outbound' : True,
                    'is_internal_relay' : True,
                    '__call__' : (340, 2233, 2234, 3904, 3905, 3968, 3969,
                                  3970, 3971, 3981, 3991, 4001, 4009, 4025,
                                  4031, 6093, 8957, 9895, 10648, 10652)},
                    ],
           'incorect' : {'header' : '9gerfvsbfhrtdjhnb'}, }


class TreckerTest(unittest.TestCase):

    def test_container_emulating(self):
        header = HEADERS['v2']
        tracker = TrackerHeader(header['header'])
        mokup_rules = header['__call__']
        self.assertEquals(mokup_rules[0], tracker[0])
        self.assertEquals(mokup_rules[1:], tracker[1:])
        self.assertEquals(len(mokup_rules), len(tracker))
        for rule in tracker:
            self.assertTrue(rule in tracker)

    def test_basic_emulating(self):
        header = HEADERS['v2']
        tracker = TrackerHeader(header['header'])
        self.assertEquals(tracker, TrackerHeader(header['header']))
        self.assertEquals('<19700101_000000: (509, 553, 812, 886)>',
                          str(tracker))
        self.assertEquals('<19700101_000000: (509, 553, 812, 886)>',
                          repr(tracker))
        self.assertTrue(isinstance(hash(tracker), int))

    def test_v1_header(self):
        header = HEADERS['v1']
        tracker = TrackerHeader(header['header'])
        self.__test_tracker(header, tracker)

    def test_v2_header(self):
        header = HEADERS['v2']
        tracker = TrackerHeader(header['header'])
        self.__test_tracker(header, tracker)

    def test_v3_headers(self):
        headers = HEADERS['v3']
        for header in headers:
            tracker = TrackerHeader(header['header'])
            self.__test_tracker(header, tracker)

    def test_incorect_header(self):
        header = HEADERS['incorect']
        self.assertRaises(IpasHeaderError, TrackerHeader, header['header'])

    def __test_tracker(self, header, tracker):
        for field in header:
            if field.startswith('__'):
                self.assertEquals(set(getattr(tracker, field)()),
                                  set(header[field]))
            elif type(header[field]) == float:
                self.assertEquals(round(getattr(tracker, field), 2),
                                  round(header[field], 2))
            else:
                self.assertEquals(getattr(tracker, field),
                                  header[field])


if __name__ == '__main__':
    unittest.main()
