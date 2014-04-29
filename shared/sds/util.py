"""
Helpers to deal with SDS responses.

:author: vkuznets
"""

import functools
import logging


class extract_sds_response:
    """Wraps function which returns result in format of
       SDSClient.query() method. e.g.

          {'google.com': [{u'elements': [u'google.com'],
                           u'meta': {u'cache': u'google.com',
                           u'ttl': 3600},
                           u'response': {u'webcat': {u'cat': 1020}}}]}

       Extracts 'response' value and constructs result dictionary in format
       {'google.com': <response data>}
    """

    def __init__(self, parse_response_func):
        """Constructor.

        :param parse_response_func:  function which takes 'response' field of
                                       SDS result dictionary and extracts useful
                                       data. Example of Web category extractor
                                       function:

                                        def web_category_extractor(resp):
                                            cat = resp['webcat']['cat']
                                            if cat == u'nocat':
                                                return None
                                            return cat
        """
        self._parse_response_func = parse_response_func
        self._log = logging.getLogger(self.__class__.__name__)


    def __call__(self, func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            sds_res = func(*args, **kwargs)
            ret = {}

            for query_item, (data,) in sds_res.iteritems():
                if 'exception' in data:
                    self._log.warning('%s: %s', query_item, data['exception'])
                    ret[query_item] = None
                    continue

                sds_response = data.get('response')
                if not sds_response:
                    ret[query_item] = None
                    continue

                ret[query_item] = self._parse_response_func(sds_response)

            return ret

        return decorator


def web_category_extractor(resp):
    """Extractor function for '/score/webcat' response."""

    cat = resp['webcat']['cat']
    if cat == u'nocat':
        return None
    return cat


def wbrs_score_extractor(resp):
    """Extractor function for '/score/wbrs' response."""

    score = resp['wbrs']['score']
    if score == u'noscore':
        return None
    return score


extract_web_categories = extract_sds_response(web_category_extractor)
extract_wbrs_scores = extract_sds_response(wbrs_score_extractor)
