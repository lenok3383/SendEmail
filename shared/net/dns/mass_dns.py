"""Utilities for performing DNS lookups.  Requires the py-adns port to be
installed.

:Status: $Id: //prod/main/_is/shared/python/net/dns/mass_dns.py#3 $
:Authors: aflury, vburenin
"""

import adns
import time


QUERY_TYPES = {
    'A': adns.rr.A,
    'CNAME': adns.rr.CNAME,
    'NS': adns.rr.NS,
    'PTR': adns.rr.PTR,
    'SOA': adns.rr.SOA,
    'TXT': adns.rr.TXT,
    }


class TimeoutError(Warning):

    """Returned as a query result when a lookup times out."""

    pass


class DNSQueryObject(object):

    """Query data container."""

    __slots__ = ['client', 'end_time', 'dns_name']

    def __init__(self, client, end_time, dns_name):
        """Constructor.

        :param client: Query object.
        :param end_time: Time after which query will be treated as timeouted.
        :param dns_name: DNS name to resolve.
        """
        self.client = client
        self.end_time = end_time
        self.dns_name = dns_name


class MassQuery(object):

    """Class to perform mass parallel DBS queries."""

    def __init__(self, timeout, nameservers=None, batch_limit=500):
        """Constructor.

        :param timeout: Max time to wait response.
        :param nameservers: List of custom DNS Servers. It can be one
               DNS server address string, or list of DNS servers.
        :param batch_limit: Maximal limit of parallel queries per call.
        """
        self.__timeout = timeout
        self.__batch_limit = batch_limit

        self.__output = open('/dev/null', 'w+')

        if nameservers:
            if isinstance(nameservers, str):
                nameservers = [nameservers]
            resolve_conf = ''
            for nameserver in nameservers:
                resolve_conf += 'nameserver %s\n' % (nameserver,)
            self.__resolver = adns.init(0, self.__output, resolve_conf)
        else:
            self.__resolver = adns.init(0, self.__output)

    def resolve(self, names, query_type):
        """Resolve bunch of domain names.

        :param names: List of domain names to resolve.
        :param query_type: DNS query type. Allowed values are keys from
               QUERY_TYPES dictionary.
        :return: Generator over DNS responses in format: (name, result)
        """
        query_type = QUERY_TYPES[query_type.upper()]
        queries = []
        for qname in names:
            qname = qname.strip()
            qobj = DNSQueryObject(self.__resolver.submit(qname, query_type),
                                  time.time() + self.__timeout, qname)
            queries.append(qobj)
            while len(queries) > self.__batch_limit:
                for result in self.__check_queries(queries):
                    yield result

        # Wait for remaining queries to finish/timeout.
        while queries:
            for result in self.__check_queries(queries):
                yield result

    def __check_queries(self, queries):
        """Walks through awaiting pool of request to return request results."""

        # Don't be too much of a CPU hog.
        time.sleep(0.1)
        i = 0
        while i < len(queries):
            qobj = queries[i]
            try:
                yield (qobj.dns_name, qobj.client.check())
                del queries[i]
            except adns.NotReady, err:
                if qobj.end_time > time.time():
                    i += 1
                else:
                    del queries[i]
                    qobj.client.cancel()
                    yield (qobj.dns_name, TimeoutError(err))
