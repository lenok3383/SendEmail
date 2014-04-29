"""Mocks for pycurl.Curl and pycurl.CurlMulti.

:Status: $Id: //prod/main/_is/shared/python/sds/test/pycurl_mockup.py#1 $
:Author: aianushe
:Last Modified By: $Author: vkuznets $
"""

import cStringIO
import pycurl


class CurlMockup(object):

    """Mock for pycurl.Curl class."""

    def __init__(self):
        self.data_dict = {}

        self.body = cStringIO.StringIO()
        self.headers = cStringIO.StringIO()
        self._http_code = None
        self.options = {}
        self.query_item = None


    def getinfo(self, what):
        if what == pycurl.HTTP_CODE:
            return self._http_code

    def setopt(self, option, value):
        self.options[option] = value

    def close(self):
        self.body = cStringIO.StringIO()
        self.headers = cStringIO.StringIO()
        self._http_code = None
        self.options = {}

    def perform(self):
        url = self.options[pycurl.URL]
        self.body.write(CurlMockup.data_dict[url]['body'])
        self.headers.write(CurlMockup.data_dict[url]['headers'])
        self._http_code = 200

    @staticmethod
    def set_data(data_dict):
        """Set CurlMockup data."""
        CurlMockup.data_dict = data_dict


class CurlMultiMockup():

    """Mock for pycurl.CurlMulti class."""

    def __init__(self):
        self.handles = []
        self.curled_number = 0

    def select(self, timeout):
        return self.curled_number

    def add_handle(self, handle):
        self.handles.append(handle)

    def remove_handle(self, handle):
        if len(handle.headers.getvalue()):
            self.curled_number -= 1
        self.handles.remove(handle)

    def close(self):
        self.handles = []
        self.curled_number = 0

    def perform(self):
        handles_num = len(self.handles)

        if self.curled_number < handles_num:
            self.handles[self.curled_number].perform()
            self.curled_number += 1
            return -1, handles_num - self.curled_number
        else:
            return 0, 0

    def info_read(self):
        return (len(self.handles) - self.curled_number,
               self.handles[:self.curled_number],
               [])

