"""Implementation of MultiCast Database Queue.

DBMC Queue allows you to have many data publishers to put some data to the queue.
Subscribers can get subscription for that queue to read data strongly
in order as data appears in that queue. It also guarantees that each subscriber
will get unique piece of data from the queue.

http://eng.ironport.com/docs/is/common/dbqueue.rst

:Status: $Id: //prod/main/_is/shared/python/dbqueue/__init__.py#3 $
:Author: vburenin
"""
