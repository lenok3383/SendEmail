"""Module with exception classes for Traffic Corpus Web API.

:Status: $Id: //prod/main/_is/shared/python/avc/traffic_corpus/errors.py#1 $
:Authors: usarfraz
"""

from shared.webapi import errors


class NoSuchHttpTransaction(errors.NotFoundError):
    """No such record(s) exists into http_transaction_* tables."""
    pass


class NoSuchLabel(errors.NotFoundError):
    """No such label into labels table."""
    pass


class NoSuchVersion(errors.NotFoundError):
    """No such version into versions table."""
    pass


class NoSuchApplication(errors.NotFoundError):
    """No such application into applications table."""
    pass


class NoSuchType(errors.NotFoundError):
    """No such type into app_types table."""
    pass


class NoSuchBehavior(errors.NotFoundError):
    """No such behavior into behaviors table."""
    pass


class NoSuchRecord(errors.NotFoundError):
    """No such record exist into a table."""
    pass


class RecordExists(errors.DuplicationError):
    """Record already exists into a table."""
    pass


class PoorlyConstructedQuery(errors.InvalidData):
    """"A query statement has wrong format"""
    pass


class CannotDelete(errors.WebAPIException):
    """Unable to remove specified record(s)."""
    pass


class CannotUpdate(errors.InvalidData):
    """Unable to update specified record(s)."""
    pass

