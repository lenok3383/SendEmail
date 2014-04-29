"""Unit tests for decoratorutils.

:Authors: scottrwi
:Status: $Id: //prod/main/_is/shared/python/util/test/test_decoratorutils.py#4 $
"""

import unittest2 as unittest

import shared.util.decoratorutils

no_args = None
gave_args = None


@shared.util.decoratorutils.allow_no_args_form
def store_args(arg=1):

    def decorator(func):
        global no_args
        no_args = arg

        return func

    return decorator


@shared.util.decoratorutils.allow_no_args_form
def store_args_2(arg=1):

    def decorator(func):
        global gave_args
        gave_args = arg

        return func

    return decorator


class TestAllowNoArgs(unittest.TestCase):

    @store_args
    def test_no_args(self):

        self.assertEqual(no_args, 1)

    @store_args_2(99)
    def test_with_args(self):
        self.assertEqual(gave_args, 99)


if __name__ == '__main__':
    unittest.main()

