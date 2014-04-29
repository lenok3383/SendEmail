"""Unit test for Decorator Registrar functionality

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/test/test_decorator_registrar.py#4 $
"""

import logging
import unittest2 as unittest

from shared.diablo.decorator_registrar import DecoratorRegistrar

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)

def register_method(val):
    """Decorator to use on method to register it"""

    def decorator(func):
        DecoratorRegistrar.register('_REGISTERED', func, (func.func_name, val))
        return func

    return decorator

class RegistrarClass(object):
    __metaclass__ = DecoratorRegistrar
    _REGISTERED = ()

    @register_method(99)
    def foo(self):
        return 42

    @register_method(66)
    def boo(self):
        return 666

class TestRegistrar(unittest.TestCase):

    def test_reg(self):
        """Ensure each method was registered with the correct values"""
        self.assertEqual(len(RegistrarClass._REGISTERED), 2)
        rc = RegistrarClass()

        (func, (name, val)) = RegistrarClass._REGISTERED[0]
        self.assertEqual(func(rc), 42)
        self.assertEqual(name, 'foo')
        self.assertEqual(val, 99)

        (func, (name, val)) = RegistrarClass._REGISTERED[1]
        self.assertEqual(func(rc), 666)
        self.assertEqual(name, 'boo')
        self.assertEqual(val, 66)

if __name__ == '__main__':
    unittest.main()
