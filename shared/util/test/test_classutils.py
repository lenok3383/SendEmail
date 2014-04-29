"""Unit test for class utils.

:Status: $Id: //prod/main/_is/shared/python/util/test/test_classutils.py#7 $
:Author: bwhitela
"""

import unittest2

import shared.util.classutils as classutils


class DummyClass(object):
    """Dummy class for testing."""
    def __init__(self, a=None, b=None):
        self.a = a
        self.b = b


class TestClassutils(unittest2.TestCase):

    """Unit tests for shared classutils."""

    def test_get_by_name(self):
        """Test the get_class_by_name function with the above DummyClass."""
        import_path = 'shared.util.test.test_classutils.DummyClass'
        imported_class = classutils.get_class_by_name(import_path)
        temp_class = imported_class(1, 2)
        self.assertEqual(temp_class.a, 1)
        self.assertEqual(temp_class.b, 2)

        bad_import_path = 'shared.util.test.test_classutils.BadClass'
        self.assertRaises(classutils.ClassLoadingError,
                          classutils.get_class_by_name, bad_import_path)

    def test_get_fqn(self):
        """Test the get_fqn_of_class function with the above DummyClass."""
        import shared.util.test.test_classutils as import_path
        self.assertEqual(classutils.get_fqn_of_class(import_path.DummyClass),
                         'shared.util.test.test_classutils.DummyClass')

    def test_new_instance(self):
        """Test the new_instance function with the above DummyClass."""
        dummy_class = DummyClass
        temp_class = classutils.new_instance(dummy_class, "{'a': 1, 'b': 2}")
        self.assertEqual(temp_class.a, 1)
        self.assertEqual(temp_class.b, 2)

        temp_class = classutils.new_instance(dummy_class, '')
        self.assertEqual(temp_class.a, None)
        self.assertEqual(temp_class.b, None)

    def test_make_struct(self):
        """Test if structure is created correctly."""
        cool_struct = classutils.make_struct('CoolClass', ['a', 'b', 'c'])
        obj = cool_struct('a\n', 2, 3)
        self.assertEqual('a\n', obj.a)
        self.assertEqual(2, obj.b)
        self.assertEqual(3, obj.c)
        self.assertEqual('CoolClass', cool_struct.__name__)
        self.assertEqual('a=a\n, b=2, c=3', str(obj))
        self.assertEqual('a=\'a\\n\', b=2, c=3', repr(obj))

    def test_make_struct_compare(self):
        """Test that __eq__ method of created objects works well."""
        cool_struct = classutils.make_struct('CoolClass', ['a', 'b', 'c'])
        obj_a = cool_struct(1, 2, 3)
        obj_b = cool_struct(1, 2, 3)
        obj_c = cool_struct(1, 2, 4)

        self.assertTrue(obj_a == obj_b)
        self.assertTrue(obj_a != obj_c)
        self.assertFalse(obj_a != obj_b)
        self.assertFalse(obj_a == obj_c)
        self.assertFalse(obj_b == obj_c)
        self.assertFalse(obj_b == 1)
        self.assertFalse(obj_b == 'some string')
        self.assertFalse(obj_b == 1000.0)


if __name__ == '__main__':
    unittest2.main()
