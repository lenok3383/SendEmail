"""System for allowing decorated methods to register themselves in a class.

This module provides a system to allow decorators on methods to provide
information to the class.  See the example for more information.

:Author: duncan, ereuveni
:Status: $Id: //prod/main/_is/shared/python/diablo/decorator_registrar.py#6 $
"""

def bind(func, obj):
    """Binds the function to an object.

    This is used when we have a function, and we really want a bound method.
    """
    # This is deep magic.  To fully understand this, you have to understand
    # descriptors.  Otherwise, all you need to know is that this is exactly how
    # bound methods are created under the hood.
    return func.__get__(obj, type(obj))


class DecoratorRegistrar(type):

    """Collects data from function attributes and stores as class variables.

    This metaclass looks for members with a '__registry' attribute. Any
    function with an __registry attribute is signalling that it has data that
    should be added to a class variable.

    Example:

    class MyClass(object):

        __metaclass__ = DecoratorRegistrar

        def my_func(self, *args, **kwargs):
            pass
        DecoratorRegistrar.register('FOO', my_func, ('attr1', 'attr2'))

        def other_func(self, *args, **kwargs):
            pass
        DecoratorRegistrar.register('FOO', other_func, ('attrA', 'attrB'))

    Then the resulting class will have the 'FOO' class attribute set:

        MyClass.FOO == [(<function my_func at 0x105678, ('attr1', 'attr2')),
                        (<function other_func at 0x101234, ('attrA', 'attrB'))]

    Note that metaclasses are inherited, so subclasses of a class that uses
    this metaclass do not need to register the metaclass directly.
    """

    def __new__(mcs, name, base_classes, class_dictionary):
        """Do the decorator registration.

        This is called at class creation time, before the class object is
        created.
        """

        added_class_members = dict()

        # Iterate over the future class's dictionary for methods that have been
        # specially decorated.  Add them to their proper registration, and note
        # the registration in the __registry of the future class.

        for class_member in class_dictionary.itervalues():
            try:
                registration_dict = class_member.__registry
            except AttributeError:
                continue

            for registration_name, registration_info in \
                    registration_dict.iteritems():
                added_class_members.setdefault(registration_name, []).append(
                        (class_member, registration_info))

        # Combine the new attributes with attributes added to parent classes,
        # even if we don't add them to the current class.  (We can not rely on
        # inheritance because we want to combine values from multiple parents
        # instead of replacing.)

        for base_class in base_classes:
            try:
                added = base_class.__added_members
            except AttributeError:
                continue
            for added_member_name in added:
                added_class_members.setdefault(added_member_name, []).extend(
                    getattr(base_class, added_member_name))

        # Now add our new members to the class.  Keep track of the added
        # members in __added_members.

        class_dictionary.update(added_class_members)
        class_dictionary['_DecoratorRegistrar__added_members'] = \
                tuple(added_class_members)

        return super(DecoratorRegistrar, mcs).__new__(
            mcs, name, base_classes, class_dictionary)

    @staticmethod
    def register(registry, func, attrs):
        """Registers a method.

        Assuming DecoratorRegistrar is the metaclass, the class containing this
        method will have a class variable ``registry``.
        """
        try:
            func.__registry[registry] = attrs
        except AttributeError:
            func.__registry = {registry: attrs}
