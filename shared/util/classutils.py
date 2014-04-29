"""Useful tools for class manipulations.

:Status: $Id: //prod/main/_is/shared/python/util/classutils.py#9 $
:Author: ted
"""
import textwrap


class ClassLoadingError(Exception):
    def __init__(self, class_fqn):
        Exception.__init__(self, "Cannot load class '%s'" % class_fqn)


def get_class_by_name(class_fqn):
    """Return a class based on the fully qualified class name string.

    Note: We assume the last '.' is the class name.

    :param class_fqn: The fully qualified class name as a string.
    :return: The class.
    """
    try:
        l = class_fqn.split('.')
        module_name = '.'.join(l[:-1])
        class_name = l[-1]
        mod = __import__(module_name, {}, {}, [class_name])
        return getattr(mod, class_name)
    except (ImportError, AttributeError):
        raise ClassLoadingError(class_fqn)


def get_fqn_of_class(klass):
    """Get the fully qualified class name of the given klass."""
    return '%s.%s' % (klass.__module__, klass.__name__,)


def new_instance(klass, kwargs_str):
    """Create an new instance of the given class by the given kwargs string.

    :param klass: The class.
    :param kwargs_str: A string of the dictionary of keyword arguments to
    initialize the class.
    """
    if kwargs_str and \
            kwargs_str.startswith('{') and \
            kwargs_str.endswith('}'):
        kwargs = eval(kwargs_str)
    else:
        kwargs = {}
    return klass(**kwargs)


def make_struct(class_name, fields):
    """Creates structure for you.

    Can be used to replace approach with dictionary to store some data in
    a class object with predefined __slots__. In many cases it is better
    than named tuple. All object members are modifiable, access to them
    is 3.5 times faster. Can be very helpful and fast in massive
    data processing.

    :param class_name: Name of the class.
    :param fields: Fields list.
    """
    cargs = ','.join(fields)
    self_fields = ','.join(['self.' + f for f in fields])
    str_fields = ','.join(['str(self.%s)' % (f,) for f in fields])
    repr_fields = ','.join(['repr(self.%s)' % (f,) for f in fields])
    str_form = ', '.join([f + '=%s' for f in fields])
    and_form = ' and '.join(['self.%s == other.%s' % (f, f) for f in fields])

    tmpl = textwrap.dedent("""\
        class %(cname)s(object):
            __slots__ = %(slots)s
            def __init__(self, %(cargs)s):
                %(self_fields)s = %(cargs)s
            def __str__(self):
                return '%(str_form)s' %% (%(str_fields)s)
            def __repr__(self):
                return '%(str_form)s' %% (%(repr_fields)s)
            def __eq__(self, other):
                if not isinstance(other, %(cname)s):
                    return NotImplemented
                return %(and_form)s
            def __ne__(self, other):
                return not self.__eq__(other)
        """) % {'cname': class_name,
                'slots': fields,
                'cargs': cargs,
                'self_fields': self_fields,
                'str_form': str_form,
                'str_fields': str_fields,
                'repr_fields': repr_fields,
                'and_form': and_form}
    d = {}
    exec tmpl in d
    return d[class_name]
