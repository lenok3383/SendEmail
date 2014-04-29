import inspect

class MockMatcher(object):

    """Each mock matcher must be inherited from this class.

    This type is used to distinguish different object from matchers.
    """

    def compare(self, param):
        """Check if matcher hits parameter value"""

        raise NotImplementedError('This method must be implemented')


class CustomMatcher(MockMatcher):

    """Use custom matcher for your custom use cases."""

    def __init__(self, matching_func):
        """Constructor.

        :param matching_func: Your custom function to compare incoming value.
        """
        MockMatcher.__init__(self)
        self.matching_func = matching_func

    def __str__(self):
        return str(self.matching_func)

    def __eq__(self, other):
        return isinstance(other, CustomMatcher) and \
               self.matching_func == other.matching_func

    def compare(self, param):
        """Check if matcher hits parameter value"""
        return self.matching_func(param)


class _AnyArgsMatcher(MockMatcher):

    """Specific class, that substitutes all the parameters to anything."""

    def __str__(self):
        return '<Accept any number arguments and any values>'

    def compare(self, param):
        return True


class _AnyMatcher(MockMatcher):

    def __str__(self):
        return '<Any value is accepted>'

    def __eq__(self, other):
        return isinstance(other, _AnyMatcher)

    def compare(self, param):
        return True


class _NoneMatcher(MockMatcher):

    def __str__(self):
        return '<None>'

    def __eq__(self, other):
        return isinstance(other, _NoneMatcher)

    def compare(self, param):
        return param is None


class _InStringMockMatcher(MockMatcher):

    """Checks if string contains some fragment."""

    def __init__(self, contains):
        """Constructor.

        :param contains: value that we are looking for.
        """

        MockMatcher.__init__(self)
        self.contains = contains

    def __str__(self):
        return '<String containing \'%s\'>' % (self.contains,)

    def __eq__(self, other):
        return isinstance(other, _InStringMockMatcher) and \
            self.contains == other.contains

    def compare(self, param):
        """Check if matcher hits parameter value"""
        if not isinstance(param, str):
            return False
        return self.contains in param


class _NotInStringMockMatcher(MockMatcher):

    """Checks if string doesn't contain some fragment."""

    def __init__(self, not_contains):
        """Constructor.

        :param contains: value that we are looking for.
        """

        MockMatcher.__init__(self)
        self.not_contains = not_contains

    def __str__(self):
        return '<String not containing \'%s\'>' % (self.not_contains,)

    def __eq__(self, other):
        return isinstance(other, _NotInStringMockMatcher) and \
            self.not_contains == other.not_contains

    def compare(self, param):
        """Check if matcher hits parameter value"""
        if not isinstance(param, str):
            return False
        return not (self.not_contains in param)


class _RegexMockMatcher(MockMatcher):

    """Checks if regex matches string."""

    def __init__(self, compiled_re):
        """Constructor.

        :param compiled_re: Compiled regex to match against string.
        """

        MockMatcher.__init__(self)
        self.compiled_re = compiled_re

    def __str__(self):
        return "<Regex '%s'>" % (self.compiled_re.pattern,)

    def __eq__(self, other):
        return isinstance(other, _RegexMockMatcher) and \
            self.compiled_re.pattern == other.compiled_re.pattern

    def compare(self, param):
        if not isinstance(param, str):
            return False
        return bool(self.compiled_re.search(param))


class _TypeMockMatcher(MockMatcher):

    """Type matcher. Use it to match only type based expectations."""

    def __init__(self, match_type):
        """Constructor.

        :param match_type: Your expected type.
        """
        MockMatcher.__init__(self)
        self.in_type = match_type

    def __str__(self):
        return str(self.in_type)

    def __eq__(self, other):
        return isinstance(other, _TypeMockMatcher) and \
            self.in_type == other.in_type

    def compare(self, param):
        """Check if matcher hits parameter value"""
        return isinstance(param, self.in_type)


class _FunctionMockMatcher(MockMatcher):

    """Matcher for parameters which are functions."""

    def __str__(self):
        return '<function or method>'

    def __eq__(self, other):
        return isinstance(other, _FunctionMockMatcher)

    def compare(self, param):
        """Check if matcher hits parameter value"""
        return inspect.isfunction(param) or inspect.ismethod(param)


class _ListMockMatcher(MockMatcher):

    """List and tuples matcher."""

    def __init__(self, contains, list_only=False, tuple_only=False):
        """Constructor.

        :param contains: List of values which are expected to be in list/tuple.
        :param list_only: Only list is expected.
        :param tuple_only: Only tuple is expected.
        """
        assert not (list_only and tuple_only), \
            'list_only and tuple_only parameters are mutually exclusive'

        MockMatcher.__init__(self)
        self.contains = contains
        self.list_only = list_only
        self.tuple_only = tuple_only

    def __str__(self):
        if not self.list_only and not self.tuple_only:
            return '<list or tuple with: %s>' % (self.contains,)

    def __eq__(self, other):
        return isinstance(other, _ListMockMatcher) and \
            self.contains == other.contains and \
            self.list_only == other.list_only and \
            self.tuple_only == other.tuple_only

    def compare(self, param):
        """Check if matcher hits parameter value"""
        if self.list_only and isinstance(param, tuple):
            return False

        if self.tuple_only and isinstance(param, list):
            return False

        if not isinstance(param, (list, tuple)):
            return False

        if isinstance(self.contains, (list, tuple)):
            for val in self.contains:
                try:
                    param.index(val)
                except ValueError:
                    return False
            return True

        return False


def str_with(val):
    """String contains value."""
    return _InStringMockMatcher(val)


def str_without(val):
    """String doesn't contain value"""
    return _NotInStringMockMatcher(val)


def regex_match(val):
    """Regex matches string"""
    return _RegexMockMatcher(val)


def is_function():
    """Expects function."""
    return _FunctionMockMatcher()


def is_type(expected_type):
    """Expects user custom type."""

    return _TypeMockMatcher(expected_type)


def is_str():
    """Expects any string"""
    return is_type(str)


def is_int():
    """Expects any int value."""
    return is_type(int)


def is_long():
    """Expects any long value."""
    return is_type(long)


def is_float():
    """Expects any float value"""
    return is_type(float)


def is_number():
    """Expects any numeric value"""
    return is_type((int, long, float))


def is_dict():
    """Expects any dictionary"""
    return _TypeMockMatcher(dict)


def is_list():
    """Expects any list"""
    return _TypeMockMatcher(list)


def is_tuple():
    """Expects any tuple"""
    return _TypeMockMatcher(tuple)


def list_or_tuple_contains(val):
    """Expects list or tuple containing value or list of values."""
    return _ListMockMatcher(val)


def tuple_contains(val):
    """Expects tuple containing value or list of values."""
    return _ListMockMatcher(val, tuple_only=True)


def list_contains(val):
    """Expects list containing value or list of values."""
    return _ListMockMatcher(val, list_only=True)


def any_args():
    """Accept any arguments, values, keyword values."""
    return _AnyArgsMatcher()


def any_val():
    """Accept any value."""
    return _AnyMatcher()


def is_none():
    """Matches none values."""
    return _NoneMatcher()
