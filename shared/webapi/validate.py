"""Module with validators for base Python types.

:Status: $Id: //prod/main/_is/shared/python/webapi/validate.py#18 $
:Authors: vburenin, rbodnarc
"""

from errors import InvalidData


class NoneChecker(object):

    """None data checker"""

    def __init__(self, none_is_ok=False):
        """Initialize NoneChecker instance.

        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self.__none_is_ok = none_is_ok

    def is_none_ok(self):
        """Return True if None is acceptable for this validator,
        False otherwise.
        >>> NoneChecker(False).is_none_ok()
        False
        >>> NoneChecker(True).is_none_ok()
        True
        """
        return self.__none_is_ok

    def check_none(self, val, msg):
        """Checks if a value is None and raise an appropriate exception.

        :param val: Value to be checked.
        :param msg: Error messages if value is None.
        :return: False if value is not None.
        :raise: InvalidData if value is None.
        >>> NoneChecker(True).check_none(1, '')
        False
        >>> NoneChecker(False).check_none(1, '')
        False
        >>> NoneChecker().check_none(1, '')
        False
        >>> NoneChecker(True).check_none(None, '')
        True
        >>> NoneChecker().check_none(None, 'err_msg')
        Traceback (most recent call last):
        InvalidData: err_msg : None
        """
        if val is None and not self.__none_is_ok:
            raise InvalidData('%s : %s' % (msg, val))
        return val is None

    def get_help(self):
        """Help function to be redefined in the child classes.
        >>> NoneChecker().get_help()
        ''
        """
        return ''


class BoolTypeFormat(NoneChecker):

    """Checker for a boolean type"""

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help message for a boolean type.
        >>> BoolTypeFormat().get_help() != ''
        True
        """
        return 'A boolean value. Can be either True or False. '

    def __call__(self, val):
        """Check a boolean value.

        :param val: A value to be checked.
        :return: Boolean representation of passed value.
        :raise: InvalidData exception if a value isn't an instance of
        boolean type.
        >>> BoolTypeFormat()(True)
        True
        >>> BoolTypeFormat()(False)
        False
        >>> BoolTypeFormat(True)(None)
        >>> BoolTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: Boolean value must not be empty : None
        >>> BoolTypeFormat()(1)
        True
        >>> BoolTypeFormat()('0')
        Traceback (most recent call last):
        InvalidData: Incorrect type "str" for the value "0". Must be "bool".
        >>> BoolTypeFormat()('True')
        True
        >>> BoolTypeFormat()('false')
        False
        >>> BoolTypeFormat()('fALSe')
        Traceback (most recent call last):
        InvalidData: Incorrect type "str" for the value "fALSe". Must be "bool".
        """
        if self.check_none(val, 'Boolean value must not be empty'):
            return val

        if val in (True, 'True', 'true', 1):
            return True
        elif val in (False, 'False', 'false', 0):
            return False
        else:
            raise InvalidData('Incorrect type "%s" for the value "%s".'
                              ' Must be "bool".' %
                              (type(val).__name__, val))


class StringTypeFormat(NoneChecker):

    """Checker for string type"""

    def __init__(self, max_length=0, none_is_ok=False):
        """Initialize StringTypeFormat instance.

        :param max_length: Maximum valid length for the strings.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self._max_len = max_length
        NoneChecker.__init__(self, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help messages for the string type.
        >>> StringTypeFormat().get_help() != ''
        True
        """
        if self._max_len > 0:
            h = 'A string with max length %s symbols. ' % (self._max_len,)
        else:
            h = 'A string. '
        return h

    def __call__(self, val):
        """Check an string value.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if a value isn't an instance of
        string type.
        >>> StringTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: String value must not be empty : None
        >>> StringTypeFormat()(chr(134))
        '\\x86'
        >>> StringTypeFormat()('12345')
        '12345'
        >>> StringTypeFormat(10)('12345')
        '12345'
        >>> StringTypeFormat(3)('12345')
        Traceback (most recent call last):
        InvalidData: String is too long, max length is 3 symbols : 12345
        >>> StringTypeFormat()(12345)
        Traceback (most recent call last):
        InvalidData: Incorrect type "int" for the value "12345". Must be "str".
        """
        if self.check_none(val, 'String value must not be empty'):
            return val

        if not isinstance(val, str):
            raise InvalidData('Incorrect type "%s" for the value "%s".'
                              ' Must be "str".' %
                               (type(val).__name__, val))

        if self._max_len and len(val) > self._max_len:
            raise InvalidData('String is too long, max length is'
                              ' %s symbols : %s' %
                               (self._max_len, val))

        return val


class ASCIIStringTypeFormat(StringTypeFormat):

    """Checker for ASCII strings"""

    def __init__(self, max_length=0, none_is_ok=False):
        """Initialize ASCIIStringTypeFormat instance.

        :param max_length: Maximum valid length for the strings.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        StringTypeFormat.__init__(self, max_length, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help messages for the ASCII string type.
        >>> ASCIIStringTypeFormat().get_help() != ''
        True
        """
        if self._max_len > 0:
            h = 'An ASCII string with max length %s symbols. ' % \
                (self._max_len,)
        else:
            h = 'An ASCII string. '
        return h

    def __call__(self, val):
        """Check an ASCII string value.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if string contain non ASCII
                characters or is too long.
        >>> ASCIIStringTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: ASCII string value must not be empty : None
        >>> ASCIIStringTypeFormat()(chr(134))
        Traceback (most recent call last):
        InvalidData: String must contain only ASCII symbols : \x86
        >>> ASCIIStringTypeFormat()('12345')
        '12345'
        """
        if self.check_none(val, 'ASCII string value must not be empty'):
            return val

        val = StringTypeFormat.__call__(self, val)

        for char in val:
            if ord(char) < 32 or ord(char) > 127:
                raise InvalidData('String must contain only ASCII symbols : %s'
                                  % (val,))

        return val


class IntTypeFormat(NoneChecker):

    """Checker for Integer type"""

    def __init__(self, min_val=None, max_val=None, none_is_ok=False):
        """Initialize IntTypeFormat instance.

        :param min_val: Minimum valid integer for a value.
        :param max_val: Maximum valid integer for a value.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self.__max_val = max_val
        self.__min_val = min_val
        NoneChecker.__init__(self, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help messages for an integer type.
        >>> IntTypeFormat().get_help() != ''
        True
        """
        if self.__max_val is None and self.__min_val is None:
            h = 'An integer value. '
        elif self.__max_val is None:
            h = 'An integer value with min value %s. ' % (self.__min_val,)
        elif self.__min_val is None:
            h = 'An integer value with max value %s. ' % (self.__max_val,)
        else:
            h = 'An integer value in range [%s, %s]. ' % \
                 (self.__min_val, self.__max_val)
        return h

    def __call__(self, val):
        """Check an integer value.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if a value cann't be converted to
                an integer.
        >>> IntTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: Integer value must not be empty : None
        >>> IntTypeFormat()('12345')
        12345
        >>> IntTypeFormat()(12)
        12
        >>> IntTypeFormat(min_val=12)(1)
        Traceback (most recent call last):
        InvalidData: Integer value must be >= 12 : 1
        >>> IntTypeFormat(min_val=12, max_val=35)(40)
        Traceback (most recent call last):
        InvalidData: Integer value must be <= 35 : 40
        >>> IntTypeFormat(min_val=12, max_val=35)(25)
        25
        >>> IntTypeFormat()(12.5)
        Traceback (most recent call last):
        InvalidData: Incorrect integer type : 12.5
        >>> IntTypeFormat()('17.555')
        Traceback (most recent call last):
        InvalidData: Incorrect integer type : 17.555
        """
        if self.check_none(val, 'Integer value must not be empty'):
            return val

        try:
            val = int(str(val))
            if self.__max_val is not None:
                if val > self.__max_val:
                    raise InvalidData('Integer value must be <= %s : %s' %
                                       (self.__max_val, val))

            if self.__min_val is not None:
                if val < self.__min_val:
                    raise InvalidData('Integer value must be >= %s : %s' %
                                       (self.__min_val, val))
            return val

        except (TypeError, ValueError):
            raise InvalidData('Incorrect integer type : %s' %
                               (val,))


class FloatTypeFormat(NoneChecker):

    """Checker for a float type"""

    def __init__(self, min_val=None, max_val=None, none_is_ok=False):
        """Initialize FloatTypeFormat instance.

        :param min_val: Minimum valid float for a value.
        :param max_val: Maximum valid float for a value.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self.__max_val = max_val
        self.__min_val = min_val
        NoneChecker.__init__(self, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help message for a float type.
        >>> FloatTypeFormat().get_help() != ''
        True
        """
        if self.__max_val is None and self.__min_val is None:
            h = 'An float value. '
        elif self.__max_val is None:
            h = 'An float value with min value %s. ' % (self.__min_val,)
        elif self.__min_val is None:
            h = 'An float value with max value %s. ' % (self.__max_val,)
        else:
            h = 'An float value in range [%s, %s]. ' % \
                 (self.__min_val, self.__max_val)
        return h

    def __call__(self, val):
        """Check a float value.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if a value isn't an instance of
        float type.
        >>> FloatTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: Float value must not be empty : None
        >>> FloatTypeFormat()('123.45')
        123.45
        >>> FloatTypeFormat()(12.5)
        12.5
        >>> FloatTypeFormat()(12)
        12.0
        >>> FloatTypeFormat()(True)
        Traceback (most recent call last):
        InvalidData: Incorrect type "bool" for the value "True". Must be "float"
        >>> FloatTypeFormat(min_val=12)(1.0)
        Traceback (most recent call last):
        InvalidData: Float value must be >= 12 : 1.0
        >>> FloatTypeFormat(min_val=12, max_val=35.5)(35.9)
        Traceback (most recent call last):
        InvalidData: Float value must be <= 35.5 : 35.9
        >>> FloatTypeFormat(min_val=12.2, max_val=35)(12.5)
        12.5
        """
        if self.check_none(val, 'Float value must not be empty'):
            return val

        try:
            val = float(str(val))
            if self.__max_val is not None:
                if val > self.__max_val:
                    raise InvalidData('Float value must be <= %s : %s' %
                                       (self.__max_val, val))

            if self.__min_val is not None:
                if val < self.__min_val:
                    raise InvalidData('Float value must be >= %s : %s' %
                                       (self.__min_val, val))
            return val
        except (TypeError, ValueError):
            raise InvalidData('Incorrect type "%s" for the value "%s".'
                              ' Must be "float"' %
                               (type(val).__name__, val))


class ListTypeFormat(NoneChecker):

    """Checker for a list type"""

    def __init__(self, item_validator=None, none_is_ok=False):
        """Initialize ListTypeFormat instance.

        :param item_validator: Validator object to be applied
                               for each item in list.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self.__item_validator = item_validator
        NoneChecker.__init__(self, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help message for a list type.
        >>> ListTypeFormat().get_help() != ''
        True
        """
        if self.__item_validator:
            item_help = self.__item_validator.get_help()
            h = 'A list object. Each item in list should be %s' % \
                    (item_help.replace(item_help[0], item_help[0].lower(), 1),)
        else:
            h = 'A list object. '

        return h

    def __call__(self, val):
        """Check a list object.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if a value isn't an instance of
        list type.
        >>> ListTypeFormat()([])
        []
        >>> ListTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: List object must not be empty : None
        >>> ListTypeFormat()(['123', '45'])
        ['123', '45']
        >>> ListTypeFormat(item_validator=IntTypeFormat())(['123', 45])
        [123, 45]
        >>> ListTypeFormat(item_validator=IntTypeFormat(none_is_ok=True))([123, None])
        [123, None]
        """

        if self.check_none(val, 'List object must not be empty'):
            return val

        if isinstance(val, list):

            if self.__item_validator:
                for i in range(len(val)):
                    val[i] = self.__item_validator(val[i])
            return val

        else:
            raise InvalidData('Incorrect type "%s" for the value "%s".'
                              ' Must be "list"' %
                               (type(val).__name__, val))


class DictTypeFormat(NoneChecker):

    """Checker for a dict type"""

    def __init__(self, item_validator=None, none_is_ok=False):
        """Initialize DictTypeFormat instance.

        :param item_validator: Validator object to be applied
                               for each item in dictionary.
        :param none_is_ok: If True, None values is valid
                           for this validator.
        """
        self.__item_validator = item_validator
        NoneChecker.__init__(self, none_is_ok)

    def get_help(self):
        """Redefine function from NoneChecker class.

        :return: Help message for a dict type.
        >>> DictTypeFormat().get_help() != ''
        True
        """
        if self.__item_validator:
            item_help = self.__item_validator.get_help()
            h = 'A dictionary object. Each value in a dictionary should be ' \
                '%s' % (item_help.replace(item_help[0],
                                          item_help[0].lower(), 1),)
        else:
            h = 'A dictionary object. '

        return h

    def __call__(self, val):
        """Check a dictionary object.

        :param val: A value to be checked.
        :return: Passed value.
        :raise: InvalidData exception if a value isn't an instance of
        dict type.
        >>> DictTypeFormat()({})
        {}
        >>> DictTypeFormat()(None)
        Traceback (most recent call last):
        InvalidData: Dictionary object must not be empty : None
        >>> DictTypeFormat()({'123' :['1', 2, '3'], '45': 45})
        {'123': ['1', 2, '3'], '45': 45}
        >>> DictTypeFormat(item_validator=IntTypeFormat())({'123': '45'})
        {'123': 45}
        >>> DictTypeFormat(item_validator=IntTypeFormat(none_is_ok=True))({'123' :None, '45': 45})
        {'123': None, '45': 45}
        """

        if self.check_none(val, 'Dictionary object must not be empty'):
            return val

        if isinstance(val, dict):

            if self.__item_validator:
                for key in val:
                    val[key] = self.__item_validator(val[key])
            return val

        else:
            raise InvalidData('Incorrect type "%s" for the value "%s".'
                              ' Must be "dict"' %
                               (type(val).__name__, val))


if __name__ == '__main__':
    import doctest
    doctest.testmod()
