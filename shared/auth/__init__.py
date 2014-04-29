"""This is the IronPort shared.auth python package.  Common base
classes for auth objects are included here.

:Status: $Id: //prod/main/_is/shared/python/auth/__init__.py#9 $
:Authors: jwescott
"""

import os
import pwd
import socket


class AuthError(Exception):
    """Base class for auth exceptions."""
    pass


class LoginFailureError(AuthError):
    """Exception raised if login fails."""
    pass


class InsufficientPrivilegesError(AuthError):

    """Exception raised if the user doesn't have sufficient privileges
    for a given operation."""

    def __init__(self, needed, has):
        """Create a new InsufficientPrivilegesError instance.

        :param needed: The list of needed privileges.
        :param has: The list of privileges a user actually has.
        """
        self.needed = needed
        self.has = has
        AuthError.__init__(self)

    def __str__(self):
        """Get string representation of an InsufficientPrivilegesError
        instance.

        :return: String with error description.
        """
        msg = 'Authorization Error: You need (%s), but you have ' % (
              ', '.join(self.needed))
        if self.has:
            msg += 'only (%s).' % (', '.join(self.has))
        else:
            msg += 'nothing.'
        return msg


class User(object):

    """Base class for user objects."""

    def __init__(self, username, *privileges):
        """Create a new User.

        :param username: The user name.
        :param *privileges: Zero or more privileges for the user.
        """
        self.username = username
        self.__privileges = set()
        self.set_privileges(*privileges)

    def __str__(self):
        """Get string representation of an User instance.

        :return: String in format 'User username'.
        """
        return 'User %s' % (self.username,)

    def get_privileges(self):
        """Get the set of privileges for this user.

        :return: set of privileges.
        >>> user = User('user', ['read', 'write'])
        >>> user.get_privileges().difference(set(['read', 'write']))
        set([])
        """
        return self.__privileges

    def set_privileges(self, *privileges):
        """Set the privileges for this user.

        :param *privileges: Zero or more privileges for the user.
        >>> user = User('user')
        >>> user.get_privileges()
        set([])
        >>> user.set_privileges('delete', 'admin')
        >>> user.get_privileges().difference(set(['delete', 'admin']))
        set([])
        >>> user.set_privileges(['read', 'write'])
        >>> user.get_privileges().difference(set(['read', 'write']))
        set([])
        >>> user.set_privileges(['read', 'write'], 'delete')
        >>> user.get_privileges().difference(set(['read', 'write', 'delete']))
        set([])
        >>> user.set_privileges(('read', 'write'), 'delete')
        >>> user.get_privileges().difference(set(['read', 'write', 'delete']))
        set([])
        >>> user.set_privileges(set(['delete', 'admin']))
        >>> user.get_privileges().difference(set(['delete', 'admin']))
        set([])
        """
        privileges = self.__flatten_list(privileges)
        self.__privileges = set(privileges)

    privileges = property(get_privileges, set_privileges)

    def assert_privileges(self, *privileges):
        """Assert that the user has the given privileges.

        :param privileges: List of needed privileges.
        :raise InsufficientPrivilegesError: If the user is missing one
                                            or more of the needed privileges
                                            in the existing privileges.
        >>> user = User('user', ['read', 'write'])
        >>> user.assert_privileges([])
        >>> user.assert_privileges(['read'])
        >>> user.assert_privileges('read')
        >>> user.assert_privileges(['delete'], 'read')
        Traceback (most recent call last):
        InsufficientPrivilegesError: Authorization Error: You need (read, delete), but you have only (read, write).
        >>> user.assert_privileges(('delete'), 'read')
        Traceback (most recent call last):
        InsufficientPrivilegesError: Authorization Error: You need (delete, read), but you have only (read, write).
        >>> user = User('user')
        >>> user.assert_privileges(['delete'])
        Traceback (most recent call last):
        InsufficientPrivilegesError: Authorization Error: You need (delete), but you have nothing.
        """
        privileges = self.__flatten_list(privileges)

        if not self.__privileges.issuperset(set(privileges)):
            raise InsufficientPrivilegesError(privileges, self.__privileges)

    def __flatten_list(self, privileges):
        """Transform everything to a list.

        Converts a list of elements/tuples/sublists into a flat list.
        E.g. : ('read', 'write', ['delete', 'admin']) ->
               ['read', 'write', 'delete', 'admin']

        :param privileges: List to be transformed.
        :return: List of single elements.
        """
        return sum(
            [list(priv) for priv in privileges if hasattr(priv, '__iter__')],
            [priv for priv in privileges if not hasattr(priv, '__iter__')]
        )


class Auth(object):

    """Base Auth class."""

    def __init__(self, user_class=User):
        """Create a new Auth object.

        :param user_class: shared.auth.User class or subclass to
                           instantiate upon successful login.
        """
        self.user_class = user_class

    def login(self, username, password):
        """Login.  Subclasses should override this method.

        :param username: Username for login.
        :param password: Password for login.
        """
        raise NotImplementedError('login should be overridden in subclasses.')

    def get_privileges(self, username):
        """Get the list of privileges for the given username.

        :param username: The user whose priviliges we want.
        :return: List of privileges.
        """
        raise NotImplementedError(
                    'get_privileges should be overridden in subclasses.')


def get_shell_user(include_hostname=True):
    """Get the name of the shell user.

    :param include_hostname: Whether or not the hostname should be included.
    :return: String with username or username and host.
    >>> len(get_shell_user()) > 0
    True
    >>> get_shell_user().find('@') > 0
    True
    >>> get_shell_user(include_hostname=False).find('@') > 0
    False
    """
    username = pwd.getpwuid(os.geteuid()).pw_name
    if include_hostname:
        return '%s@%s' % (username, socket.gethostname())
    else:
        return username


if __name__ == '__main__':
    import doctest
    doctest.testmod()

