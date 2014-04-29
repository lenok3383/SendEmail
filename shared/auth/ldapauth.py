"""LDAP authentication and authorization.

:Status: $Id: //prod/main/_is/shared/python/auth/ldapauth.py#10 $
:Authors: jwescott, rbodnarc
"""

import ConfigParser
import ldap
import os

import shared.conf

from shared.conf import env
from shared.auth import (User,
                         LoginFailureError,
                         Auth as BaseAuth)


LDAP = 'ldap'
HOSTNAME = 'ldap.hostname'
USER_SEARCH_BASE = 'ldap.user_search_base'
GROUP_SEARCH_BASE_DN = 'ldap.group_search_base_dn'
GROUP_SEARCH_FILTER_TEMPLATE = 'ldap.group_search_filter_template'


class Auth(BaseAuth):

    """Login class that uses LDAP for authentication and
    authorization.  To use this class, you'll need a configuration
    file in CONFROOT called 'ldap.conf', containing something like
    below, replacing YOUR_GROUP_PREFIX' as necessary.

    ::

    [ldap]
    hostname=ns1.eq.ironport.com
    user_dn_template=uid=%s,ou=people,ou=corporate,dc=ironport,dc=com
    group_search_base_dn=ou=production,dc=ironport,dc=com
    group_search_filter_template=(&(cn=<YOUR GROUP PREFIX>*)(objectclass=groupOfUniqueNames)(|(uniqueMember=uid=%s,ou=system,ou=account,ou=production,dc=ironport,dc=com)(uniqueMember=uid=%s,ou=people,ou=corporate,dc=ironport,dc=com)(uniqueMember=uid=%s,ou=system,ou=account,ou=corporate,dc=ironport,dc=com)))
    """

    def __init__(self, user_class=User, config_file=None):
        """Create a new LDAP Auth object.

        :param config_file: The full pathname to the LDAP configuration file.
        :param user_class: shared.auth.User class or subclass to
                           instantiate upon successful login.
        """
        BaseAuth.__init__(self, user_class)
        if config_file is None:
            config_file = LDAP
        self.__config = shared.conf.get_config(config_file)

        self.__ldap = ldap.initialize('ldap://%s' % (self.__config[HOSTNAME]))

    def login(self, username, password):
        """Login via LDAP.

        :param username: Username for login.
        :param password: Password for login.
        :return: Instance of user_class with privileges from LDAP.
        :raise: LoginFailureError if authentication failed.
        """
        try:

            search_dn = 'uid=%s,ou=people,ou=corporate,%s' % (
                username, self.__config[USER_SEARCH_BASE])

            successful_binds = 0
            results = self.__ldap.search_s(search_dn, ldap.SCOPE_SUBTREE)
            for result in results:
                try:
                    user_dn_bind = result[0]
                    self.__ldap.simple_bind_s(user_dn_bind, password)
                    successful_binds += 1
                except ldap.INVALID_CREDENTIALS:
                    pass

        except ldap.NO_SUCH_OBJECT:
            raise LoginFailureError(
                "LDAP Error: invalid uid '%s'." % (username,))
        except ldap.UNWILLING_TO_PERFORM as err:
            raise LoginFailureError('LDAP Error: %s.' % (err.args[0]['info'],))
        except ldap.LDAPError as err:
            raise LoginFailureError(str(err))

        if successful_binds < 1:
            raise  LoginFailureError('LDAP Error: No successful binds')
        elif successful_binds > 1:
            raise  LoginFailureError('LDAP Error: Too many successful binds')

        privs = self.get_privileges(username)
        return self.user_class(username, privs)

    def get_privileges(self, username):
        """Get the list of privileges for the given username.

        :param username: The user whose priviliges we want.
        :return: list of privileges.
        """
        search_dn = self.__config[GROUP_SEARCH_BASE_DN]
        search_filter = self.__config[GROUP_SEARCH_FILTER_TEMPLATE]
        usernames_list = (username,) * search_filter.count('%s')
        search_filter = search_filter % usernames_list

        results = self.__ldap.search_s(search_dn,
                                       ldap.SCOPE_SUBTREE,
                                       filterstr=search_filter,
                                       attrlist=('cn',))
        return [ r[1]['cn'][0] for r in results ]
