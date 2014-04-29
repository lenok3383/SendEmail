import unittest2 as unittest
import ldap

from shared.auth import ldapauth, LoginFailureError
from shared.conf import config

LDAP_DATA = {'user1' : ('pass', set(['read'])),
             'user2' : ('great_pass', set(['read', 'write'])),}
class LdapMock:

    SCOPE_SUBTREE = ldap.SCOPE_SUBTREE
    INVALID_CREDENTIALS = ldap.INVALID_CREDENTIALS
    NO_SUCH_OBJECT = ldap.NO_SUCH_OBJECT
    UNWILLING_TO_PERFORM = ldap.UNWILLING_TO_PERFORM
    LDAPError = ldap.LDAPError

    @staticmethod
    def initialize(host):
        return LdapObjectMock()


class LdapObjectMock:

    def search_s(self, base, scope, filterstr=None, attrlist=None):
        if filterstr:
            if filterstr in LDAP_DATA:
                return [['', {'cn' : [priv]}] for priv in LDAP_DATA[filterstr][1]]
            else:
                raise ldap.NO_SUCH_OBJECT(filterstr)
        else:
            if base.startswith('uid='):
                for key in LDAP_DATA.keys():
                    if base[4:].startswith(key):
                        return [[key]]
        raise ldap.NO_SUCH_OBJECT(base)

    def simple_bind_s(self, who, cred):
        if who in LDAP_DATA:
            if LDAP_DATA[who][0] == cred:
                return True
            else:
                raise ldap.INVALID_CREDENTIALS()
        else:
            raise ldap.NO_SUCH_OBJECT()

class TestLdapAuth(unittest.TestCase):

    def setUp(self):
        config_dict = {'ldap.hostname': 'ldap-master.ironport.com',
                       'ldap.user_dn_template':'uid=%s,ou=people,ou=corporate,'
                       'dc=ironport,dc=com',
                       'ldap.group_search_base_dn':'ou=production,dc=ironport,'
                       'dc=com',
                       'ldap.group_search_filter_template': '%s',
                       'ldap.user_search_base': 'dc=ironport,dc=com',
                       'ldap.user_search_filter': '(&(objectclass=ironportuser)'
                       '(uid=%s))',
                       }
        self.__orig_ldap = ldapauth.ldap
        ldapauth.ldap = LdapMock()
        config_obj = config.ConfigFromDict(config_dict)
        self.addCleanup(config.clear_config_cache)
        config.set_config('ldap', config_obj)
        self.__handle = ldapauth.Auth()

    def tearDown(self):
        ldapauth.ldap = self.__orig_ldap

    def test_valid_credentials(self):
        user1 = self.__handle.login('user1', LDAP_DATA['user1'][0])
        user2 = self.__handle.login('user2', LDAP_DATA['user2'][0])

        self.assertEquals(user1.privileges, LDAP_DATA['user1'][1])
        self.assertEquals(user2.privileges, LDAP_DATA['user2'][1])

    def test_invalid_credentials(self):
        self.assertRaises(LoginFailureError, self.__handle.login,
                          'user3', 'pass')
        self.assertRaises(LoginFailureError, self.__handle.login,
                          'user2', 'simple_pass')

if __name__ == '__main__':
    unittest.main()
