
import grp
import pwd
import unittest2

from shared.process import setuid

class MockupGrpPwd:
    """Mockup object for pwd and grp python modules."""

    test_users = [pwd.struct_passwd(('user1', 'password', 1, 3, 'user1',
                                     '/home/user2/', 'bash')),
                  pwd.struct_passwd(('user2', 'password', 1, 4, 'user2',
                                     '/home/user2/', 'bash'))]

    test_groups = [grp.struct_group(('gr_name1', 'gr_passwd', 1, ['user1',
                                                                  'user2'])),
                   grp.struct_group(('gr_name2', 'gr_passwd', 2, ['user2'])),
                   grp.struct_group(('user1', 'gr_passwd', 3, [])),
                   grp.struct_group(('user2', 'gr_passwd', 4, []))]

    @staticmethod
    def getpwnam(user):
        """Return password of the mockup database entry for the given user."""
        for i in MockupGrpPwd.test_users:
            if i.pw_name == user:
                return i
        raise KeyError('Not found')

    @staticmethod
    def getgrgid(gr_id):
        """Return group entry of the mockup database for the given
        group id."""
        for i in MockupGrpPwd.test_groups:
            if i.gr_gid == gr_id:
                return i
        raise KeyError('Not found')

    @staticmethod
    def getgrall():
        """Get list of the mockup group entries."""
        return MockupGrpPwd.test_groups


class MockupOs:
    """Mockup object for os python module."""

    euid = 1000
    uid = 1

    @staticmethod
    def getuid():
        """Return current process mockup user id."""
        return MockupOs.uid

    @staticmethod
    def geteuid():
        """Return current process effective mockup user id."""
        return MockupOs.euid

    @staticmethod
    def seteuid(euid):
        """Set the current process mockup effective user id."""
        MockupOs.euid = euid


def uid_echo(echo_string='Hello'):
    """Test function for execution with parameters.

    :Parameters:
        - `echo_string`: string to return.

    :Return:
        String in format 'euid: echo_string'
    """
    return '%s: %s' % (setuid.os.geteuid(), echo_string)


def get_exception():
    """Test function to produce an exception."""
    return 1 / 0


class Test(unittest2.TestCase):
    """Test case class to test do_as_uid and get_user_groups functions."""

    def setUp(self):
        self.orig_os = setuid.os
        setuid.os = MockupOs
        self.orig_pwd = setuid.pwd
        self.orig_grp = setuid.grp
        setuid.pwd = MockupGrpPwd
        setuid.grp = MockupGrpPwd

    def tearDown(self):
        setuid.os = self.orig_os
        setuid.pwd = self.orig_pwd
        setuid.grp = self.orig_grp

    def test_do_as_uid(self):
        self.assertEqual(setuid.os.geteuid(), 1000)
        self.assertEqual(setuid.do_as_uid(setuid.os.geteuid), 1)
        self.assertEqual(setuid.os.geteuid(), 1000)
        self.assertEqual(setuid.do_as_uid(uid_echo), '1: Hello')
        self.assertEqual(uid_echo(), '1000: Hello')

    def test_do_as_uid_raise(self):
        self.assertEqual(setuid.os.geteuid(), 1000)
        self.assertRaises(Exception, setuid.do_as_uid, get_exception)
        self.assertEqual(setuid.os.geteuid(), 1000)
        self.assertRaises(Exception, setuid.do_as_uid, setuid.os.geteuid, 1)
        self.assertEqual(setuid.os.geteuid(), 1000)

    def test_get_user_groups(self):
        self.assertRaises(KeyError, setuid.get_user_groups, 'ohmelevs')
        self.assertEqual(setuid.get_user_groups('user1'), {1: 'gr_name1',
                                                           3: 'user1'})
        self.assertEqual(setuid.get_user_groups('user2'), {1: 'gr_name1',
                                                           2: 'gr_name2',
                                                           4: 'user2'})
        self.assertNotEqual(setuid.get_user_groups('user1'), {1: 'gr_name1',
                                                              2: 'gr_name2',
                                                              4: 'user2'})

if __name__ == "__main__":
    unittest2.main()
