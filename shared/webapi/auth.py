"""Web API Authenticator.

:Status: $Id: //prod/main/_is/shared/python/webapi/auth.py#11 $
:Authors: vburenin, rbodnarc
"""

from shared.auth import User, LoginFailureError
from shared.auth.ldapauth import Auth

from paste.auth.basic import AuthBasicAuthenticator, AuthBasicHandler
from paste.httpheaders import AUTHORIZATION, REMOTE_USER, AUTH_TYPE


class AuthHandler(AuthBasicHandler):
    """Web API authentication handler."""

    def __init__(self, application, realm, auth_func=None):
        """AuthHandler __init__ method.

        :param application: Callable object (actually ServiceExposer instance).
        :param realm: Name of the authentication realm.
        :param auth_func: Callable object that receives username and password
                          and returns None or auth.User instance.
        """
        self.application = application
        self.authenticate = Authenticator(realm, auth_func)

    def __call__(self, environ, start_response):
        """AuthHandler __call__ method.

        :param environ: Request environment variable.
        :param start_response: Call-back function obtained from index.wsgi.
        :return: Result page to be sent to the client.
        """
        username = REMOTE_USER(environ)
        if not username:
            result = self.authenticate(environ)
            if isinstance(result, User):
                AUTH_TYPE.update(environ, 'basic')
                REMOTE_USER.update(environ, result.username)
                environ['http_user'] = result
            elif result:
                return result.wsgi_application(environ, start_response)

        return self.application(environ, start_response)


class Authenticator(AuthBasicAuthenticator):
    """Authenticator class."""

    def __init__(self, realm, auth_func=None):
        """Authenticator __init__ method.

        :param realm: Name of the authentication realm.
        :param auth_func: Callable object that receives username and password
                          and returns None or auth.User instance.
        """
        self.authfunc = auth_func or self.ldap_auth
        AuthBasicAuthenticator.__init__(self, realm, self.authfunc)

    def __call__(self, environ):
        """Authenticator __call__ method.

        :param environ: Request environment variable.
        :return: auth.User instance.
        """
        authorization = AUTHORIZATION(environ)
        if not authorization:
            return self.build_authentication()

        (authmeth, auth) = authorization.split(' ', 1)
        if 'basic' != authmeth.lower():
            return self.build_authentication()

        auth = auth.strip().decode('base64')
        username, password = auth.split(':', 1)

        return self.authfunc(username, password)

    def ldap_auth(self, username, password):
        """Default authentication function (actually LDAP authentication).

        :param username: Username.
        :param password: Password.
        :return: None or auth.User instance.
        """
        try:
            return Auth().login(username, password)
        except LoginFailureError:
            return None
