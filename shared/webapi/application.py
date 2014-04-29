"""get_application function to get callable object to be used in index.wsgi.

:Status: $Id: //prod/main/_is/shared/python/webapi/application.py#9 $
:Authors: vburenin, rbodnarc
"""

from apiscanner import ServiceExposer
from auth import AuthHandler

def get_application(resource_path, default_version=None,
                    auth_func=None, realm_name='IronPort Web API'):
    """get_application function to be used in index.wsgi.

    :param resource_path: Path to a folder with Web API resources.
    :param default_version: Name of the version to be used as the default
                            version.  If it is not set, the last available
                            version will be used as default.
    :param auth_func: Callable object that receives username and password
                      and returns None or auth.User instance.
    :param realm_name: Name of the authentication realm.
    :return: Callable application object.
    """
    return AuthHandler(ServiceExposer(resource_path,
                                      default_version),
                       realm_name, auth_func)
