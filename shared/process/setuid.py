"""Unix setuid utility functions.

:Status: $Id: //prod/main/_is/shared/python/process/setuid.py#6 $
:Authors: jwescott, ohmelevs
"""

import grp
import os
import pwd


def do_as_uid(func, *args):
    """Execute function as the actual uid rather than the effective uid.

    This function ensures that the effective uid before and
    after the function call is the same.  It is only during the
    execution of func that the actual uid will be used.

    :Parameters:
        - `func`: function to execute.
        - `*args`: arguments for the specified function.

    :Return:
        Result of the execution specified function with the given parameters.
    """
    uid = os.getuid()
    euid = os.geteuid()
    try:
        os.seteuid(uid)
        return func(*args)
    finally:
        os.seteuid(euid)


def get_user_groups(user):
    """Get the list of groups to which a user belongs.

    :Parameters:
        - `user`: the user name in question.

    :Return:
        Dictionary in format: { gid1: gname1, gid2: gname2 }.
    """
    main_group = pwd.getpwnam(user).pw_gid
    main_group_name = grp.getgrgid(main_group).gr_name
    groups = { main_group : main_group_name }
    for group in grp.getgrall():
        if user in group.gr_mem:
            groups[group.gr_gid] = group.gr_name
    return groups
