"""
A simple fast RPC implementation. It clones from the coro version.
This version works with non-coro Python.

It implements a simple form of efficient (i.e., not chatty) RPC.
The client has a handle to a remote 'root' object.  It can
make attribute references on this root, ending in a function
call.  For example:

root.sub_object_1.method_3 (arg0, arg1, ...)

Will send a packet encoding the attribute path (in this case
("sub_object_1", "method_3") to the server side, which will call
method_3() with the given arguments.

:Author: ted
:Status: $Id: //prod/main/_is/shared/python/rpc/__init__.py#1 $
"""


class RPCError(Exception):
    """
    Base class for RPC exceptions
    """
    pass


class RPCServerUnreachable(RPCError):
    """
    Exception of this class will be raised when client
    can not connect to the RPC server
    """
    pass
