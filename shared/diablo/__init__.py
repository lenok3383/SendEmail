"""Diablo Framework

A replacement for the FT framework to make it more modulular.
The framework aims to make writing a long running daemon very simple
and consistent across products.

For additional information look at the User's Guide:
http://eng.ironport.com/docs/is/shared/diablo_user_guide.rst

:Authors: scottrwi
:Status: $Id: //prod/main/_is/shared/python/diablo/__init__.py#10 $
"""

# Generally speaking, clients should not import any of the sub-modules
# directly.  Here we set up the things that should be accessible in the
# shared.diablo namespace.
#
# For example, instead of:
#   shared.diablo.run_diablo.main
# Use:
#   shared.diablo.main
# Etc...

# Although "from shared.diablo import *" is poor form, this is a suitable
# way of documenting what can be imported, and can be useful in some
# situations. (Don't forget to list the things in this file!)

__all__ = ('app_thread', 'DiabloBase', 'main', 'WebPlugin', 'web_method',
           'RPCPlugin', 'rpc_method', 'BackdoorPlugin', 'NodeEventPlugin')

# We use from imports here to bind local names.

from shared.diablo.base import app_thread
from shared.diablo.base import DiabloBase
from shared.diablo.run_diablo import main

# Plugins
from shared.diablo.plugins.web import WebPlugin, web_method
from shared.diablo.plugins.rpc import RPCPlugin, rpc_method
from shared.diablo.plugins.monitoring import MonitorPlugin
from shared.diablo.plugins.backdoor import BackdoorPlugin
from shared.diablo.plugins.node_event import NodeEventPlugin
try:
    from shared.diablo.plugins.zkft import ZooKeeperFTPlugin,\
        DiabloShutdownException
    from shared.diablo.plugins.zknode import ZooKeeperNodePlugin
    __all__ += ('ZooKeeperFTPlugin', 'ZooKeeperNodePlugin',
        'DiabloShutdownException')
except ImportError:
    # not all python installations include zookeeper
    pass
