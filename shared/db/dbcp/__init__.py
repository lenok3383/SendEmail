"""Database connection pooling.

Most applications will not need to import the shared.db.dbcp modules
directly. See shared.db for more usage information.

:Status: $Id: //prod/main/_is/shared/python/db/dbcp/__init__.py#9 $
:Authors: ohmelevs
"""

from shared.db.dbcp.pool_manager import DBPoolManagerImpl


_dbpool_manager_instance = None

def DBPoolManager(config_path=None):
    """Returns a singleton instance of the DBPoolManagerImpl class.

    :Parameters:
    - config_path: Path to the config file.
    """
    global _dbpool_manager_instance
    if _dbpool_manager_instance is None:
        _dbpool_manager_instance = DBPoolManagerImpl(config_path)
    return _dbpool_manager_instance


def shutdown_pool_manager():
    """Stops pool manager and deletes reference to the singleton instance."""
    global _dbpool_manager_instance
    if _dbpool_manager_instance is not None:
        _dbpool_manager_instance.shutdown()
        _dbpool_manager_instance = None

