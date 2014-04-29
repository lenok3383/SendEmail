"""Database Access Routines

The shared.db namespace should contain all the functionality generally needed
to use databases.

In general it is expected that most products will have a db.conf file that
looks as follows:

[corpus]
db_type=mysql
pool_size=10
timeout=3
idle_timeout=20
log_queries=False
db=corpus
rw.host=corpus-host.ironport.com
rw.user=writer
rw.password=sekr!t
ro.host=corpus-host.ironport.com
ro.user=reader
ro.password=sekr!t

- db_type: the type of database, only 'mysql' is
  supported right now
- pool_size: maximum number of connections
- idle_timeout: timeout period before unused connection
  been released from the pool
- timeout: timeout period when create actual connection to db
- log_queries: log all queries and their execution time. It is
  a debug option, should be turned off in production env

Optional:
- auto_commit: Session auto commit parameter (1|0)
- isolation_level: Isolation level (e.g. "READ COMMITTED").
- session_options: Mysqld server system options to be set per
                   session: {'<option_name>': <option_value>, ...}
- charset: specific charset.

Example Usage:

# Validation of database configuration for needed pools. (my_app read-write and
# read-only, corpus read-only)
shared.db.validate_pools(('my_app',) ('my_app', 'corpus'))

# Executing SQL in tranasctions.
def set_languages(message_id, language_ids):
    pool = pool_manager.get_rw_pool('corpus')
    with pool.transaction() as cursor:
        cursor.execute('DELETE FROM message_languages'
                       'WHERE message_id = %s', (message_id,))
        cursor.executemany('INSERT INTO message_languages'
                           '(message_id, language_id) VALUES (%s, %s)',
                           [(message_id, lng_id) for lng_id in language_ids])

# Working with a DB connection object itself.
with pool_manager.get_rw_pool('corpus').connection() as conn:
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS some_table')
    if conn.show_warnings():
        log.warning('Warning: %s', conn.show_warnings()[-1])

To identify where the SQL come from in database easily, you can
setup a environment variable APP_SQL_TAG. When APP_SQL_TAG is
non-empty value, it will be appended to your query as a comment.
Such as:

  SELECT * FROM foo -- test_app

:Status: $Id: //prod/main/_is/shared/python/db/__init__.py#5 $
:Authors: duncan, ohmelevs
"""

import shared.db.dbcp

__all__ = ('retry', 'get_ro_pool', 'get_rw_pool', 'set_ro_pool', 'set_rw_pool',
           'validate_pools', 'clear_pool_cache')

# shared.db.retry
from shared.db.utils import retry


def get_ro_pool(pool_name):
    """Get the named read-only database pool.

    If the pool does not already exist, it will be instantiated based on the
    configuration in the db.conf.

    :Parameters:
        - `pool_name`: name of pool
    """
    return shared.db.dbcp.DBPoolManager().get_ro_pool(pool_name)


def get_rw_pool(pool_name):
    """Get the named read-write database pool.

    If the pool does not already exist, it will be instantiated based on the
    configuration in the db.conf.

    :Parameters:
        - `pool_name`: name of pool
    """
    return shared.db.dbcp.DBPoolManager().get_rw_pool(pool_name)


def set_ro_pool(pool_name, pool_obj):
    """Associate the given pool object with the read-only pool name.

    This is primarily used to facilitate unit testing -- a given pool object
    can be used in place of one automatically instantiated from the db.conf
    config file.

    :Parameters:
        - `pool_name`: name of pool
        - `pool_obj`: pool object
    """
    return shared.db.dbcp.DBPoolManager().set_ro_pool(pool_name, pool_obj)


def set_rw_pool(pool_name, pool_obj):
    """Associate the given pool object with the read-write pool name.

    This is primarily used to facilitate unit testing -- a given pool object
    can be used in place of one automatically instantiated from the db.conf
    config file.

    :Parameters:
        - `pool_name`: name of pool
        - `pool_obj`: pool object
    """
    return shared.db.dbcp.DBPoolManager().set_rw_pool(pool_name, pool_obj)


def validate_pools(rw_pools, ro_pools):
    """Loads and verifies the proper functioning of all the listed pools.

    This can be used at daemon start time (for example) to verify all of the
    needed database pools are properly configured.

    :Parameters:
    - rw_pools: list of the rw pools to validate.
    - ro_pools: list of the ro pools to validate.

    :Raises:
    - Exception if the database connections are not valid.
    """
    return shared.db.dbcp.DBPoolManager().validate_pools(rw_pools, ro_pools)


def clear_pool_cache():
    """Clears all instantiated DB pools.

    All instantiated pools are deleted. New calls to `get_rw_pool` and
    `get_ro_pool` revert to their standard behavior.

    This should be used in unit testing to re-set the pool manager to a known
    (empty) state.
    """
    return shared.db.dbcp.DBPoolManager().clear_pool_cache()


def shutdown():
    """Shutdown the database pool manager and clean up state.

    This will clear the pool cache, stop the connection handling threads, and
    remove the singleton pool manager.
    """
    return shared.db.dbcp.shutdown_pool_manager()

