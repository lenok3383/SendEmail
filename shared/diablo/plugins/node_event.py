# $Header: //prod/main/_is/shared/python/diablo/plugins/node_event.py#1 $
# Copyright (c) 2012 Cisco Systems, Inc.
# All rights reserved.
# Unauthorized redistribution prohibited.

"""Node Event Plugin for Diablo.

:Authors: parker jmacelro
"""

__version__ = '$Revision: #1 $'

import logging
import time

import shared.db
import shared.web

from shared.diablo import web_method
from shared.diablo.plugins import public_method
from shared.diablo.plugins import DiabloPlugin


DEFAULT_SLEEP = 3600
DEFAULT_MAX_ENTRIES = 1000


class NodeEventPlugin(DiabloPlugin):

    """Diablo plugin that logs specific events to a MySQL table.

    The events logged are intended to be used with monitoring and viewing.
    Use the following in your __init__ to enable the plugin in your application:
        self.register_plugin(shared.diablo.NodeEventPlugin, db_pool_name='%s')

    The following configuration can be placed in your diablo daemon
    configuration section if you do not wish to use the defaults:
        - `max_node_event_entries`: The max number of entries to store.
        - `node_event_housekeeping_sleep`: The sleep time between housekeepings.

    This plugin requires that a table named node_events is created in the
    database.  This table should have the following schema:

        CREATE TABLE node_events (
            event_id int unsigned AUTO_INCREMENT,
            app_name VARCHAR(255) BINARY NOT NULL,
            service_name VARCHAR(255) BINARY NOT NULL,
            node_id VARCHAR(255) BINARY NOT NULL,
            event VARCHAR(32) BINARY NOT NULL,
            hostname VARCHAR(255) BINARY,
            pid int unsigned,
            event_details VARCHAR(255),
            event_ts int unsigned NOT NULL,
            PRIMARY KEY (event_id),
            INDEX app_idx(app_name, service_name, node_id),
            INDEX cluster_idx(service_name),
            index node_idx(node_id)
        ) ENGINE = InnoDB DEFAULT CHARSET=latin1;

    """
    NAME = 'node_event'

    def __init__(self, *args, **kwargs):
        """Initialize the plugin.

        It is required that db_pool_name is set as a kwarg.  This is
        the database pool that node event will use to store entries.
        """
        self.__log = logging.getLogger('diablo.plugins.node_event')

        if 'db_pool_name' not in kwargs:
            raise ValueError('Missing required db_pool_name parameter.')

        super(NodeEventPlugin, self).__init__(*args, **kwargs)

        self._housekeeping_sleep = \
            self.conf.get('%s.node_event_housekeeping_sleep' %
                          (self.conf_section,), DEFAULT_SLEEP)

        self._max_event_entries = \
            self.conf.get('%s.max_node_event_entries' % (self.conf_section,),
                          DEFAULT_MAX_ENTRIES)

        # Setup the db pool and validate the rw pool only. There is no need
        # to check the ro pool.
        self._db_pool_name = kwargs['db_pool_name']
        shared.db.validate_pools((self._db_pool_name,), ())
        self._db_rw_pool = shared.db.get_rw_pool(self._db_pool_name)

        # Set the _node_id attribute to the node id value specified in the
        # daemon object if it exists.  Otherwise, we will create our own.
        if self.daemon_obj.node_id:
            self._node_id = self.daemon_obj.node_id
        else:
            self._node_id = '%s_%s' % (self.daemon_obj.hostname,
                                         self.daemon_obj.pid)

    def startup(self):
        """Start the node event plugin."""
        self.__log.info('Starting up node event plugin.')
        self.start_in_thread('event_houskeeping', self._housekeeping)

    def shutdown(self):
        """Stop the node event plugin."""
        self.__log.info('Shutting down node event plugin.')
        self.join_threads()

    def _housekeeping(self):
        """Housekeeping thread is used to periodically purge the database."""
        self.__log.debug('Starting node event plugin housekeeping thread.')

        while self.daemon_obj.should_continue():
            self.__log.debug('Node event plugin housekeeping loop.')
            self._purge_events_db()
            # Sleep before trying to cleanup again
            self.daemon_obj.shallow_sleep(self._housekeeping_sleep)

    def _purge_events_db(self):
        """Purges node event database of old events.

        The maximum number of event entries to keep is configurable.
        """
        with self._db_rw_pool.transaction() as cursor:
            cursor.execute("""SELECT max(event_id) FROM node_events""")
            found_row = cursor.fetchone()

            # If there are no entries there is nothing to do.
            if not found_row or not found_row[0]:
                return

            min_event_id = found_row[0] - self._max_event_entries
            # There is no need to delete negative event ids.
            if min_event_id < 1:
                return

            self.__log.debug('Node event deleting event ids <= %s.',
                            min_event_id)

            while 1:
                deleted_rows = cursor.execute("""DELETE FROM node_events
                                                 WHERE event_id <= %s
                                                 ORDER BY event_id ASC
                                                 LIMIT 1000""",
                                              (min_event_id,))

                if not deleted_rows:
                    break

                self.__log.debug('Node event plugin deleted %s rows.',
                                deleted_rows)

    @public_method
    def log_node_event(self, service_name, event, event_details, event_ts=None):
        """Places an entry in the node event database.

        :Parameters:
            -`service_name`: The name of the service logging the event.
            -`event`: The event being logged.
            -`event_details`: Details specific to this occurence of the event.
            -`event_ts`: Optional time stamp for event entry.
        """
        # Use the provided time stamp or create one.
        if event_ts:
            event_ts = int(event_ts)
        else:
            event_ts = int(time.time())

        self.__log.debug('Logging node event: %s - %s - %s - %s',
                        service_name, event, event_details, event_ts)

        with self._db_rw_pool.transaction() as cursor:
            cursor.execute("""INSERT INTO node_events(
                                app_name,
                                service_name,
                                node_id,
                                hostname,
                                pid,
                                event,
                                event_details,
                                event_ts)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                           (self.daemon_obj.app_name,
                            service_name,
                            self._node_id,
                            self.daemon_obj.hostname,
                            self.daemon_obj.pid,
                            event,
                            event_details,
                            event_ts,))

    @web_method(name='node_events')
    def web_node_events(self, *args, **kwargs):
        """Display node events information."""
        table_header = ['Event ID', 'Application Name', 'Service Name',
                        'Node ID', 'Event', 'Hostname', 'PID', 'Event Details',
                        'Event Time']
        table_data = []

        data = self._get_node_events()

        for row in data:
            table_data.append(row)

        table_data.insert(0, table_header)

        return ('<h1>Node Events</h1>'
                + shared.web.html_formatting.list_of_lists_to_table(table_data,
                                                                    escape=False))

    @shared.db.utils.retry
    def _get_node_events(self):
        """Fetch all node events for our app name."""
        data = []
        with self._db_rw_pool.transaction() as cursor:
            cursor.execute("""SELECT event_id,
                                     app_name,
                                     service_name,
                                     node_id,
                                     event,
                                     hostname,
                                     pid,
                                     event_details,
                                     FROM_UNIXTIME(event_ts)
                              FROM node_events
                              WHERE app_name = %s
                              ORDER BY event_id DESC""",
                           (self.daemon_obj.app_name,))
            data = cursor.fetchall()

        return data
