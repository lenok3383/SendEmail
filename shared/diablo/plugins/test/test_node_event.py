# $Header: //prod/main/_is/shared/python/diablo/plugins/test/test_node_event.py#1 $
# Copyright (c) 2012 Cisco Systems, Inc.
# All rights reserved.
# Unauthorized redistribution prohibited.

"""Unit testing for Node Event Plugin.

:Authors: jmacelro

"""

__version__ = '$Revision: #1 $'

import datetime
import logging
import os
import unittest2 as unittest
import warnings

import shared.conf
import shared.testing.case

from shared.diablo.plugins import node_event


# Setup logging.
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestNodeEventPlugin(shared.testing.case.TestCase):

    """Test the node event plugin functionality."""

    DB_POOL_NAME = 'node_event_pool'

    def setUp(self):
        self.dbm = self.get_temp_database_manager()
        self.dbm.create_db(self.DB_POOL_NAME)
        self._rw_pool = shared.db.get_rw_pool(self.DB_POOL_NAME)
        self._create_table()
        self.plug = node_event.NodeEventPlugin(MockDaemon(), {}, '',
                                               db_pool_name=self.DB_POOL_NAME)

    def test_db_initialization(self):
        """Ensure that the database has no entries."""
        sql = 'SELECT * FROM node_events'
        self.assertEqual(self._execute_sql(sql), ())

    def test_db_pool_name_required(self):
        """Test db_pool_name assertion."""
        with self.assertRaises(ValueError):
            node_event.NodeEventPlugin()

    def test_db_schema(self):
        """Ensure the schema is as we intended it to be."""
        sql = 'DESC node_events'
        schema = self._execute_sql(sql)
        expected_schema = (('event_id', 'int(10) unsigned', 'NO', 'PRI', None,
                            'auto_increment'),
                           ('app_name', 'varchar(255)', 'NO', 'MUL', None, ''),
                           ('service_name', 'varchar(255)', 'NO', 'MUL', None,
                            ''),
                           ('node_id', 'varchar(255)', 'NO', 'MUL', None, ''),
                           ('event', 'varchar(32)', 'NO', '', None, ''),
                           ('hostname', 'varchar(255)', 'YES', '', None, ''),
                           ('pid', 'int(10) unsigned', 'YES', '', None, ''),
                           ('event_details', 'varchar(255)', 'YES', '', None,
                            ''),
                           ('event_ts', 'int(10) unsigned', 'NO', '', None, ''))
        self.assertEquals(schema, expected_schema)

    def test_config(self):
        """Test max entries and sleep time configuration.

        When configured the values in configuration should be used.  If not
        configured then default values should be used.
        """
        self.assertEquals(node_event.DEFAULT_SLEEP,
                          self.plug._housekeeping_sleep)
        self.assertEquals(node_event.DEFAULT_MAX_ENTRIES,
                          self.plug._max_event_entries)

        conf_section = 'test_conf'
        max_entry_str = '%s.max_node_event_entries' % (conf_section,)
        max_entry = 15
        sleep_str = '%s.node_event_housekeeping_sleep' % (conf_section,)
        sleep = 2
        # Create a new plug with sleep and max entries configured.
        self.plug = node_event.NodeEventPlugin(MockDaemon(),
                                               {sleep_str:sleep,
                                                max_entry_str:max_entry},
                                               conf_section,
                                               db_pool_name=self.DB_POOL_NAME)
        self.assertEquals(sleep, self.plug._housekeeping_sleep)
        self.assertEquals(max_entry, self.plug._max_event_entries)

    def test_node_id(self):
        """Test default and inherited node_id values."""
        # Test configured.
        self.assertEquals(self.plug._node_id, MockDaemon.node_id)

        # Test default.
        md = MockDaemon()
        md.node_id = None
        self.plug = node_event.NodeEventPlugin(md, {}, '',
                                               db_pool_name=self.DB_POOL_NAME)
        node_id_str = '%s_%s' % (self.plug.daemon_obj.hostname,
                                 self.plug.daemon_obj.pid)
        self.assertEquals(self.plug._node_id, node_id_str)

    def test_purge_events(self):
        """Test purge with event count greater and equal to the max."""
        # Configure plugin with max entry set.
        conf_section = 'test_conf'
        max_entry_str = '%s.max_node_event_entries' % (conf_section,)
        max_entry = 2
        self.plug = node_event.NodeEventPlugin(MockDaemon(),
                                               {max_entry_str:max_entry},
                                               conf_section,
                                               db_pool_name=self.DB_POOL_NAME)

        # Log more events than the max entry.
        for x in xrange(10):
            self.plug.log_node_event('service', 'event', 'event_details')

        sql = 'SELECT COUNT(*) from node_events'
        count = self._execute_sql(sql)
        self.assertEquals(count, ((10L,),))
        self.plug._purge_events_db()
        count = self._execute_sql(sql)
        self.assertEquals(count, ((2L,),))

        # Test when we are at max entries but not over.
        self.plug._purge_events_db()
        self.assertEquals(count, ((2L,),))

    @shared.db.utils.retry
    def test_log_node_event(self):
        """Test logging event with and without custom timestamp."""
        custom_ts = 123
        event = 'default_ts_event'
        self.plug.log_node_event('test_service', event, 'details')
        self.plug.log_node_event('test', 'ts_event',
                                 'event_details', event_ts=custom_ts)
        with self._rw_pool.transaction() as cursor:
            cursor.execute("""SELECT event_ts FROM node_events
                              WHERE event_ts = %s""", (custom_ts,))
            ts_row = cursor.fetchall()
        self.assertEquals(ts_row[0][0], custom_ts)

        with self._rw_pool.transaction() as cursor:
            cursor.execute("""SELECT event FROM node_events
                              WHERE event = %s""", (event,))
            event_row = cursor.fetchall()
        self.assertEquals(event_row[0][0], event)

    def test_get_node_events(self):
        """Test that we retrieve events properly."""
        self.plug.log_node_event('service', 'event', 'details', event_ts=123)
        events = self.plug._get_node_events()
        expected_events = ((1L, 'mockdaemon', 'service', '12345', 'event',
                            'secapps', 12345L, 'details',
                            datetime.datetime(1969, 12, 31, 16, 2, 3)),)
        self.assertEquals(events, expected_events)

    @shared.db.utils.retry
    def _execute_sql(self, sql):
        """Helper function to execute sql."""
        with self._rw_pool.transaction() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    @shared.db.utils.retry
    def _create_table(self):
        """Prepare a table in the database."""
        # If the table already exists the MySQL library is kind enough to print
        # out a warning, so catch that and ignore it.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                'Table .* already exists')

            with self._rw_pool.transaction() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS node_events(
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
                    INDEX node_idx(node_id)
                ) ENGINE = InnoDB DEFAULT CHARSET=latin1;""")


class MockDaemon(object):
    app_name = 'mockdaemon'
    node_id = 12345
    hostname = 'secapps'
    pid = 12345

    def __init__(self):
        self.continue_var = True

    def should_continue(self):
        return self.continue_var

    def shallow_sleep(self, seconds):
        threading.Event().wait(seconds)


if __name__ == '__main__':
    unittest.main()


