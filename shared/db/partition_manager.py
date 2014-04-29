#!/usr/bin/env python
"""DB manager class that adds and removes partitions according to their age.

Initial setup
-------------

All tables should conform to a standard initial configuration:

    - The table must have a partition called `last_partition'. This partition
      will be split in two when adding additional partitions. The last partition
      will contain data less than the maximal value of the table's primary key.

:Author: gmazzola, vburenin
:Version: $Id: //prod/main/_is/shared/python/db/partition_manager.py#1 $
"""

import logging.config
import optparse
import os
import sys
import time

import shared.conf
import shared.db
from shared.db.utils import retry

LAST_PARTITION_NAME = 'last_partition'

class PartitionManager(object):

    def __init__(self, config, rw_pool_name):
        """Constructor.

        :param config: A valid config object. The partition manager expects a
            config in the format specified below. The section headers refer to
            tables that partition_manager will manage. Multiple tables can be
            specified by creating additional sections.

                ;; Specify the table to be partitioned here in the section header.
                [<table_name>]

                ;; The time interval represented by a partition. Example: a
                ;; partition_size of 12h will contain 12 hours worth of data.
                ;; Accepted suffixes are: s, m, h, d.
                partition_size=<size>

                ;; Partions older than this value will be removed. Accepted
                ;; suffixes are: s, m, h, d.
                oldest_partition=<oldest>

                ;; The number of partitions that partition_manager creates ahead
                ;; for this table.
                partitions_ahead=<ahead>

        :param rw_pool_name: Name of the conf section in db.conf.  The only
                parameters that must be specifically set are the rw.host and db.
        """

        self.conf = config
        self.log = logging.getLogger('partition_manager')
        self._rw_pool = shared.db.get_rw_pool(rw_pool_name)

    @retry
    def _get_partitions(self, table):
        """Get the names of partitions for a given table.

        :param table: Look up the partitions for this table.
        :return: A list of partitions enabled for this table.
        """

        query = """SELECT partition_name
                   FROM INFORMATION_SCHEMA.partitions
                   WHERE table_name = %s AND table_schema = DATABASE()"""

        with self._rw_pool.transaction() as cursor:
            cursor.execute(query, (table,))
            data = cursor.fetchall()
            return [row[0] for row in data]

    @retry
    def _create_partition(self, table, partition):
        """Create a table partition.

        :param table: Create the partition in this table.
        :param partition: The partition name to create.
        """

        # remove first letter 'p' in partition name.
        partition_time = int(partition[1:])
        query = """ALTER TABLE %s REORGANIZE PARTITION %s INTO
                  (PARTITION %s VALUES LESS THAN (%d),
                   PARTITION %s VALUES LESS THAN (MAXVALUE))""" % \
                   (table, LAST_PARTITION_NAME, partition, partition_time,
                    LAST_PARTITION_NAME)

        with self._rw_pool.transaction() as cursor:
            cursor.execute(query)

    @retry
    def _drop_partition(self, table, partition):
        """Delete a partition from a table.

        :param table: Delete the partition from this table.
        :param partition: The name of the partition to delete.
        """

        query = """ALTER TABLE %s DROP PARTITION %s""" % (table, partition)
        with self._rw_pool.transaction() as cursor:
            cursor.execute(query)

    @retry
    def _merge_partitions(self, table, source_partitions):
        """Merges the partitions in src into the `last_partition' partition.
           This effectively removes the partitions without deleting the data.

        :param source_partitions: A list of partition names to merge.
        """

        # Need to append `last_partition' into the list of partitions to merge
        if LAST_PARTITION_NAME not in source_partitions:
            source_partitions.append(LAST_PARTITION_NAME)

        partitions = ", ".join(source_partitions)

        query = """ALTER TABLE %s REORGANIZE PARTITION %s INTO
                  (PARTITION %s VALUES LESS THAN (MAXVALUE))""" % \
                   (table, partitions, LAST_PARTITION_NAME)

        with self._rw_pool.transaction() as cursor:
            cursor.execute(query)

    def _update_partitions(self, table, partition_size, oldest_partition,
                          partitions_ahead):
        """Add and remove partitions according to their age.

        :param table: The table name to operate on.
        :param partition_size: The size of an individual partition in seconds.
        :param oldest_partition: The maximum age of a partition in seconds.
        :param partitions_ahead: The number of partitions to create for future data.
        """

        self.log.info('Processing table: %s', table)

        # Fetch the list of partitions for this table.
        partitions = self._get_partitions(table)
        self.log.info('The following partitions were found: %s', partitions)

        # Sanity check: the partition manager requires that a partition called
        # LAST_PARTITION_NAME be configured.
        if LAST_PARTITION_NAME not in partitions:
            raise Exception('The "%s" table is missing the required partition '
                            '"%s".' % (table, LAST_PARTITION_NAME))

        # Build a list of the partitions allocated for future data.
        future_partitions = []

        # Scan the table for old partitions to delete.
        ctime = int(time.time())
        for partition in partitions:
            self.log.info('Processing partition: %s', partition)

            # The partition manager names its partitions using the following
            # naming convention: p<ctime>, where <ctime> refers to the UNIX
            # timestamp when the partition was created.
            if partition.startswith('p'):
                try:
                    ptime = int(partition[1:])
                    if ptime > 0 and ptime < ctime - oldest_partition:
                        self._drop_partition(table, partition)
                        self.log.info('Old partition deleted: %s.%s', table, partition)

                    if ptime > ctime:
                        future_partitions.append(ptime)

                except ValueError:
                    self.log.warning('Incorrect partition name: %s', partition)

        # Remove the first future partition from this list. This partition will contain
        # data, because it includes the current time in its window, and thus it is
        # computationally expensive to modify.
        if future_partitions:
            # Pre-allocate future partitions after the end of this active partition.
            allocate_after = future_partitions.pop(0)
        else:
            # Pre-allocate future partitions after the current time.
            allocate_after = ctime

        # Verify that any previously-created future partitions conform to our current
        # value of partition_size. Without this check _create_partition() could fail.
        # If this check fails, we'll merge the future partitions into last_partition.
        part_size = self.__determine_partition_size(future_partitions)
        if part_size != partition_size and future_partitions:
            self.log.info('Partition size has changed. Merging previously-allocated '
                          'future partitions...')

            merged = ['p%s' % pid for pid in future_partitions]
            self._merge_partitions(table, merged)
            self.log.info('Merged the partitions %s into "%s".' % \
                          (merged, LAST_PARTITION_NAME))

            # Need to update our `partitions' variable to reflect that we just performed
            # partitions -= merged. Note that we reference `partitions' below.
            partitions = [pid for pid in partitions if pid not in merged]

        # Round allocate_after up to the nearest `partition_size' seconds, so we can create
        # partitions for the next `partitions_ahead' time units.
        allocate_after += partition_size - allocate_after % partition_size

        for pid in xrange(partitions_ahead):
            pname = 'p' + str(allocate_after + pid * partition_size)

            if pname not in partitions:
                self._create_partition(table, pname)
                self.log.info('New partition created: %s.%s', table, pname)

    def __determine_partition_size(self, partitions):
        """Determines the partition_size used to create the given partitions.

        :param partitions: A list of integer timestamps with >1 elements.

        :return: The difference between the (i)th and (i-1)th elements in the
            list, or None if that difference is not constant.
        """
        partitions.sort()
        if len(partitions) < 2:
            return None

        partition_size = partitions[1] - partitions[0]

        for i in xrange(1, len(partitions)):
            if partitions[i] - partitions[i-1] != partition_size:
                return None

        return partition_size

    def __convert_time(self, timestamp):
        """Converts a user-specified timestamp to seconds.

        :param timestamp: String in ^\d+(s|m|h|d)$ format. Valid suffixes are:
                     - s: seconds
                     - m: minutes
                     - h: hours
                     - d: days

        :Return: the number of seconds represented by string.
        """

        seconds_per_unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        number = timestamp[:-1]
        suffix = timestamp[-1]

        if suffix in seconds_per_unit:
            return int(number) * seconds_per_unit[suffix]
        else:
            raise Exception('Invalid timestamp: %s' % timestamp)

    def process_all_tables(self):
        """Entry point to process all tables"""

        # We expect that the config [sections] refer to tables that need partitioning.
        tables = self.conf.sections()
        self.log.info('The following tables need to be processed: %s', tables)

        for table in tables:
            size = self.__convert_time(self.conf.get('%s.partition_size' % table))
            oldest = self.__convert_time(self.conf.get('%s.oldest_partition' % table))
            ahead = self.conf.get('%s.partitions_ahead' % table)

            self._update_partitions(table, size, oldest, ahead)


if __name__ == '__main__':
    # PartitionManager initialization:
    # 1) Prepare the OptionParser...
    me = os.path.basename(sys.argv[0])
    parser = optparse.OptionParser(usage='Usage: %s [OPTIONS] <product>' % (me,))

    parser.add_option('-c', '--config-file', metavar='FILE', action='store',
                    type='string', dest='conf_file',
                    help='Load the configuration from FILE. By default, '
                         '%s loads $CONFROOT/partition_manager.conf.' % (me,))

    parser.add_option('-l', '--logging-config-file', metavar='FILE',
                    action='store', type='string', dest='log_file',
                    help='Load the logging configuration from FILE.')

    # 2) ...Get the <product> name...
    (options, args) = parser.parse_args()
    if len(args) == 1:
        product = args[0]
    else:
        sys.stderr.write('Product name must be set.\n')
        sys.stderr.write(parser.format_help())
        sys.exit(1)

    # 3) ...Load the PartitionManager config object...
    if options.conf_file:
        conf = shared.conf.config.Config(options.conf_file).get_snapshot()
    else:
        conf = shared.conf.get_config('partition_manager').get_snapshot()

    # 4) ...Configure logging...
    if options.log_file:
        log_file = options.log_file
        logging.config.fileConfig(log_file)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                            format='%(asctime)s %(levelname)s [%(name)s] - %(message)s')

    # 5) ...Done!
    partman = PartitionManager(conf, product)
    partman.process_all_tables()

