"""This is the client-side API for phalanx.

Phalanx aggregates SBNP and WBNP data by storing meta information about the
log files in a centralized database.  Phalanx takes multiple files
residing on multiple machines and provides access to them as if they were one
file.  As such, clients should treat the PHDistributedReader object as a file handle.
When a request for data is made, meta information retrieved from the database
is used to make an http request for the actual data.

The one prerequisite for using this is defining the following section in your
db.conf file.

[wbnp_data_queue]
db_type=mysql
pool_size=eval(1)
idle_timeout=eval(int(10))
timeout=eval(3)
log_queries=eval(False)
db=wbnp_phalanx
rw.host=somedb.ironport.com
rw.user=write_username
rw.password=write_password

Note that the db and rw.host variables must match the production phalanx
system.  The conf section name can be anything, but must be passed in to the
constructor of PHDistributedReader.

For usage and other details, see the docstrings.

$Id: //prod/main/_is/shared/python/phonehome/distributedreader.py#5 $
:Authors: brianz, vburenin
$DateTime: 2011/11/11 12:27:48 $
"""

import logging
import time
import urllib2

from shared.db import dbcp
from shared.dbqueue import mcqsubscriber


class PHLogChunk(object):

    """A data structure representing a chunk of data as defined by Phalanx."""

    def __init__(self, hostport, file_path, start_b, end_b):
        self.hostport = hostport
        self.file_path = file_path
        self.start_b = int(start_b)
        self.end_b = int(end_b)

    def __len__(self):
        """Return the size of the chunk in bytes"""
        return int(self.end_b - self.start_b)

    def __repr__(self):
        return '<%s %s %s %s %s>' % (self.__class__.__name__, self.hostport,
                                    self.file_path, self.start_b, self.end_b)


class PHDistributedReader(object):

    """Phone Home Log Importer class.

    It supports iteration around the PH logs, seeking amongst the collective
    set of logs and raw data access.
    """

    def __init__(self, reader_name, subscriber_db_conf_section,
                 publisher_db_conf_section, queue_name, db_conf_file_name=None,
                 status_refresh_secs=60):
        """Constructor.

        :param name: The name of the application/reader.  To parallelize
                     processing of a set of log files, create multiple
                     instances of processing code with identical names.
                     Data access is synchronous across multiple readers of
                     the same name.
        :param subscriber_db_conf_section:
                    Name of the conf section in db.conf.  The only parameters
                    which must be specifically set are the rw.host and db.
                    Reader pointer data will be store in that DB in the
                    queue_state table.
        :param publisher_db_conf_section:
                    Name of the conf section in db.conf.  The only parameters
                    which must be specifically set are the ro.host and db.
                    Data about publisher pointer is required for multicast
                    queue reader.
        :param queue_name:
                    The name of the data queue to read from.  Again, this must
                    match what is in the production phalanx system.
        :param db_conf_file_name: configuration file name. Default file name
                    will be used if None.
        :param status_refresh_secs:
                    The number of seconds to wait in between queries to the
                    database to determine how far behind the reader is from the
                    end of the queue.  Since this calculation has many db
                    queries, it should be set to some resonable value
                    (default=60).
        """

        self.__reader_name = reader_name

        # Status variables.
        self.__status_refresh = status_refresh_secs
        self.__last_status_ts = -1
        self.__items_behind = -1
        self.__secs_behind = -1

        self.__log = logging.getLogger('PHDistributedReader')
        pool_manager = dbcp.DBPoolManager(db_conf_file_name)
        sub_rw_pool = pool_manager.get_rw_pool(subscriber_db_conf_section)
        pub_ro_pool = pool_manager.get_ro_pool(publisher_db_conf_section)

        self.__reader = mcqsubscriber.MulticastQueueSubscriber(
            queue_name, self.__reader_name, sub_rw_pool, pub_ro_pool)
        self.__reader.init_queue_state()

        self.__packet_iter = None

    def __iter__(self):
        """Used to support iteration returning the next chunk of data."""
        return self

    def next(self):
        """Get next packet of data.

        :return: A string representing one packet (three lines) of PH log data.

        :exception: Throw StopIteration when no new data is immediately
                    available.
        :exception: Throw IOError if the HTTP request for data was
                    unsuccessful.
        """
        if self.__packet_iter:
            try:
                return self.__packet_iter.next()
            except StopIteration:

                # End of that packet iterator, build it again and start over
                # from the next chunk.

                self.__log.debug('End of packet reached')
                self.__packet_iter = None

        chunk_meta = self._get_chunk_meta()
        # Now if we're really at the end of the queue, say so.
        if not chunk_meta:
            raise StopIteration('End of queue reached')

        self.__log.debug('Begin reading of new chunk: %s', chunk_meta)
        self.__packet_iter = self._get_packets(chunk_meta)

        return self.__packet_iter.next()

    def seek(self, location):
        """Seek to a particular location the PH log files.

        :param location: A tuple as returned by tell().

        :exception: Throw OutOfRangeError is the given location is invalid.
        """
        self.__reader.seek(location)

    def seek_to_start(self):
        """Seek to the beginning of the PH log files."""
        self.__reader.seek(0)

    def seek_to_end(self):
        """Seek to the end of the PH log files."""
        self.__reader.seek(-1)

    def seek_to_ts(self, timestamp):
        """Seek to an approximate location based on timestamp.

        :param timestamp: Number of seconds representing the time that a
                          chunk of data was added.
        """
        self.__reader.seek_to_ts(timestamp)

    def tell(self):
        """Return a tuple describing current read location.

        :return: A tuple representing the current read location.
        """
        return self.__reader.tell()

    @property
    def items_behind(self):
        """Items behind from the front of the queue."""
        self.__refresh_status()
        return self.__items_behind

    @property
    def seconds_behind(self):
        """Number of seconds behind from the front of the queue."""
        self.__refresh_status()
        return self.__secs_behind

    def _build_request(self, phchunk):
        url = 'http://%s/%s' % (phchunk.hostport, phchunk.file_path)
        req = urllib2.Request(url=url)
        req.add_header('Host', phchunk.hostport)
        req.add_header('Range',
                       'bytes=%d-%d' % (phchunk.start_b, phchunk.end_b))
        return req

    def _get_packets(self, phchunk):
        """An iterator which returns packets given a chunk object.

        Packet is a three lines of PHLog data.

        :param phchunk: A PHLogChunk object
        :yield: A packet of data as a string
        """
        try:
            packet = []
            response = urllib2.urlopen(self._build_request(phchunk))
            for i, data in enumerate(response):
                if not data.strip():
                    continue

                packet.append(data)

                # Yield three packet together.
                if (i + 1) % 3 == 0:
                    yield ''.join(packet)
                    packet = []

        except urllib2.URLError, err:
            raise IOError(err)

    def _get_chunk_meta(self):
        """Get a PHLogChunk collection of meta data about a chunk of PH data.

        :return: A PHLogChunk object or None if there is no data available.
        """
        data = self.__reader.pop(1)
        if not data:
            return None

        hostport, file_path, start_b, end_b = data[0]
        return PHLogChunk(hostport, file_path, start_b, end_b)

    def __refresh_status(self):
        """Refresh current status.

        This call is heavy, so caching is used to reduce database load.
        """
        now = time.time()
        if now - self.__last_status_ts > self.__status_refresh:
            self.__log.debug('Refreshing status information')
            self.__last_status_ts = now
            self.__items_behind, self.__secs_behind = self.__reader.read_stat()
