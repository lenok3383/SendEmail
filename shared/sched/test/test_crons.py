#!/usr/local/bin/python
"""Unittests for crons module.

:Status: $Id: //prod/main/_is/shared/python/sched/test/test_crons.py#6 $
:Authors: rbodnarc
"""

import copy
import datetime
import random
import re
import shlex
import socket
import time
import unittest2 as unittest

from shared.sched import crons
from shared.util import timeutils


class _CronQueriesMockup(object):

    """Database mockup."""

    def __init__(self, db_name=None, crons_data=None, crons_local_data=None,
                 crons_history_data=None):
        """Create _CronsQueriesMockup instance.

        :param db_name: Database name, used by original _CronsQueries class.
        :param crons_data: Dictionary with data for 'crons' table.
        :param crons_history_data: List with data for 'crons_history'
                                   table.
        """
        self._crons = crons_data
        self._crons_local = crons_local_data
        self._crons_history = crons_history_data

    def __call__(self, *args, **kwargs):
        return self

    def commands_list(self):
        return self._crons.keys()

    def commands_list_by_hostname(self, hostname, command_pattern):
        return [c['command'] for c in
                                 list(self._crons.itervalues()) +
                                 list(self._crons_local.itervalues())
                             if c['hostname'] == hostname and
                                c['mon_lock'] is not None and
                                re.match(command_pattern, c['command'])]

    def command_info(self, columns, command):
        cron = self._crons.get(command)
        if cron:
            return [self._crons[command][column] for column in columns]

    def lock(self, mon_lock, lastrunstart, hostname, command,
             lastrunstart_restr, command_pattern, command_max_count):
        if command_pattern is not None and \
           len([c['command'] for c in
                                 list(self._crons.itervalues()) +
                                 list(self._crons_local.itervalues())
                             if c['hostname'] == hostname and
                                c['mon_lock'] is not None and
                                re.match(command_pattern, c['command'])])\
            >= command_max_count:
            return 0
        cron = self._crons.get(command)
        if cron:
            if cron['mon_lock'] is None and \
               cron['lastrunstart'] == lastrunstart_restr:

                cron['mon_lock'] = mon_lock
                cron['lastrunstart'] = lastrunstart
                cron['hostname'] = hostname
                return 1
        return 0

    def clear(self, command):
        self._crons[command]['lastrunstart'] = None
        self._crons[command]['lastrunend'] = None
        self._crons[command]['mon_lock'] = None
        self._crons[command]['fail_count'] = 0

    def finish_success(self, lastrunend, lastsuccess, result, command):
        cron = self._crons.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['lastsuccess'] = lastsuccess
            cron['result'] = result
            cron['fail_count'] = 0
            cron['mon_lock'] = None

    def finish_fail(self, lastrunend, result, command):
        cron = self._crons.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['result'] = result
            cron['fail_count'] += 1

    def finish_fail_soft(self, lastrunend, result, lastrunstart, command):
        cron = self._crons.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['result'] = result
            cron['mon_lock'] = None
            cron['lastrunstart'] = lastrunstart
            cron['fail_count'] += 1

    def history_check(self):
        return 'crons_history'

    def history_create(self):
        pass

    def history_log(self, command, lastrunstart, hostname, exectime, result):
        self._crons_history.append({'command' : command,
                                    'lastrunstart' : lastrunstart,
                                    'hostname' : hostname,
                                    'exectime' : exectime,
                                    'result' : result})

    def history_report(self, lastrunstart_begin, lastrunstart_end,
                       command=None):
        return [[history['command'], history['lastrunstart'],
                 history['exectime'], history['result']]
                for history in self._crons_history
                if history['lastrunstart'] >= lastrunstart_begin and
                   history['lastrunstart'] <= lastrunstart_end and
                   (command is None or history['command'] == command)]

    def history_expire(self, command, lastrunstart):
        self._crons_history[:] = [history for history in self._crons_history
                                          if history['command'] != command or
                                        history['lastrunstart'] > lastrunstart]


class _CronQueriesLocalMockup(_CronQueriesMockup):

    def command_info(self, columns, command):
        cron = self._crons_local.get(command)
        if cron:
            return [self._crons_local[command][column] for column in columns]
        else:
            cron = self._crons.get(command)
            if cron:
                return [self._crons[command][column] for column in columns]


    def lock(self, mon_lock, lastrunstart, hostname, command,
             lastrunstart_restr, command_pattern, command_max_count):
        if command_pattern is not None and \
           len([c['command'] for c in
                                 list(self._crons.itervalues()) +
                                 list(self._crons_local.itervalues())
                             if c['hostname'] == hostname and
                                c['mon_lock'] is not None and
                                re.match(command_pattern, c['command'])])\
            >= command_max_count:
            return 0

        cron = self._crons.get(command)
        if cron:
            if command not in self._crons_local:
                self._crons_local[command] = cron.copy()

            cron = self._crons_local[command]

            if cron['mon_lock'] is None and \
               cron['lastrunstart'] == lastrunstart_restr:

                cron['mon_lock'] = mon_lock
                cron['lastrunstart'] = lastrunstart
                cron['hostname'] = hostname
                return 1
        return 0

    def clear(self, command):
        self._crons_local[command]['lastrunstart'] = None
        self._crons_local[command]['lastrunend'] = None
        self._crons_local[command]['mon_lock'] = None
        self._crons_local[command]['fail_count'] = 0

    def finish_success(self, lastrunend, lastsuccess, result, command):
        cron = self._crons_local.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['lastsuccess'] = lastsuccess
            cron['result'] = result
            cron['fail_count'] = 0
            cron['mon_lock'] = None

    def finish_fail(self, lastrunend, result, command):
        cron = self._crons_local.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['result'] = result
            cron['fail_count'] += 1

    def finish_fail_soft(self, lastrunend, result, lastrunstart, command):
        cron = self._crons_local.get(command)
        if cron:
            cron['lastrunend'] = lastrunend
            cron['result'] = result
            cron['mon_lock'] = None
            cron['lastrunstart'] = lastrunstart
            cron['fail_count'] += 1



HOST = socket.gethostname()

# Data for 'crons' table.
CRONS = {'cron_job' : {'command' : 'cron_job',
                       'mon_lock' : None,
                       'day_of_month' : None,
                       'day_of_week' : None,
                       'hour' : None,
                       'minute' : None,
                       'frequency' : 60,
                       'lastrunstart' : None,
                       'lastrunend' : None,
                       'lastsuccess' : None,
                       'result' : None,
                       'hostname' : None,
                       'description' : None,
                       'fail_count' : 0,
                       'max_allowed_fails' : 0,
                       'fail_option' : 'all_soft',
                       'fail_codes' : None,
                       'retry_interval' : 0},

         'cron_job2' : {'command' : 'cron_job2',
                        'mon_lock' : None,
                        'day_of_month' : None,
                        'day_of_week' : None,
                        'hour' : None,
                        'minute' : None,
                        'frequency' : None,
                        'lastrunstart' : None,
                        'lastrunend' : None,
                        'lastsuccess' : None,
                        'result' : None,
                        'hostname' : None,
                        'description' : None,
                        'fail_count' : 0,
                        'max_allowed_fails' : 0,
                        'fail_option' : 'all_soft',
                        'fail_codes' : None,
                        'retry_interval' : 0}
}

CRONS_LOCAL = {'cron_job' : {'command' : 'cron_job',
                             'mon_lock' : None,
                             'day_of_month' : None,
                             'day_of_week' : None,
                             'hour' : None,
                             'minute' : None,
                             'frequency' : 60,
                             'lastrunstart' : None,
                             'lastrunend' : None,
                             'lastsuccess' : None,
                             'result' : None,
                             'hostname' : HOST,
                             'description' : None,
                             'fail_count' : 0,
                             'max_allowed_fails' : 0,
                             'fail_option' : 'all_soft',
                             'fail_codes' : None,
                             'retry_interval' : 0}}

CMD = 'echo'
BAD_CMD = 'echhoo'



class TestCrons(unittest.TestCase):

    """Test case for CronJob, CronHistory and CronReport classes."""

    def setUp(self):
        self._timeshift = 0
        self._timestamp = 0

        self._now = time.time()
        self._time_orig = crons.time.time
        crons.time.time = self._time

        self._crons_data = copy.deepcopy(CRONS)
        self._crons_local_data = copy.deepcopy(CRONS_LOCAL)
        self._crons_history_data = list()

        self._queries_orig = crons._CronQueries
        crons._CronQueries = _CronQueriesMockup(
                            crons_data=self._crons_data,
                            crons_local_data=self._crons_local_data,
                            crons_history_data=self._crons_history_data)
        self._queries_local_orig = crons._CronQueriesLocal
        crons._CronQueriesLocal = _CronQueriesLocalMockup(
                            crons_data=self._crons_data,
                            crons_local_data=self._crons_local_data,
                            crons_history_data=self._crons_history_data)

        self._subprocess_call_original = crons.subprocess.call
        crons.subprocess.call = self._subprocess_call

    def tearDown(self):
        crons.time.time = self._time_orig
        crons._CronQueries = self._queries_orig
        crons._CronQueriesLocal = self._queries_local_orig
        crons.subprocess.call = self._subprocess_call_original
        reload(crons)

    def _time(self):
        """time.time() function's mockup. Returns our specified value."""
        self._timestamp += self._timeshift
        return self._timestamp

    def _subprocess_call(self, cmd, *args):
        """subprocess.call() function's mockup. Returns our specified value."""
        if cmd == shlex.split(CMD):
            return 0
        elif cmd == shlex.split(BAD_CMD):
            return 123

        return 1

    def _dt(self, *args):
        """Converts year, month, day etc into unix timestamp."""
        return time.mktime(datetime.datetime(*args).timetuple())

    ###############
    # Test clear()
    ###############

    def test_clear_default(self):
        cron_job = crons.CronJob('db', 'cron_job')

        cron_job.clear()

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 0)


    def test_clear_with_data(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._crons_data['cron_job']['mon_lock'] = '<lock>'
        self._crons_data['cron_job']['fail_count'] = 3
        self._crons_data['cron_job']['lastrunstart'] = 123
        self._crons_data['cron_job']['lastrunend'] = 1234

        cron_job.clear()

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 0)

    def test_clear_default_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        cron_job.clear()

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_local_data['cron_job']['fail_count'], 0)

    ###############
    # Test status()
    ###############

    def test_status_default(self):
        cron_job = crons.CronJob('db', 'cron_job')
        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], None)

    def test_status_default_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)
        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], HOST)

    def test_status_locked_with_hostname(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        self._crons_data['cron_job']['lastrunstart'] = None
        self._crons_data['cron_job']['mon_lock'] = '<lock>'
        self._crons_data['cron_job']['hostname'] = 'cool_host'

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_LOCKED)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], 'cool_host')

    def test_status_locked_without_hostname(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        self._crons_data['cron_job']['lastrunstart'] = None
        self._crons_data['cron_job']['mon_lock'] = '<lock>'
        self._crons_data['cron_job']['hostname'] = None

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_LOCKED)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], None)

    def test_status_locked_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        self._crons_local_data['cron_job']['mon_lock'] = '<lock>'

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_LOCKED)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], HOST)

    def test_status_notyet_freq_1(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        self._crons_data['cron_job']['lastrunstart'] = 250
        self._crons_data['cron_job']['frequency'] = 60

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], 250)
        self.assertEquals(status['hostname'], None)

    def test_status_notyet_freq_2(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        self._crons_data['cron_job']['lastrunstart'] = 0
        self._crons_data['cron_job']['frequency'] = 360

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], 0)
        self.assertEquals(status['hostname'], None)

    def test_status_ready_freq_1(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 1000
        self._crons_data['cron_job']['lastrunstart'] = 270
        self._crons_data['cron_job']['frequency'] = 600

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], 270)
        self.assertEquals(status['hostname'], None)

    def test_status_ready_freq_2(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        self._crons_data['cron_job']['lastrunstart'] = None
        self._crons_data['cron_job']['frequency'] = 360

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], None)

    def test_status_notyet_start_time_1(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 1, 0, 2)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1,
                                                                 1, 0, 2)
        self._crons_data['cron_job2']['minute'] = 0
        self._crons_data['cron_job2']['hour'] = 0

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 1, 0, 2))
        self.assertEquals(status['hostname'], None)

    def test_status_notyet_start_time_2(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 3, 23)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 3)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = None
        self._crons_data['cron_job2']['day_of_week'] = 4

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 3))
        self.assertEquals(status['hostname'], None)

    def test_status_notyet_start_time_3(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 9)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 6)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = None
        self._crons_data['cron_job2']['day_of_week'] = None
        self._crons_data['cron_job2']['day_of_month'] = 10

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 6))
        self.assertEquals(status['hostname'], None)

    def test_status_notyet_start_time_4(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 5, 1, 59)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 3)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = 2
        self._crons_data['cron_job2']['day_of_week'] = None
        self._crons_data['cron_job2']['day_of_month'] = 5

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_NOTYET)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 3))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_0(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 5, 1, 59)
        self._crons_data['cron_job2']['lastrunstart'] = None
        self._crons_data['cron_job2']['minute'] = 0
        self._crons_data['cron_job2']['hour'] = 0

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], None)
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_1(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 2, 1, 1)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 1)
        self._crons_data['cron_job2']['minute'] = 0
        self._crons_data['cron_job2']['hour'] = 0

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 1))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_2(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 8, 12, 1)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 3)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = None
        # 8.1.1970, same as 1.1.1970 was Thursday, fourth day of week
        self._crons_data['cron_job2']['day_of_week'] = 4

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 3))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_3(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 10, 2)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 6)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = None
        self._crons_data['cron_job2']['day_of_week'] = None
        self._crons_data['cron_job2']['day_of_month'] = 10

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 6))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_4(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 5, 3)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 5, 1)
        self._crons_data['cron_job2']['minute'] = None
        self._crons_data['cron_job2']['hour'] = 2
        self._crons_data['cron_job2']['day_of_week'] = None
        self._crons_data['cron_job2']['day_of_month'] = 5

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 5, 1))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_start_time_5(self):
        cron_job = crons.CronJob('db', 'cron_job2')

        self._timestamp = self._dt(1970, 1, 1, 2, 2)
        self._crons_data['cron_job2']['lastrunstart'] = self._dt(1970, 1, 1)
        self._crons_data['cron_job2']['minute'] = 1
        self._crons_data['cron_job2']['hour'] = 2
        self._crons_data['cron_job2']['day_of_week'] = 4
        # be sure that day of week suppresses day of month
        self._crons_data['cron_job2']['day_of_month'] = 8

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 1))
        self.assertEquals(status['hostname'], None)

    def test_status_ready_local(self):
        # All the logic should be tested by tests above, just a sanity check
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        self._timestamp = self._dt(1970, 1, 1, 2, 2)
        self._crons_local_data['cron_job']['lastrunstart'] = self._dt(1970, 1, 1)
        self._crons_local_data['cron_job']['minute'] = 1
        self._crons_local_data['cron_job']['hour'] = 2
        self._crons_local_data['cron_job']['day_of_week'] = 4
        # be sure that day of week suppresses day of month
        self._crons_local_data['cron_job']['day_of_month'] = 8

        status = cron_job.status()

        self.assertEquals(status['status'], crons.STATUS_READY)
        self.assertEquals(status['lastrunstart'], self._dt(1970, 1, 1))
        self.assertEquals(status['hostname'], HOST)


    ############
    # Test run()
    ############

    def test_run_normal(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], 300)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 300)

    def test_run_normal_2(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._crons_data['cron_job']['lastrunstart'] = 200
        self._crons_data['cron_job']['frequency'] = 60

        self._timestamp = 290

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 290)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 290)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], 290)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 290)

    def test_run_normal_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        self._crons_local_data['cron_job']['lastrunstart'] = 200
        self._crons_local_data['cron_job']['frequency'] = 60

        self._timestamp = 290

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['lastsuccess'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 290)

    def test_run_mail_cron_locked_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        self._crons_data['cron_job']['mon_lock'] = '<lock'
        self._crons_local_data['cron_job']['lastrunstart'] = 200
        self._crons_local_data['cron_job']['frequency'] = 60

        self._timestamp = 290

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['lastsuccess'], 290)
        self.assertEquals(self._crons_local_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 290)

    def test_run_normal_no_cron_local(self):
        cron_job = crons.CronJob('db', 'cron_job2', local=True)

        self._crons_data['cron_job2']['lastrunstart'] = 200
        self._crons_data['cron_job2']['frequency'] = 60

        self._timestamp = 290

        self.assertTrue('cron_job2' not in self._crons_local_data)

        result = cron_job.run(CMD)

        self.assertTrue('cron_job2' in self._crons_local_data)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_local_data['cron_job2']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job2']['lastrunstart'], 290)
        self.assertEquals(self._crons_local_data['cron_job2']['lastrunend'], 290)
        self.assertEquals(self._crons_local_data['cron_job2']['lastsuccess'], 290)
        self.assertEquals(self._crons_local_data['cron_job2']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job2']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job2')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 290)

    def test_run_force(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._crons_data['cron_job']['lastrunstart'] = 300
        self._crons_data['cron_job']['frequency'] = 60

        self._timestamp = 330

        result = cron_job.run(CMD, force=True)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 330)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 330)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], 330)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 330)

    def test_run_force_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)

        self._crons_local_data['cron_job']['lastrunstart'] = 300
        self._crons_local_data['cron_job']['frequency'] = 60

        self._timestamp = 330

        result = cron_job.run(CMD, force=True)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], 330)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], 330)
        self.assertEquals(self._crons_local_data['cron_job']['lastsuccess'], 330)
        self.assertEquals(self._crons_local_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 330)

    def test_run_command_class_1(self):
        cron_job = crons.CronJob('db', 'cron_job', command_class='cron_job.*',
                                 command_max=1)

        start_history_len = len(self._crons_history_data)

        self._crons_data['cron_job']['frequency'] = 60
        self._crons_data['cron_job']['lastrunstart'] = 200
        self._crons_data['cron_job']['mon_lock'] = None

        self._crons_data['cron_job2']['mon_lock'] = '<lock>'
        self._crons_data['cron_job2']['hostname'] = HOST

        self._timestamp = 330

        result = cron_job.run(CMD)

        self.assertEquals(crons.STATUS_LOCKED, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 200)
        self.assertEquals(len(self._crons_history_data), start_history_len)

    def test_run_command_class_2(self):
        cron_job = crons.CronJob('db', 'cron_job', command_class='cron_job.*',
                                 command_max=2)

        start_history_len = len(self._crons_history_data)

        self._crons_data['cron_job']['frequency'] = 60
        self._crons_data['cron_job']['lastrunstart'] = 200
        self._crons_data['cron_job']['mon_lock'] = None

        self._crons_data['cron_job2']['mon_lock'] = '<lock>'
        self._crons_data['cron_job2']['hostname'] = HOST

        self._timestamp = 330

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 330)
        self.assertEquals(len(self._crons_history_data), start_history_len + 1)

    def test_run_command_class_local(self):
        cron_job = crons.CronJob('db', 'cron_job', command_class='cron_job.*',
                                 command_max=2, local=True)

        start_history_len = len(self._crons_history_data)

        self._crons_local_data['cron_job']['frequency'] = 60
        self._crons_local_data['cron_job']['lastrunstart'] = 200
        self._crons_data['cron_job2']['mon_lock'] = None

        self._crons_data['cron_job2']['mon_lock'] = '<lock>'
        self._crons_data['cron_job2']['hostname'] = HOST

        self._timestamp = 330

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], 330)
        self.assertEquals(len(self._crons_history_data), start_history_len + 1)

    def test_run_instance(self):
        self._crons_data['cron_job-0'] = copy.deepcopy(
                                                self._crons_data['cron_job'])
        self._crons_data['cron_job-0']['mon_lock'] = '<lock>'

        self._crons_data['cron_job-1'] = copy.deepcopy(
                                                self._crons_data['cron_job'])

        cron_job = crons.CronJob('db', 'cron_job')

        self._crons_data['cron_job-1']['lastrunstart'] = 200
        self._crons_data['cron_job-1']['frequency'] = 60

        self._timestamp = 330

        result = cron_job.run(CMD, instance=True)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job-1']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job-1']['lastrunstart'], 330)
        self.assertEquals(self._crons_data['cron_job-1']['lastrunend'], 330)
        self.assertEquals(self._crons_data['cron_job-1']['lastsuccess'], 330)
        self.assertEquals(self._crons_data['cron_job-1']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job-1']['result'],
                          self._crons_history_data[-1]['result'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job-1')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 330)

    def test_run_locked(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._crons_data['cron_job']['mon_lock'] = '<lock>'

        start_len = len(self._crons_history_data)

        result = cron_job.run(CMD)

        self.assertEquals(crons.STATUS_LOCKED, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], '<lock>')
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], None)
        self.assertEquals(self._crons_data['cron_job']['hostname'], None)
        self.assertEquals(self._crons_data['cron_job']['result'], None)

        self.assertEquals(len(self._crons_history_data), start_len)

    def test_run_locked_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)
        self._crons_local_data['cron_job']['mon_lock'] = '<lock>'

        start_len = len(self._crons_history_data)

        result = cron_job.run(CMD)

        self.assertEquals(crons.STATUS_LOCKED, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'], '<lock>')
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_local_data['cron_job']['lastsuccess'], None)
        self.assertEquals(self._crons_local_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job']['result'], None)

        self.assertEquals(len(self._crons_history_data), start_len)

    def test_run_not_yet(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._crons_data['cron_job']['lastrunstart'] = 200
        self._crons_data['cron_job']['frequency'] = 600

        self._timestamp = 340

        start_len = len(self._crons_history_data)

        result = cron_job.run(CMD)

        self.assertEquals(crons.STATUS_NOTYET, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 200)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], None)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], None)
        self.assertEquals(self._crons_data['cron_job']['hostname'], None)
        self.assertEquals(self._crons_data['cron_job']['result'], None)

        self.assertEquals(len(self._crons_history_data), start_len)

    def test_run_fail_default(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 200

        self._crons_data['cron_job']['lastsuccess'] = 100

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'],
                          '<%s> <%s>' % (HOST, time.ctime(200)))
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 200)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 200)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], 100)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'],
                          123)

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 200)

    def test_run_fail_default_local(self):
        cron_job = crons.CronJob('db', 'cron_job', local=True)
        self._timestamp = 200

        self._crons_local_data['cron_job']['lastsuccess'] = 100

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_local_data['cron_job']['mon_lock'],
                          '<%s> <%s>' % (HOST, time.ctime(200)))
        self.assertEquals(self._crons_local_data['cron_job']['lastrunstart'], 200)
        self.assertEquals(self._crons_local_data['cron_job']['lastrunend'], 200)
        self.assertEquals(self._crons_local_data['cron_job']['lastsuccess'], 100)
        self.assertEquals(self._crons_local_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_local_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'],
                          123)
        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 200)

    def test_run_fail_soft_fail_count_1(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['frequency'] = 50
        self._crons_data['cron_job']['fail_option'] = 'all_soft'
        self._crons_data['cron_job']['fail_count'] = 0
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], None)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 1)
        self.assertEquals(self._crons_data['cron_job']['max_allowed_fails'], 3)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'],
                          result)

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['exectime'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 300)

    def test_run_fail_soft_fail_count_2(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['frequency'] = 50
        self._crons_data['cron_job']['fail_option'] = 'all_soft'
        self._crons_data['cron_job']['fail_count'] = 1
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 2)

    def test_run_fail_soft_fail_count_3(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['frequency'] = 250
        self._crons_data['cron_job']['fail_option'] = 'all_soft'
        self._crons_data['cron_job']['fail_count'] = 1
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 170)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 2)

    def test_run_fail_codes_1(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_soft'
        self._crons_data['cron_job']['fail_codes'] = '1, 2, 123'
        self._crons_data['cron_job']['fail_count'] = 0
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 1)

    def test_run_fail_codes_2(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_soft'
        self._crons_data['cron_job']['fail_codes'] = '1, 2'
        self._crons_data['cron_job']['fail_count'] = 0
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 1)

    def test_run_fail_codes_3(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_soft'
        self._crons_data['cron_job']['fail_codes'] = '1, 2, 123'
        self._crons_data['cron_job']['fail_count'] = 2
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 3)

    def test_run_fail_codes_4(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_hard'
        self._crons_data['cron_job']['fail_codes'] = '1, 2, 123'
        self._crons_data['cron_job']['fail_count'] = 1
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 2)

    def test_run_fail_codes_5(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_hard'
        self._crons_data['cron_job']['fail_codes'] = '1, 2'
        self._crons_data['cron_job']['fail_count'] = 2
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 3)

    def test_run_fail_codes_6(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'check_hard'
        self._crons_data['cron_job']['fail_codes'] = '1, 2'
        self._crons_data['cron_job']['fail_count'] = 1
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 2)

    def test_run_fail_codes_7(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 300

        self._crons_data['cron_job']['fail_option'] = 'all_hard'
        self._crons_data['cron_job']['fail_count'] = 0
        self._crons_data['cron_job']['max_allowed_fails'] = 3

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 300)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 300)
        self.assertEquals(self._crons_data['cron_job']['fail_count'], 1)

    def test_run_exectime(self):
        cron_job = crons.CronJob('db', 'cron_job')
        self._timestamp = 350
        self._timeshift = 5

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['mon_lock'], None)
        self.assertEquals(self._crons_data['cron_job']['hostname'], HOST)
        self.assertEquals(self._crons_data['cron_job']['result'],
                          self._crons_history_data[-1]['result'])
        self.assertEquals(self._crons_data['cron_job']['lastrunend'],
                          self._crons_data['cron_job']['lastsuccess'])

        self.assertTrue(self._crons_data['cron_job']['lastrunend'] >
                        self._crons_data['cron_job']['lastrunstart'])

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'],
                          self._crons_data['cron_job']['lastrunstart'])
        self.assertEquals(self._crons_data['cron_job']['lastrunend'] -
                          self._crons_history_data[-1]['lastrunstart'],
                          self._crons_history_data[-1]['exectime'])

        self.assertTrue(self._crons_history_data[-1]['exectime'] > 0)

    def test_run_expire_success(self):
        cron_job = crons.CronJob('db', 'cron_job', history_expire_time=300)
        self._timestamp = 900

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 450,
                                         'hostname' : 'some_host',
                                         'exectime' : 1,
                                         'result' : 0})

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 750,
                                         'hostname' : 'some_host',
                                         'exectime' : 2,
                                         'result' : 0})

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 550,
                                         'hostname' : 'some_host',
                                         'exectime' : 1,
                                         'result' : 0})

        result = cron_job.run(CMD)

        self.assertEquals(crons.RETURN_SUCCESS, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 900)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 900)
        self.assertEquals(self._crons_data['cron_job']['lastsuccess'], 900)

        # 2 deleted, 1 added
        self.assertEquals(len(self._crons_history_data), 2)

    def test_run_expire_fail(self):
        cron_job = crons.CronJob('db', 'cron_job', history_expire_time=300)
        self._timestamp = 900

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 450,
                                         'hostname' : 'some_host',
                                         'exectime' : 1,
                                         'result' : 0})

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 750,
                                         'hostname' : 'some_host',
                                         'exectime' : 2,
                                         'result' : 0})

        self._crons_history_data.append({'command' : 'cron_job',
                                         'lastrunstart' : 550,
                                         'hostname' : 'some_host',
                                         'exectime' : 1,
                                         'result' : 0})

        result = cron_job.run(BAD_CMD)

        self.assertEquals(crons.RETURN_FAILURE, result)

        self.assertEquals(self._crons_data['cron_job']['lastrunstart'], 900)
        self.assertEquals(self._crons_data['cron_job']['lastrunend'], 900)

        # 1 added, nothing deleted
        self.assertEquals(len(self._crons_history_data), 4)

    def test_run_history_unknown(self):
        cron_job = crons.CronJob('db', 'cron_job_unknown')

        self._timestamp = 300
        start_len = len(self._crons_history_data)

        cron_job.run(CMD, history_only=True)

        self.assertEquals(len(self._crons_history_data), start_len + 1)

        self.assertEquals(self._crons_history_data[-1]['command'],
                          'cron_job_unknown')
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 300)
        self.assertEquals(self._crons_history_data[-1]['result'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)

    def test_run_history_known(self):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = 300
        start_len = len(self._crons_history_data)

        cron_job.run(CMD, history_only=True)

        self.assertEquals(len(self._crons_history_data), start_len + 1)

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 300)
        self.assertEquals(self._crons_history_data[-1]['result'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)

    def test_run_history_expire(self):
        cron_job = crons.CronJob('db', 'cron_job')
        start_len = len(self._crons_history_data)

        self._timestamp = 300
        self.assertEquals(crons.RETURN_SUCCESS, cron_job.run(CMD, history_only=True))
        self.assertEquals(crons.RETURN_SUCCESS, cron_job.run(CMD, history_only=True))
        self.assertEquals(crons.RETURN_SUCCESS, cron_job.run(CMD, history_only=True))

        self.assertEquals(len(self._crons_history_data), start_len + 3)

        self._timestamp = 900
        cron_job = crons.CronJob('db', 'cron_job', history_expire_time=300)

        self.assertEquals(crons.RETURN_SUCCESS, cron_job.run(CMD))

        self.assertEquals(len(self._crons_history_data), start_len + 1)

        self.assertEquals(self._crons_history_data[-1]['command'], 'cron_job')
        self.assertEquals(self._crons_history_data[-1]['lastrunstart'], 900)
        self.assertEquals(self._crons_history_data[-1]['result'], 0)
        self.assertEquals(self._crons_history_data[-1]['hostname'], HOST)

    ########################
    # Test CronReport class
    ########################

    def _prepare_report(self, errors=False):
        cron_job = crons.CronJob('db', 'cron_job')

        self._timestamp = self._now - timeutils.ONEDAY * 10
        for _ in range(19):
            cron_job.run(CMD, history_only=True)
            self._timeshift = random.randint(0, 5)
        if errors:
            cron_job.run(BAD_CMD, history_only=True)
        else:
            cron_job.run(CMD, history_only=True)

        self._timestamp = self._now - timeutils.ONEDAY * 5

        cron_job = crons.CronJob('db', 'cron_job2')

        for _ in range(19):
            cron_job.run(CMD, history_only=True)
            self._timeshift = random.randint(0, 5)

        if errors:
            cron_job.run(BAD_CMD, history_only=True)
        else:
            cron_job.run(CMD, history_only=True)

    def test_report_1(self):
        self._crons_history_data[:] = list()
        self._prepare_report(errors=False)

        self.assertEquals(len(self._crons_history_data), 40)

        self._timestamp = self._now

        cron_report = crons.CronReport('db', report_begin=12)
        report = cron_report.report()

        self.assertTrue('cron_job' in report)
        self.assertTrue('cron_job2' in report)

        for rep in report.itervalues():
            self.assertTrue('days' in rep)
            for day in rep['days']:
                self.assertTrue('day' in day)
                self.assertTrue('utime' in day)
                self.assertTrue('max' in day)
                self.assertTrue('min' in day)
                self.assertTrue('mean' in day)
                self.assertTrue('median' in day)
                self.assertTrue('stddev' in day)
                self.assertTrue('success' in day)
                self.assertAlmostEquals(day['success'], 1.0)
                self.assertTrue('count' in day)
                self.assertEquals(day['count'], 20)

            self.assertTrue('summary' in rep)

            self.assertTrue('day' in rep['summary'])
            self.assertTrue('utime' in rep['summary'])
            self.assertTrue('max' in rep['summary'])
            self.assertTrue('min' in rep['summary'])
            self.assertTrue('mean' in rep['summary'])
            self.assertTrue('median' in rep['summary'])
            self.assertTrue('stddev' in rep['summary'])
            self.assertTrue('success' in rep['summary'])
            self.assertAlmostEquals(rep['summary']['success'], 1.0)
            self.assertTrue('count' in rep['summary'])
            self.assertEquals(rep['summary']['count'], 20)

    def test_report_2(self):
        self._crons_history_data[:] = list()
        self._prepare_report(errors=True)

        self.assertEquals(len(self._crons_history_data), 40)

        self._timestamp = self._now

        cron_report = crons.CronReport('db', 'cron_job2',
                                       report_begin=6, report_end=4)
        report = cron_report.report()

        self.assertTrue('cron_job' not in report)
        self.assertTrue('cron_job2' in report)

        for rep in report.itervalues():
            self.assertTrue('days' in rep)
            for day in rep['days']:
                self.assertTrue('day' in day)
                self.assertTrue('utime' in day)
                self.assertTrue('max' in day)
                self.assertTrue('min' in day)
                self.assertTrue('mean' in day)
                self.assertTrue('median' in day)
                self.assertTrue('stddev' in day)
                self.assertTrue('success' in day)
                self.assertTrue(day['success'] < 1.0)
                self.assertTrue('count' in day)
                self.assertEquals(day['count'], 20)

            self.assertTrue('summary' in rep)

            self.assertTrue('day' in rep['summary'])
            self.assertTrue('utime' in rep['summary'])
            self.assertTrue('max' in rep['summary'])
            self.assertTrue('min' in rep['summary'])
            self.assertTrue('mean' in rep['summary'])
            self.assertTrue('median' in rep['summary'])
            self.assertTrue('stddev' in rep['summary'])
            self.assertTrue('success' in rep['summary'])
            self.assertTrue(rep['summary']['success'] < 1.0)
            self.assertTrue('count' in rep['summary'])
            self.assertEquals(rep['summary']['count'], 20)

    def test_report_3(self):
        self._crons_history_data[:] = list()
        self._prepare_report(errors=True)

        self._timestamp = self._now

        cron_report = crons.CronReport('db', 'cron_job', report_begin=20,
                                       report_end=15)
        report = cron_report.report()

        self.assertTrue('cron_job' not in report)
        self.assertTrue('cron_job2' not in report)

    def test_report_console(self):
        self._crons_history_data[:] = list()
        self._prepare_report(errors=True)

        self.assertEquals(len(self._crons_history_data), 40)

        cron_report = crons.CronReport('db', 'cron_job')

        self.assertTrue(len(cron_report.console_report(format_date=True)) > 0)
        self.assertTrue(len(cron_report.console_report(format_date=False)) > 0)

        cron_report = crons.CronReport('db')

        self.assertTrue(len(cron_report.console_report(format_date=True)) > 0)
        self.assertTrue(len(cron_report.console_report(format_date=False)) > 0)


if __name__ == '__main__':
    unittest.main()
