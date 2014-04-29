"""Manage processes which need to run periodically on a cluster.

Also accumulates statistic information about executed jobs and
can generate performance reports.

Synchronization is performed via database.

For details, please check:

http://eng.ironport.com/docs/is/common/crons.rst
http://eng.ironport.com/docs/is/common/auto-retry-crons.rst

A crons table must have the following structure::

  CREATE TABLE `crons` (
    `command` varchar(100) NOT NULL DEFAULT '',
    `mon_lock` varchar(200) DEFAULT NULL,
    `day_of_month` int(11) DEFAULT NULL,
    `day_of_week` int(11) DEFAULT NULL,
    `hour` int(11) DEFAULT NULL,
    `minute` int(11) DEFAULT NULL,
    `frequency` int(11) DEFAULT NULL,
    `lastrunstart` int(11) DEFAULT NULL,
    `lastrunend` int(11) DEFAULT NULL,
    `lastsuccess` int(11) DEFAULT NULL,
    `result` int(11) DEFAULT NULL,
    `hostname` varchar(100) DEFAULT NULL,
    `description` varchar(255) DEFAULT NULL,
    `fail_count` int unsigned NOT NULL DEFAULT 0,
    `max_allowed_fails` int unsigned NOT NULL DEFAULT 0,
    `fail_option` enum('all_soft', 'check_soft', 'check_hard', 'all_hard') NOT NULL DEFAULT 'all_soft',
    `fail_codes` varchar(255) NOT NULL DEFAULT '',
    `retry_interval` int unsigned DEFAULT 0,
    `exectime_warning` int(11) DEFAULT NULL,
    `exectime_error` int(11) DEFAULT NULL,
    PRIMARY KEY  (`command`)
  ) ENGINE=InnoDB;

Note that ``day_of_week`` uses 0 or 7 for Sunday, 1 for Monday, and so
on.

`exectime_warning` and `exectime_error` fields will be used by MonOps as
threasholds for run time of the cron job.

-----
There is also support for "local" crons - the ones we don't want to synchronize
within a cluster, but still want to monitor.  Configuration for this cron
(frequency, time to run, fail policy) is expected to be found in `crons` table.

So, if `crons.py` is run with `--local` flag, `crons_local` table will be
created (if it doesn't exist; table structure is really similar to `crons`)
and populated with a record for corresponding cron for current hostname
(if it was not created previously).  Subsequent processing is the same
as for common crons - checks for locks, status, readiness to be run,
history recording etc.

The same cron may be simultaneously run as local and as normal cron.
-----

:Status: $Id: //prod/main/_is/shared/python/sched/crons.py#10 $
:Authors: jwescott, kylev, gperry, rbodnarc
"""

import datetime
import logging
import math
import optparse
import os
import shlex
import socket
import subprocess
import sys
import time

from shared.db.dbcp import DBPoolManager
from shared.db.utils import retry
from shared.util import timeutils


# Status code indicating that the job is ready to run.
STATUS_READY = 0

# Status code indicating that it is not yet time to run the job.
STATUS_NOTYET = 1

# Return code indicating that either another machine is running
# the job currently or that the job failed on its last run attempt.
STATUS_LOCKED = 2

# Return code indicating successful job execution.
RETURN_SUCCESS = 0

# Return code indicating that job execution failed.
RETURN_FAILURE = 3


class CronJob(object):

    """Main class for executing commands like crons using database locks."""

    def __init__(self, db_name, cmd_name, history_expire_time=None,
                 command_class=None, command_max=1, local=False):
        """Create a CronJob instance.

        :param db_name: Name of a section in 'db.conf'.
        :param cmd_name: Name of a command in 'crons' table.
        :param history_expire_time: Delete history records for commands,
                                    executed more than 'history_expire_time'
                                    seconds ago.
        :param command_class: Regexp for command name.
        :param command_max: A maximum number of jobs, that matched
                            'command_class' pattern and currently ran on this
                            host.  I.e. that if this number reached, the
                            command won't be executed.
        """
        self.cmd_name = cmd_name
        self.hostname = socket.gethostname()
        self._command_class = command_class
        self._command_max = command_max
        self._logger = logging.getLogger()
        self._history = _CronHistory(db_name, cmd_name, history_expire_time)

        if local:
            self._queries = _CronQueriesLocal(db_name, self.hostname)
        else:
            self._queries = _CronQueries(db_name)

    def run(self, command, force=False, instance=False, history_only=False):
        """Try to execute job.

        The whole process of running a cron job looks like:
        1 - Check job status.
        2 - Try to lock the job in the database.
        3 - Run the command.
        4 - Unlock the job in the database.
        5 - Expires out-dated history data.

        Job execution on step (1) may be stopped for two reasons.
        Either not enough time has elapsed since the last invocation
        of the job or because a lock already exists on the job.  First
        reason may be suppressed by passing 'force=True' parameter.

        Getting the lock on step (2) may fail even if we got READY
        status in case of race conditions (another host has already locked
        the job).

        :param command: The shell command string to run.
        :param force: Force job to run right now, ignoring previous
                      running time.
        :param instance: Run instance based job.
        :param history_only: Execute command, ignoring information from
                             the `crons` table (e.g. lastrunstart, locks etc).
                             Information about execution will be stored only
                             in the `crons_history` table.
        :return: RETURN_SUCCESS if the job ran successfully,
                 RETURN_FAILURE if the job failed to execute properly,
                 STATUS_NOTYET if it is too early to run the job, or
                 STATUS_LOCKED if the job is locked.
        """
        start_time = int(time.time())

        if history_only:
            # No locking required.
            status = STATUS_READY
        elif instance:
            status = self._start_instance(force, start_time)
        else:
            status = self._start(force, start_time)

        if status == STATUS_READY:
            self._logger.info('Job "%s" is about to run on %s.',
                              self.cmd_name, self.hostname)

            self._history.start(start_time)

            command_args = shlex.split(command)
            return_code = subprocess.call(command_args)

            finish_time = int(time.time())

            if not history_only:
                # Unlock job.
                status = self._finish(return_code, finish_time)

            self._history.finish(return_code, finish_time)

        return status

    def clear(self):
        """Force a clear on any locked jobs.

        This method should only be called when a problem has been
        corrected and the job needs to be kicked off again.
        """
        self._logger.info('Clearing crons information for "%s".',
                          self.cmd_name)
        self._queries.clear(self.cmd_name)

    def status(self, now=None):
        """Get status of the job.

        Check if the job is locked. If not, analyze records in the crons
        table in the database to decide is it time to execute command.

        :param now: Actual time.
        :return: Dictionary of next structure:
                 {'status': <STATUS_LOCKED or STATUS_READY or STATUS_NOTYET>,
                  'lastrunstart': <job's last run time>,
                  'hostname': <name of host that locked the job
                              (if it is locked) or None>
                  }
        """
        if now is None:
            now = int(time.time())

        status_dict = {'status': None,
                       'lastrunstart': None,
                       'hostname': None}

        # Check if there is already max number of jobs running on this host.
        if self._command_class is not None:
            matching_cmds = self._queries.commands_list_by_hostname(
                                            self.hostname, self._command_class)

            if len(matching_cmds) >= self._command_max:
                self._logger.info('%d jobs of class "%s" running on this '
                                  'host.', len(matching_cmds),
                                  self._command_class)
                status_dict['status'] = STATUS_LOCKED
                status_dict['hostname'] = self.hostname
                return status_dict

        columns_to_select = ['mon_lock', 'day_of_month', 'day_of_week',
                             'hour', 'minute', 'frequency', 'lastrunstart',
                             'lastrunend', 'lastsuccess', 'hostname']
        command_info = self._queries.command_info(columns_to_select,
                                                  self.cmd_name)

        job = dict(zip(columns_to_select, command_info))

        status_dict['lastrunstart'] = job['lastrunstart']
        status_dict['hostname'] = job['hostname']

        if job['mon_lock'] is not None:
            # The job is locked.  Either another machine is running
            # the job or the job failed and has not been cleared.
            self._logger.debug('Job "%s" already locked by %s.',
                               self.cmd_name, job['hostname'])
            status_dict['status'] = STATUS_LOCKED
            return status_dict

        # The job is not locked.  First check to see if the job is
        # set up to run at specific times of day or at a given
        # frequency.  Then, determine whether or not it is time to
        # run the job.

        has_defined_start_time = any(job.get(field) is not None for
                                     field in ['minute', 'hour', 'day_of_week',
                                               'day_of_month'])
        ready = False

        if has_defined_start_time:
            # The job is set up to run at specific times of a day.
            ready = self._check_specified_time(now,
                                               job['lastrunstart'],
                                               job['minute'],
                                               job['hour'],
                                               job['day_of_week'],
                                               job['day_of_month'])
        else:
            # The job is set up to run at a given frequency.
            if job['lastrunstart'] is None or \
               (job['frequency'] is not None and
                job['lastrunstart'] < now - job['frequency']):
                ready = True

        if ready:
            self._logger.debug('Job "%s" is ready to run.',
                               self.cmd_name)
            status_dict['status'] = STATUS_READY
        else:
            self._logger.debug('Job "%s" not ready to run.',
                               self.cmd_name)
            status_dict['status'] = STATUS_NOTYET

        return status_dict

    def _start(self, force=False, start_time=None):
        """Attempt to start the running of the job.

        First it checks job status in the crons table in the database.
        If job is not locked and it is time to for the job to run according
        to the configured frequency or run time, it tries to get a lock
        in the database.

        :param force: Force job to run right now, ignoring previous
                      running time.
        :param start_time: Time when the execution was started.
        :return: STATUS_READY if the job is ready to run and lock was
                              successfully obtained,
                 STATUS_NOTYET if it is too early to run this job, or
                 STATUS_LOCKED if the job is locked.
        """
        if start_time is None:
            start_time = int(time.time())

        status_dict = self.status(start_time)

        if status_dict['status'] == STATUS_READY:
            status_dict['status'] = self._get_lock(status_dict['lastrunstart'],
                                                   start_time)
        elif status_dict['status'] == STATUS_NOTYET and force:
            self._logger.debug('Forcing start of "%s" job on %s',
                               self.cmd_name, self.hostname)
            status_dict['status'] = self._get_lock(status_dict['lastrunstart'],
                                                   start_time)

        return status_dict['status']

    def _start_instance(self, force=False, start_time=None):
        """A special case of _start().

        It tries to start a numbered instance of a cron.  It will attempt
        to get a lock on "name-n" for values of n starting at 0.
        Return values are the same.

        :param start_time: Time when the execution was started.
        :param force: Force job to run right now, ignoring previous
                      running time.
        """
        orig_cmd = self.cmd_name

        status = STATUS_NOTYET
        commands = self._queries.commands_list()
        instance = 0

        while True:
            self.cmd_name = '%s-%d' % (orig_cmd, instance)
            if self.cmd_name in commands:
                status = self._start(force, start_time)
            else:
                # We've gotten past the nth one.
                break

            if status == STATUS_READY:
                # Found one!
                break
            else:
                # Keep looking.
                instance += 1

        if not status == STATUS_READY:
            # Set things back to normal in case this object gets used again.
            self.cmd_name = orig_cmd

        # Command name may be changed during processing.
        self._history.change_command(self.cmd_name)

        return status

    def _finish(self, return_code, finish_time=None):
        """Update database information according to the command's return code.

        If the return code from the command is 0, release the lock on the job
        in the crons table in the database.  Otherwise, analyze return code,
        using 'fail_option', 'max_allowed_fails', 'fail_code' fields
        in the crons table in the database and update database according to
        results of this analysis.

        :param return_code: The return code of the executed command.
        :param finish_time: Time when the execution was finished.
        :return: RETURN_SUCCESS if executed successfully of with the soft fail,
                 RETURN_FAILURE if command execution failed.
        """
        if finish_time is None:
            finish_time = int(time.time())

        status = None

        if return_code == 0:
            self._queries.finish_success(finish_time, finish_time,
                                         return_code, self.cmd_name)
            self._logger.info('Execution of "%s" on host %s finished '
                              'successfully.',
                              self.cmd_name, self.hostname)
            status = RETURN_SUCCESS
        elif self._analyze_fail_code(return_code):

            self._finish_fail_soft(return_code, finish_time)
            self._logger.warning('Execution of "%s" on host %s finished '
                                 'with soft fail. Return code: %s.',
                                 self.cmd_name, self.hostname, return_code)
            status = RETURN_SUCCESS
        else:
            self._queries.finish_fail(finish_time, return_code, self.cmd_name)
            self._logger.error('Execution of "%s" on host %s finished '
                               'with hard fail. Return code: %s.',
                               self.cmd_name, self.hostname, return_code)
            status = RETURN_FAILURE

        if status == RETURN_SUCCESS:
            self._logger.debug('Job "%s" has been unlocked.',
                               self.cmd_name)
        else:
            self._logger.debug('Job "%s" has been leaved as locked.',
                               self.cmd_name)

        return status

    def _analyze_fail_code(self, return_code):
        """Check if we should keep job locked.

        If command execution finished with non-zero return code, use
        special mechanism to determine if job fail 'softly' (job will be
        repeatedly executed later) or hardly (the lock on job will be kept
        and job execution will be stopped).

        :param return_code: Return code of command execution.
        :return: True in case of soft fail,
                 False in case of hard fail.
        """
        columns_to_select = ['fail_count', 'max_allowed_fails', 'fail_option',
                             'fail_codes']
        command_info = self._queries.command_info(columns_to_select,
                                                  self.cmd_name)
        job = dict(zip(columns_to_select, command_info))

        try:
            job['fail_codes'] = [int(x) for x in job['fail_codes'].split(',')]
        except:
            job['fail_codes'] = list()

        if job['fail_option'] == 'all_hard':
            return False
        elif job['fail_option'] == 'check_hard':
            if return_code in job['fail_codes']:
                return False
        elif job['fail_option'] == 'check_soft':
            if return_code not in job['fail_codes']:
                return False

        if job['fail_count'] + 1 >= job['max_allowed_fails']:
            return False
        else:
            return True

    def _finish_fail_soft(self, return_code, finish_time):
        """Helper to handle case of the soft fail.

        It modifies lastrunstart field for command's record in the table crons
        in the database to force command to run earlier than usual after any
        soft fail.  While determining this lastrunstart shift it uses
        information about job's frequency, retry_interval and number
        of previous fails.  If job doesn't have defined frequency, function
        tries to calculate it.

        :param return_code: The return code of the executed command.
        :param finish_time: Time when the execution was finished.
        """
        columns_to_select = ['day_of_month', 'day_of_week', 'hour', 'minute',
                             'retry_interval', 'frequency', 'fail_count']
        command_info = self._queries.command_info(columns_to_select,
                                                  self.cmd_name)
        job = dict(zip(columns_to_select, command_info))

        frequency = None
        retry_interval = None

        if job['frequency'] is not None:
            frequency = job['frequency']
        else:
            if job['day_of_week'] is not None:
                frequency = timeutils.ONEWEEK
            elif job['day_of_month'] is not None:
                frequency = timeutils.ONEMONTH
            elif job['hour'] is not None:
                frequency = timeutils.ONEHOUR
            elif job['minute'] is not None:
                frequency = timeutils.ONEMINUTE
            else:
                frequency = 0

        if job['retry_interval']:
            retry_interval = job['retry_interval']
        else:
            if frequency > math.floor(60 * 2 ** job['fail_count']):

                retry_interval = math.floor(60 * 2 ** job['fail_count'])
            else:
                retry_interval = frequency

        lastrunstart = finish_time - frequency + retry_interval

        self._queries.finish_fail_soft(finish_time, return_code, lastrunstart,
                                       self.cmd_name)

    def _get_lock(self, lastrunstart, start_time=None):
        """Try to get the lock on job.

        To manage race condition use value of 'lastrunstart' field, that
        must change if any host has already got the lock.

        :param lastrunstart: Time of last job execution.
        :param start_time: Time when the execution was started.
        :return: STATUS_READY if job was locked successfully,
                 STATUS_LOCKED if another host has already lock the job.
        """
        if start_time is None:
            start_time = int(time.time())

        lock_string = '<%s> <%s>' % (self.hostname, time.ctime(start_time))

        if self._queries.lock(mon_lock=lock_string,
                              lastrunstart=start_time,
                              hostname=self.hostname,
                              command=self.cmd_name,
                              command_pattern=self._command_class,
                              command_max_count=self._command_max,
                              lastrunstart_restr=lastrunstart) > 0:
            self._logger.debug('Job "%s" has been locked by %s.',
                               self.cmd_name, self.hostname)
            return STATUS_READY
        else:
            self._logger.debug('Job "%s" already locked by another host.',
                               self.cmd_name)
            return STATUS_LOCKED

    def _check_specified_time(self, now, lastrunstart,
                              minute=None, hour=None,
                              day_of_week=None, day_of_month=None):
        """Check if it is time to run a start-time specified job.

        :param now: Now as unix timestamp.
        :param lastrunstart: Time of last run.
        :param minute: Number of minute to start the execution [0-59].
        :param hour: Number of hour to start the execution[0-23].
        :param day_of_week: Number of day to start the execution[0-7].
        :param day_of_month: Number of month to start the execution[0-31].
        :return: True if it is time to run the job,
                 False if it is too jet to run the job.
        """
        # The job is set up to run at specific times of day.
        localtime = time.localtime(now)
        curr_hour = localtime.tm_hour
        curr_minute = localtime.tm_min
        curr_day_of_month = localtime.tm_mday

        # crons uses 0 / 7 for Sunday.
        curr_day_of_week = (localtime.tm_wday + 1) % 7

        day_ready = False

        # Day of week always trumps day of month.  If no day
        # of week or month was set, it means it should run
        # every day.

        if day_of_week is None and \
           day_of_month is None:
            day_ready = True
        elif day_of_week is not None:
            if day_of_week == curr_day_of_week:
                day_ready = True
        elif day_of_month is not None:
            if day_of_month == curr_day_of_month:
                day_ready = True

        if day_ready:

            # Our day, at least, has come.  Now we must
            # determine whether or not our hour and minute
            # have also come.

            dt_args = list()
            # Append the datetime.datetime args to the above list.
            dt_args.append(localtime.tm_year)
            dt_args.append(localtime.tm_mon)
            dt_args.append(curr_day_of_month)

            if hour is None and minute is None:
                pass
            elif hour is None:
                dt_args.append(curr_hour)
                dt_args.append(minute)
            elif minute is None:
                dt_args.append(hour)
                dt_args.append(curr_minute)
            else:
                dt_args.append(hour)
                dt_args.append(minute)

            runtime = time.mktime(datetime.datetime(*dt_args).timetuple())

            if runtime < now and (lastrunstart is None or
                                  runtime > lastrunstart):
                return True

        return False


class _CronHistory(object):

    """Accumulate statistic information about cron jobs execution."""

    def __init__(self, db_name, cmd_name, history_expire_time=None):
        """Create a _CronHistory instance.

        The following table will be created if it does not exist in
        the database you are referencing:

            CREATE TABLE crons_history (
              command varchar(100) NOT NULL default '',
              lastrunstart int(10) UNSIGNED NOT NULL default 0,
              hostname varchar(100) default NULL,
              exectime int(10) UNSIGNED default NULL,
              result int(11) default NULL,
              PRIMARY KEY  (`command`, `lastrunstart`, `hostname`)
            ) ENGINE=InnoDB;

        :param db_name: Name of a section in 'db.conf'.
        :param cmd_name: The cron command name in the database.
        :param history_expire_time: Delete history records for commands,
                                    executed more then 'history_expire_time'
                                    seconds ago.
        """
        self.cmd_name = cmd_name
        self.hostname = socket.gethostname()

        self._logger = logging.getLogger()
        self._queries = _CronQueries(db_name)

        self._start_time = None
        self._finish_time = None
        self._expire_time = history_expire_time

        if not self._queries.history_check():
            self._queries.history_create()

    def change_command(self, cmd_name):
        """Change command name for CronHistory object.

        :param cmd_name: New command name.
        """
        self.cmd_name = cmd_name

    def start(self, start_time=None):
        """Record the start of a command.

        :param start_time: Time when the execution was started.
        """
        if start_time is None:
            self._start_time = int(time.time())
        else:
            self._start_time = start_time
        self._finish_time = None

    def finish(self, return_code, finish_time=None):
        """Record the end of a command and expire outdated history log.

        :param return_code: The return code of the executed command.
        :param finish_time: Time when the execution was finished.
        """
        assert self._start_time is not None, \
              'Job "%s" was not started.' % (self.cmd_name,)

        assert self._finish_time is None, \
            'Execution information already stored for "%s".' % (self.cmd_name,)

        if finish_time is None:
            self._finish_time = int(time.time())
        else:
            self._finish_time = finish_time

        self._queries.history_log(
                            command=self.cmd_name,
                            lastrunstart=self._start_time,
                            hostname=self.hostname,
                            exectime=self._finish_time - self._start_time,
                            result=return_code)

        self._logger.debug('Execution information stored for "%s".' %
                           (self.cmd_name,))

        if return_code == RETURN_SUCCESS:
            self._expire(self._finish_time)

    def _expire(self, now=None):
        """Expire out-dated history data.

        :param now: End of expiration period (i.e. actual time).
        """
        if now is None:
            now = int(time.time())

        if self._expire_time is not None:
            lastrunstart = now - self._expire_time
            expired = self._queries.history_expire(self.cmd_name,
                                                   lastrunstart)

            if expired > 0:
                self._logger.debug('Deleted %d expired records.', expired)


class CronReport(object):

    """Class for generating performance reports for cron jobs."""

    def __init__(self, db_name, cmd_name=None,
                 report_begin=None, report_end=None):
        """Create a CronReport instance.

        :param start_time: Start time for analysis.
        :param cmd_name: The cron command name in the database. If not passed,
                         analyse history for all jobs in the crons_history table
                         in the database.
        """
        self._queries = _CronQueries(db_name)
        self._report_data = self._prepare_report_data(report_begin,
                                                      report_end,
                                                      cmd_name)

    def console_report(self, format_date=False):
        """Generate a report in human-readable form.

        :param format_date: If True, trasform time stamps into '27 Jan 2011'.
        :return: A string with the report.
        """
        report_dict = self.report()

        if not report_dict:
            return 'Nothing to report!'

        # Dictionary with formats.  First element of value - column header,
        # second element format str for values in this column.
        format_map = {'min':     ['   Min', '%6d'],
                      'max':     ['   Max', '%6d'],
                      'mean':    ['   Mean', '%7.2f'],
                      'median':  [' Median', '%7.2f'],
                      'stddev':  ['  Std Dev', '%9.2f'],
                      'success': ['Success', '%7.2f'],
                      'count':   ['Run count', '%9d'],
                      'day':     ['Day        ', '%11s'],
                      'utime':   ['Unixtime   ', '%11d'],
                      'summary': ['', '*Summary*  ']
                      }

        head_line_pattern = '"%s" report (from %s to %s)'

        if format_date:
            headers = ('day', 'min', 'max', 'mean', 'median', 'stddev',
                       'success', 'count')

        else:
            headers = ('utime', 'min', 'max', 'mean', 'median', 'stddev',
                       'success', 'count')

        output = list()
        commands = sorted(report_dict.keys())

        for command in commands:

            command_output = list()

            sum_info = report_dict[command]['summary']
            days_info = report_dict[command]['days']

            # Append header line.
            command_output.append(head_line_pattern %
                                            (command,
                                             sum_info[headers[0]][0],
                                             sum_info[headers[0]][1]))

            # Append column headers.
            header_formats = [format_map[header][0] for header in headers]
            command_output.append(' '.join(header_formats))

            # Append information for each available day.
            for day_info in days_info:
                command_output.append(' '.join(format_map[h][1]
                                               for h in headers) %
                                         tuple(day_info[h]
                                               for h in headers))

            # Form format for summary row.
            summary_format = [format_map['summary'][1]]
            summary_format.extend([format_map[h][1] for h in headers[1:]])

            # Apply summary data to formed format.
            command_output.append(' '.join(summary_format) %
                                  tuple(sum_info[h]
                                        for h in headers[1:]))

            output.append('\n')
            output.extend(command_output)

        return '\n'.join(output)

    def report(self):
        """Calculates report data using information from 'cron_history' table.

        Returned data can be used to create your own reports.

        :return: A dictionary with following structure:
                {<command name> : {'days' : ({'day' : <readable date>,
                                              'utime' : <utimestamp date>,
                                              'max' : <max exec time for day>,
                                              'min' : <min exec time for day>,
                                              ...
                                              <rest of data>
                                               ...
                                              },
                                              ...
                                              <next day>,
                                              ...
                                             )
                                    'summary' : {'day' : <start and end date
                                                          of analysis in
                                                          readable format>,
                                                 'utime' : <start and end date
                                                            of analysis as
                                                            utimestamp>,
                                                 'max' :  <max exec time for
                                                           period>,
                                                 'min' : <min exec time for
                                                          period>,
                                                 ...
                                                 <rest of data>
                                                 ...}
                                               }
                <rest of commands>}
        """
        report_result = dict()

        for command, command_info in self._report_data.iteritems():

            report = report_result.setdefault(command, dict())
            days_report = report.setdefault('days', list())

            for day in sorted(command_info.keys()):

                day_info = command_info[day]
                day_report = dict()

                day_report['max'] = max(day_info['exectime'])
                day_report['min'] = min(day_info['exectime'])
                day_report['mean'] = self._mean(day_info['exectime'])
                day_report['median'] = self._median(day_info['exectime'])
                day_report['stddev'] = self._stddev(day_info['exectime'])
                day_report['success'] = self._success(day_info['result'])
                day_report['count'] = len(day_info['exectime'])
                day_report['utime'] = day
                day_report['day'] = time.strftime('%d %b %Y',
                                                  time.localtime(day))

                days_report.append(day_report)

            sum_report = dict()

            exectimes = [i for day_info in command_info.itervalues()
                           for i in day_info['exectime']]
            results = [i for day_info in command_info.itervalues()
                         for i in day_info['result']]
            runs = [i for day_info in command_info.itervalues()
                      for i in day_info['lastrunstart']]

            sum_report['max'] = max(exectimes)
            sum_report['min'] = min(exectimes)
            sum_report['mean'] = self._mean(exectimes)
            sum_report['median'] = self._median(exectimes)
            sum_report['stddev'] = self._stddev(exectimes)
            sum_report['success'] = self._success(results)
            sum_report['count'] = len(exectimes)
            sum_report['utime'] = (min(runs),
                                   max(runs))
            sum_report['day'] = (time.ctime(min(runs)),
                                 time.ctime(max(runs)))

            report['summary'] = sum_report

        return report_result

    def _prepare_report_data(self, report_begin, report_end, command=None):
        """Helper to get jobs' history from database and distribute it by day.

        :param command: The cron command name in the database. If not passed,
                        select history for all jobs in the crons_history table
                        in the database.
        :return: A dictionary with command names as keys and commands history
                 information as values.
        """
        begin_timestamp = 0
        end_timestamp = time.time()

        if report_begin is not None:
            begin_timestamp = timeutils.today() - \
                              report_begin * timeutils.ONEDAY
        if report_end is not None:
            end_timestamp = timeutils.today() - report_end * timeutils.ONEDAY

        raw_data = self._queries.history_report(begin_timestamp, end_timestamp,
                                                command)

        report_data = dict()
        for command_data in raw_data:

            day_command_info = report_data.setdefault(command_data[0],
                                                      dict())

            info = day_command_info.setdefault(
                                timeutils.today(command_data[1]), dict())

            info.setdefault('lastrunstart', list()).append(command_data[1])
            info.setdefault('exectime', list()).append(command_data[2])
            info.setdefault('result', list()).append(command_data[3])

        return report_data

    def _success(self, series):
        """Calculates persentage of successfully executed jobs.

        :param series: A list/tuple of return codes.
        :return: Float in [0, 1].
        """
        return float(series.count(0)) / len(series)

    def _mean(self, series):
        """Calculates mean/average for a given series on numbers.

        :param series: A list/tuple of numbers
        :return: Mean for the series.
        """
        return float(sum(series)) / len(series)

    def _median(self, series):
        """Calculates median for a given series on numbers.

        :param series: A list/tuple of numbers
        :return: Median fot the series.
        """
        series = list(series)
        series.sort()
        middle_idx = (len(series) - 1) / 2.0
        low = series[int(math.floor(middle_idx))]
        high = series[int(math.ceil(middle_idx))]
        return (low + high) / 2.0

    def _stddev(self, series):
        """Calculates standard deviation for a given series on numbers.

        :param series: A list/tuple of numbers
        :return: Standard deviation for the series.
        """
        if len(series) <= 1:
            return 0.0
        else:
            mean = self._mean(series)
            sigma = 0
            for elem in series:
                sigma += (elem - mean) ** 2
            return (sigma / (len(series) - 1)) ** 0.5


class _CronQueries(object):

    """"Helper class for executing database queries."""

    def __init__(self, db_name):
        """Create a _CronQueries instance.

        :param db_name: Name of section in db.conf.
        """
        self._pool = DBPoolManager().get_rw_pool(db_name)

    @retry()
    def _local_exists(self, cursor):
        check_query = """ SHOW TABLES LIKE 'crons_local'
                      """
        cursor.execute(check_query)
        return bool(cursor.fetchone())

    @retry()
    def commands_list(self):
        """Get all command names.

        :return: List with command names.
        """
        query = """SELECT `command`
                   FROM `crons`
                """
        with self._pool.transaction() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]

    @retry()
    def commands_list_by_hostname(self, hostname, command_pattern):
        """Get all commands, locked by specified host.

        :param hostname: Host name.
        :param command_pattern: Regexp pattern for command name.
        :return: List with command names.
        """
        query = """SELECT `command`
                   FROM `%s`
                   WHERE `hostname` = %%s AND
                         `command` REGEXP %%s AND
                         `mon_lock` IS NOT NULL
                """
        with self._pool.transaction() as cursor:
            if self._local_exists(cursor):
                query = '%s UNION ALL %s' % (query % ('crons',),
                                             query % ('crons_local',))
                args = (hostname, command_pattern, hostname, command_pattern)
            else:
                query = query % ('crons',)
                args = (hostname, command_pattern)

            cursor.execute(query, args)
            return [row[0] for row in cursor.fetchall()]

    @retry()
    def command_info(self, columns, command):
        """Get information for specified command.

        :param columns: Columns to be selected.
        :param command: Command name.
        :return: List with requested information.
        """
        query = """SELECT `%s`
                   FROM `crons`
                   WHERE `command` = %%s
                """ % ('`, `'.join(columns),)
        with self._pool.transaction() as cursor:
            cursor.execute(query, (command,))
            return cursor.fetchone()

    @retry(retry_count=1)
    def lock(self, mon_lock, lastrunstart, hostname, command,
             lastrunstart_restr, command_pattern, command_max_count):
        """Get a lock on the command.

        :param mon_lock: Lock string.
        :param lastrunstart: New lastrustart value.
        :param hostname: New hostname value.
        :param command: Name of command that must be locked.
        :param lastrunstart_restr: Previous value of lastrunstart field.
                                   Used to cope with the race condition on
                                   several hosts.
        :param command_pattern: Regexp for command name.
        :param command_max: A maximum number of jobs, that matched
                            'command_pattern' and currently ran on this
                            host.
        :return: 1 if lock was successfully acquired, 0 otherwise.
        """
        select_query = """SELECT COUNT(*)
                          FROM `crons`
                          WHERE `hostname` = %s AND
                                `command` REGEXP %s AND
                                `mon_lock` IS NOT NULL
                          FOR UPDATE
                       """

        update_query = """UPDATE `crons`
                          SET `mon_lock` = %s,
                              `lastrunstart` = %s,
                              `lastrunend` = 0,
                              `hostname` = %s,
                              `result` = NULL
                          WHERE `command` = %s AND
                                `mon_lock` IS NULL
                       """

        with self._pool.transaction() as cursor:
            if command_pattern is not None:
                cursor.execute(select_query, (hostname, command_pattern))
                command_count = cursor.fetchone()[0]
                if command_count >= command_max_count:
                    return 0

            if lastrunstart_restr is None:
                update_query += ' AND `lastrunstart` IS NULL'
                return cursor.execute(update_query, (mon_lock, lastrunstart,
                                                     hostname, command))
            else:
                update_query += ' AND `lastrunstart` = %s'
                return cursor.execute(update_query, (mon_lock, lastrunstart,
                                                     hostname, command,
                                                     lastrunstart_restr))

    @retry()
    def clear(self, command):
        """Clear information about previous runs for specified command.

        :param command: Command name.
        """
        query = """UPDATE `crons`
                   SET `lastrunstart` = NULL,
                       `lastrunend` = NULL,
                       `mon_lock` = NULL,
                       `fail_count` = 0
                   WHERE `command` = %s
                """
        with self._pool.transaction() as cursor:
            cursor.execute(query, (command,))

    @retry()
    def finish_success(self, lastrunned, lastsuccess, result, command):
        """Store run information for successfully finished command.

        :param lastrunend: New lastrunend value.
        :param lastsuccess: New lastsuccess value.
        :param result: New result value.
        :param command: Command name.
        """
        query = """UPDATE `crons`
                   SET `lastrunend` = %s,
                       `lastsuccess` = %s,
                       `result` = %s,
                       `mon_lock` = NULL,
                       `fail_count` = 0
                   WHERE `command` = %s
                """
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunned, lastsuccess, result, command))

    @retry()
    def finish_fail(self, lastrunend, result, command):
        """Store run information for the failed command.

        :param lastrunend: New lastrunend value.
        :param result: New result value.
        :param command: Command name.
        """
        query = """UPDATE crons
                   SET lastrunend = %s,
                       result = %s,
                       fail_count = fail_count + 1
                   WHERE command = %s"""
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunend, result, command))

    @retry()
    def finish_fail_soft(self, lastrunend, result, lastrunstart, command):
        """Store run information for the command, finished with soft fail.

        :param lastrunend: New lastrunend value.
        :param result: New result value.
        :param lastrunstart: New lastrunstart value.
        :param command: Command name.
        """
        query = """UPDATE crons
                   SET lastrunend = %s,
                       result = %s,
                       mon_lock = NULL,
                       lastrunstart = %s,
                       fail_count = fail_count + 1
                   WHERE command = %s"""
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunend, result, lastrunstart, command))

    @retry()
    def history_check(self):
        """Check if `crons_history` table exists in the database.

        :return: Empty list if table doesn't exists, not empty list otherwise.
        """
        query = """SHOW TABLES LIKE 'crons_history'"""
        with self._pool.transaction() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]

    @retry()
    def history_create(self):
        """Create `crons_history` table."""
        query = """CREATE TABLE IF NOT EXISTS crons_history
                   (command varchar(100) NOT NULL DEFAULT '',
                    lastrunstart int(10) UNSIGNED NOT NULL DEFAULT 0,
                    hostname varchar(100) DEFAULT NULL,
                    exectime int(10) UNSIGNED DEFAULT NULL,
                    result int(11) DEFAULT NULL,
                    PRIMARY KEY (command, lastrunstart, hostname)
                    ) ENGINE=InnoDB"""
        with self._pool.transaction() as cursor:
            cursor.execute(query)

    @retry()
    def history_log(self, command, lastrunstart, hostname, exectime, result):
        """Store information about executed command.

        :param command: Command name.
        :param lastrunstart: Start time of command execution.
        :param hostname: Host that executed command.
        :param exectime: Execution time.
        :param result: Result of execution.
        """
        query = """INSERT INTO crons_history
                     (command, lastrunstart, hostname, exectime, result)
                   VALUES (%s, %s, %s, %s, %s)"""
        with self._pool.transaction() as cursor:
            cursor.execute(query, (command, lastrunstart, hostname,
                                   exectime, result))

    @retry()
    def history_report(self, lastrunstart_begin, lastrunstart_end,
                       command=None):
        """Get information about command execution.

        :param lastrunstart_begin: Start time of requested period.
        :param lastrunstart_end: End time of requested period.
        :param command: Command name.  If None - get information about
                        all commands.
        :return: List of lists with execution information.
        """
        query = """SELECT command, lastrunstart, exectime, result
                   FROM crons_history
                   WHERE lastrunstart >= %s AND
                         lastrunstart <= %s"""
        with self._pool.transaction() as cursor:
            if command is not None:
                query += ' AND command = %s'
                cursor.execute(query, (lastrunstart_begin, lastrunstart_end,
                                       command))
            else:
                cursor.execute(query, (lastrunstart_begin, lastrunstart_end))
            return cursor.fetchall()

    @retry()
    def history_expire(self, command, lastrunstart):
        """Delete old history records for specified command.

        :param command: Command name.
        :param lastrunstart: Records with lastrunstart value less than given
                             will be expired.
        :return: Number of deleted records.
        """
        query = """DELETE FROM crons_history
                   WHERE command = %s AND
                         lastrunstart < %s"""
        with self._pool.transaction() as cursor:
            return cursor.execute(query, (command, lastrunstart))


class _CronQueriesLocal(_CronQueries):

    """Queries to database for "local" crons."""

    def __init__(self, db_name, hostname):
        """Init _CronQueriesLocal instance.

        :param db_name: Name of section in db.conf.
        :param hostname: Hostname - a part of primary key for queries to
                         `crons_local` (`command` is the other part).
        """
        self.hostname = hostname
        super(_CronQueriesLocal, self).__init__(db_name)

    @retry()
    def command_info(self, columns, command):
        """Get information for specified command.

        If there is no information about local cron, return information
        about corresponding base cron.

        :param columns: Columns to be selected.
        :param command: Command name.
        :return: List with requested information.
        """
        query = """SELECT %s
                   FROM `crons_local`
                   WHERE `command` = %%s AND
                         `hostname` = %%s
                """ % (', '.join(columns),)

        with self._pool.transaction() as cursor:
            if self._local_exists(cursor):
                cursor.execute(query, (command, self.hostname))
                res = cursor.fetchone()
                if res:
                    # There is local cron record in `crons_local`.
                    return res

            # Look for original record.
            query = """SELECT %s
                       FROM `crons`
                       WHERE `command` = %%s
                    """ % (', '.join(columns),)
            cursor.execute(query, (command,))
            return cursor.fetchone()

    @retry()
    def _create_crons_local(self, cursor):
        """Create `crons_local` table.

        Does nothing if table already exists.

        :param cursur: DB cursor object to execute a query.
        """
        create_query = """ CREATE TABLE IF NOT EXISTS `crons_local` (
                               `command` varchar(100) NOT NULL DEFAULT '',
                               `mon_lock` varchar(200) DEFAULT NULL,
                               `day_of_month` int(11) DEFAULT NULL,
                               `day_of_week` int(11) DEFAULT NULL,
                               `hour` int(11) DEFAULT NULL,
                               `minute` int(11) DEFAULT NULL,
                               `frequency` int(11) DEFAULT NULL,
                               `lastrunstart` int(11) DEFAULT NULL,
                               `lastrunend` int(11) DEFAULT NULL,
                               `lastsuccess` int(11) DEFAULT NULL,
                               `result` int(11) DEFAULT NULL,
                               `hostname` varchar(100) DEFAULT NULL,
                               `description` varchar(255) DEFAULT NULL,
                               `fail_count` int unsigned NOT NULL DEFAULT 0,
                               `max_allowed_fails` int unsigned NOT NULL DEFAULT 0,
                               `fail_option` enum('all_soft', 'check_soft', 'check_hard', 'all_hard') NOT NULL DEFAULT 'all_soft',
                               `fail_codes` varchar(255) NOT NULL DEFAULT '',
                               `retry_interval` int unsigned DEFAULT 0,
                               PRIMARY KEY  (`command`, `hostname`)
                             ) ENGINE=InnoDB;
                        """

        cursor.execute(create_query)

    @retry()
    def _insert_local_cron(self, cursor, command, hostname):
        """Insert a new record into `crons_local` table.

        Copies all the data for the job from `crons` table except for
        execution statistics like `lastrunstart` or `fail_codes`.

        :param cursor: DB cursor object to execute queries.
        :param command: Command/job name.
        :param hostname: Hostname cron will be running on.
        """
        select_query = """SELECT `command`,
                                 NULL as mon_lock,
                                 `day_of_month`,
                                 `day_of_week`,
                                 `hour`,
                                 `minute`,
                                 `frequency`,
                                 NULL as lastrunstart,
                                 NULL as lastrunend,
                                 NULL as lastsuccess,
                                 NULL as result,
                                 '%s' as hostname,
                                 `description`,
                                 0 as fail_count,
                                 `max_allowed_fails`,
                                 `fail_option`,
                                 '' as fail_codes,
                                 `retry_interval`
                          FROM `crons`
                          WHERE command = %%s
                       """ % (hostname,)

        insert_query = """INSERT IGNORE INTO `crons_local`
                          (`command`, `mon_lock`,
                           `day_of_month`, `day_of_week`,
                           `hour`, `minute`, `frequency`,
                           `lastrunstart`, `lastrunend`, `lastsuccess`,
                           `result`, `hostname`, `description`,
                           `fail_count`, `max_allowed_fails`, `fail_option`, `fail_codes`, `retry_interval`)
                          VALUES
                          (%s)
                          """ % (', '.join(['%s'] * 18),)

        cursor.execute(select_query, command)
        cursor.execute(insert_query, cursor.fetchone())

    @retry(retry_count=1)
    def lock(self, mon_lock, lastrunstart, hostname, command,
             lastrunstart_restr, command_pattern=None, command_max_count=None):
        """Get a lock on the command.

        :param mon_lock: Lock string.
        :param lastrunstart: New lastrustart value.
        :param hostname: New hostname value.
        :param command: Name of command that must be locked.
        :param lastrunstart_restr: Previous value of lastrunstart field.
                                   Used to cope with the race condition on
                                   several hosts.
        :param command_pattern: Regexp for command name.
        :param command_max_count: A maximum number of jobs, that matched
                            'command_pattern' and currently ran on this
                            host.
        :return: 1 if lock was successfully acquired, 0 otherwise.
        """
        select_local_query = """SELECT 1
                                FROM `crons_local`
                                WHERE `command` = %s AND
                                      `hostname` = %s
                             """

        update_query = """UPDATE `crons_local`
                          SET `mon_lock` = %s,
                              `lastrunstart` = %s,
                              `lastrunend` = 0,
                              `result` = NULL
                          WHERE `command` = %s AND
                                `hostname` = %s AND
                                `mon_lock` IS NULL
                       """

        count_query = """SELECT SUM(count)
                         FROM (SELECT COUNT(*) as count
                               FROM `crons_local`
                               WHERE `hostname` = %s AND
                                     `command` REGEXP %s AND
                                     `mon_lock` IS NOT NULL
                               FOR UPDATE
                               UNION ALL
                               SELECT COUNT(*) as count
                               FROM `crons`
                               WHERE `hostname` = %s AND
                                     `command` REGEXP %s AND
                                     `mon_lock` IS NOT NULL
                               FOR UPDATE) s
                      """

        with self._pool.transaction() as cursor:
            if not self._local_exists(cursor):
                self._create_crons_local(cursor)

            cursor.execute(select_local_query, (command, hostname))
            if not cursor.fetchone():
                self._insert_local_cron(cursor, command, hostname)

            cursor.execute(count_query, (hostname, command_pattern,
                                         hostname, command_pattern))
            if cursor.fetchone()[0] >= command_max_count:
                return 0

            if lastrunstart_restr is None:
                update_query += ' AND lastrunstart IS NULL'
                return cursor.execute(update_query, (mon_lock, lastrunstart,
                                                     command, hostname))
            else:
                update_query += ' AND lastrunstart = %s'
                return cursor.execute(update_query, (mon_lock, lastrunstart,
                                                     command, hostname,
                                                     lastrunstart_restr))

    @retry()
    def clear(self, command):
        """Clear information about previous runs for specified command.

        :param command: Command name.
        """
        query = """UPDATE `crons_local`
                   SET `lastrunstart` = NULL,
                       `lastrunend` = NULL,
                       `mon_lock` = NULL,
                       `fail_count` = 0
                   WHERE `command` = %s AND
                         `hostname` = %s
                """
        with self._pool.transaction() as cursor:
            cursor.execute(query, (command, self.hostname))

    @retry()
    def finish_success(self, lastrunned, lastsuccess, result, command):
        """Store run information for successfully finished command.

        :param lastrunend: New lastrunend value.
        :param lastsuccess: New lastsuccess value.
        :param result: New result value.
        :param command: Command name.
        """
        query = """UPDATE `crons_local`
                   SET `lastrunend` = %s,
                       `lastsuccess` = %s,
                       `result` = %s,
                       `mon_lock` = NULL,
                       `fail_count` = 0
                   WHERE `command` = %s AND
                         `hostname` = %s
                """
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunned, lastsuccess, result, command,
                                   self.hostname))

    @retry()
    def finish_fail(self, lastrunend, result, command):
        """Store run information for the failed command.

        :param lastrunend: New lastrunend value.
        :param result: New result value.
        :param command: Command name.
        """
        query = """UPDATE `crons_local`
                   SET `lastrunend` = %s,
                       `result` = %s,
                       `fail_count` = `fail_count` + 1
                   WHERE `command` = %s AND
                         `hostname` = %s
                   """
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunend, result, command, self.hostname))

    @retry()
    def finish_fail_soft(self, lastrunend, result, lastrunstart, command):
        """Store run information for the command, finished with soft fail.

        :param lastrunend: New lastrunend value.
        :param result: New result value.
        :param lastrunstart: New lastrunstart value.
        :param command: Command name.
        """
        query = """UPDATE `crons_local`
                   SET `lastrunend` = %s,
                       `result` = %s,
                       `mon_lock` = NULL,
                       `lastrunstart` = %s,
                       `fail_count` = `fail_count` + 1
                   WHERE `command` = %s AND
                         `hostname` = %s"""
        with self._pool.transaction() as cursor:
            cursor.execute(query, (lastrunend, result, lastrunstart, command,
                                   self.hostname))


def std_logger():
    """Get stdout logger."""
    fmt = '%(asctime)s [%(name)s] %(levelname)s %(message)s'
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    log = logging.getLogger()
    log.addHandler(handler)
    return log


def main():
    """Script's entry point"""
    # Parse command-line options.
    me = os.path.basename(sys.argv[0])
    option_parser = optparse.OptionParser(usage='usage: %s [OPTIONS] <dbname> '
                                                '<command name>' % (me,))

    option_parser.add_option('-v', '--verbosity', metavar='LVL',
                             action='store', type='int', dest='verbosity',
                             default=0,
                             help='debug verbosity level (0-3)')

    # Command string and running modes.
    option_parser.add_option('-r', '--run', metavar='CMDSTR',
                             action='store', type='string', dest='run',
                             help='Lock and run the job, using CMDSTR.')
    option_parser.add_option('-c', '--clear', action='store_true',
                             dest='clear', default=False,
                             help='Clear any existing locks on the job.')
    option_parser.add_option('-f', '--force', action='store_true',
                             dest='force', default=False,
                             help='Force the job to run now regardless '
                                  'of last run time.')
    option_parser.add_option('-i', '--instance', action='store_true',
                             dest='instance', default=False,
                             help='Attempt to run an instance-style cron.')
    option_parser.add_option('-s', '--status', action='store_true',
                             dest='status', default=False,
                             help='Show the status of the job.')

    # Cron history stuff.
    option_parser.add_option('-l', '--log', action='store_true',
                             dest='log', default=False,
                             help='Log the duration of the run of this '
                                  'script regardless of whether or not '
                                  'it is a real cron.')
    option_parser.add_option('-k', '--keep', metavar='DAYS', action='store',
                             dest='keep', default=60, type='int',
                             help='Number of days to keep old history data '
                                  'for the provided command name.  '
                                  'Default=%default')
    option_parser.add_option('', '--report-begin', metavar='DAYS', type='int',
                             action='store', dest='report_begin', default=None,
                             help='Display performance report by day starting '
                                  'from specified day ago.  If command name '
                                  'is missed, print report for all commands '
                                  'in table "crons_history"')
    option_parser.add_option('', '--report-end', metavar='DAYS', type='int',
                             action='store', dest='report_end', default=None,
                             help='Display performance report by day ending '
                                  'at specified day ago.  If command name '
                                  'is missed, print report for all commands '
                                  'in table "crons_history"')

    # Checking number of similar jobs on current host.
    option_parser.add_option('', '--command-class', action='store',
                             dest='command_class', default=None,
                             help='When running, check how many jobs like '
                                  'this are being run by this host.  Takes '
                                  'a regular expression.')
    option_parser.add_option('', '--command-max', action='store',
                             type='int', dest='command_max', default=1,
                             help='When using a class of commands, ensure at '
                                  'most this number of jobs are running '
                                  'on this host.  Default=%default.')

    option_parser.add_option('', '--local', action='store_true',
                             dest='local', default=False,
                             help='Run a local cron.')

    (options, args) = option_parser.parse_args()

    _log = std_logger()
    if options.verbosity == 1:
        _log.setLevel(logging.INFO)
    elif options.verbosity >= 2:
        _log.setLevel(logging.DEBUG)

    db_name = None
    cmd_name = None

    if args:
        db_name = args[0]
    else:
        sys.stderr.write('Database name must be set.\n')
        sys.stderr.write(option_parser.format_help())
        return 1

    if len(args) > 1:
        cmd_name = args[1]

    # Check report ranges.
    if (options.report_begin is not None and
        options.report_end is not None and
        options.report_begin <= options.report_end):
        sys.stderr.write('Report\'s begin day must be less than '
                         'report\'s end day.\n')
        return 1

    # Print history report.
    if (options.report_begin is not None or
        options.report_end is not None):
        print(CronReport(db_name, cmd_name,
                         options.report_begin, options.report_end,
                         ).console_report(format_date=True))
        return 0

    if cmd_name is None:
        sys.stderr.write('Command name must be set.\n')
        sys.stderr.write(option_parser.format_help())
        return 1

    queries = _CronQueries(db_name)

    if cmd_name not in queries.commands_list():
        sys.stderr.write('No record for command %s in "crons" table'
                         % (cmd_name,))
        return 1

    # Check if there are any instance-style crons.
    if options.instance:
        instance_basic_name = '%s-%d' % (cmd_name, 0)
        if instance_basic_name not in queries.commands_list():
            sys.stderr.write('No instance-style crons found for command %s '
                             'in "crons" table' % (cmd_name,))
            return 1

    history_expire_time = options.keep * timeutils.ONEDAY
    cron_job = CronJob(db_name, cmd_name,
                       history_expire_time=history_expire_time,
                       command_class=options.command_class,
                       command_max=options.command_max,
                       local=options.local)

    # Show status of the job.
    if options.status:
        status_dict = cron_job.status()
        if status_dict['lastrunstart'] is not None:
            lastrunstart = time.ctime(status_dict['lastrunstart'])
        else:
            lastrunstart = '<unknown>'

        if status_dict['status'] == STATUS_READY:
            print('The job is ready to run again.  Last ran: %s.' %
                  (lastrunstart,))
        elif status_dict['status'] == STATUS_NOTYET:
            print('It is too soon to run the job.  Last ran: %s.' %
                  (lastrunstart,))
        elif status_dict['status'] == STATUS_LOCKED:
            print('The job is locked by \'%s\'.  Locked at: %s' %
                  (status_dict['hostname'], lastrunstart))
        return 0

    # Clear all locks on the job.
    if options.clear:
        cron_job.clear()

    # Try to run specified command.
    if options.run:
        return cron_job.run(command=options.run,
                            force=options.force,
                            instance=options.instance,
                            history_only=options.log)

    return 0


if __name__ == '__main__':
    sys.exit(main())
