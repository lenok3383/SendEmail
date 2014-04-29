"""This handles utility functions for communicating with a Perforce Server.

:Status: $Id: //prod/main/_is/shared/python/scm/perforce.py#6 $
:Authors: bjung ereuveni
:Last Updated By: $Author: vtorshyn $

Utility functions for common perforce operations.
"""

import os
import re
import shutil
import tempfile
import time

from shared.process import processutils

_perforce = None
TMP = '/tmp'


class PerforceException(Exception):
    pass


class Perforce(object):

    """Perforce class performs perforce functions."""

    # This regular expression is used to prepare a dictionary to be used to
    # gather the result returned from doing a p4 changes in the changes()
    # instance method when a long description is requested.

    CHANGE_RE_STR = '^Change\s+(?P<num>\d+)\s+on\s+(?P<date>[\d/]+)' \
                         '\s+by\s+(?P<user>\S+)$'

    # This regular expression is used to prepare a dictionary to be used to
    # gather the result returned from doing a p4 files in the files() instance
    # method.

    FILES_RE_STR = '^(?P<path>\S+?)\#(?P<rev>\d+)\s+\-\s+(?P<op>\S+)' \
                   '\s+change\s+(?P<num>\d+)\s+\((?P<type>\w+)\)\s*$'

    def __init__(self, user='p4', password='', host='perforce.ironport.com',
            port=1666):
        """Constructor for Perforce object.  A wrapper for Perforce functions.

        For each new perforce function, it will be logged in and logged out.

        :param user: Username to login to perforce with.  Default: 'p4'.
        :param password: Password to login to perforce with.  Default: ''.
        :param host: Hostname of the perforce server.  Default:
            'perforce.ironport.com'.
        :param port: Port number of the perforce server.  Default: 1666
        """

        self.change_re = re.compile(Perforce.CHANGE_RE_STR)
        self.files_re = re.compile(Perforce.FILES_RE_STR)

        # Environment variables are used to pass in a different
        # OS environment to the subprocess.

        self.env_vars = dict(os.environ)
        self.env_vars['P4USER'] = user
        self.env_vars['P4PORT'] = '%s:%s' % (host, port)

        self._password = password

    def __enter__(self):
        """Enter code. Logs in to perforce.

        :Raises: PerforceException
        """

        # Create a temporary directory used to store the p4 tickets file.
        # This will become X/perforce_Y where X is the temp_dir from
        # tempfile and Y is randomized characters from tempfile module.
        # X is pulled from environment vars TMPDIR, TEMP, or TMP, or platform
        # specific attributes, or the current working directory.

        temp_dir = tempfile.mkdtemp(prefix='perforce_')
        self.env_vars['P4TICKETS'] = os.path.join(temp_dir, '.p4tickets')
        self.env_vars['TMP'] = temp_dir

        command = 'p4 login'

        try:
            processutils.execute(command,
                command_input=('%s\n' % (self._password,)), timeout=30,
                env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)

        return self

    def __exit__(self, err_type, err_value, trace_back):
        """Exit code. Process exception and do logout from perforce.

        :Raises: PerforceException
        """

        command = 'p4 logout'
        try:
            processutils.execute(command, timeout=30, env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)
        finally:
            # Check for existence of the temp directory.
            # If it exists, delete it.
            temp_dir = self.env_vars['TMP']
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)

    def files(self, path, show_deleted=True):
        """Returns a list of the files in dirname.

        :Parameters:
            `path`: path name of the files to return.

        :Return: a list of dictionaries, where each
            dictionary contains: {
                'path':...,
                'revision':...,
                'operation':...,
                'change_number':...,
                'type':...
            }
        :Raises: PerforceException
        """

        files = []

        command = 'p4 files %s' % (path,)
        try:
            output = processutils.execute(command, read_output=True,
                env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)

        for line in output.splitlines():
            match = self.files_re.match(line)
            if match:
                if not show_deleted and match.group('op') == 'delete':
                    continue
                files.append({'path': match.group('path'),
                    'revision': int(match.group('rev')),
                    'operation': match.group('op'),
                    'change_number': int(match.group('num')),
                    'type': match.group('type')})

        return files

    def describe(self, change_id):
        """Returns description for change_id (change #).

        :Parameters:
            `change_id`: change number

        :Return:
            Output of 'p4 describe'.
        :Raises: PerforceException
        """
        try:
            command = 'p4 describe -s %d' % (change_id,)
            output = processutils.execute(command, read_output=True,
                env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)
        except TypeError as err:
            raise PerforceException(
                'Wrong change id: %s %s' % (
                    str(change_id),
                    str(type(change_id))))
        return output

    def changes(self, path, timeout=10, max_results=None):
        """Returns a list of the changes.

        :Parameters:
            `path`: path to call p4 changes on.
            `timeout`: Seconds to wait before timeout and process is killed.
                       Default: 10
            `max_results`: Maximum results to retrieve.  Default: None.

        :Return: a list of dictionaries of all changes.
        :Raises: PerforceException
        """

        changes = []
        errors = []

        command_args = ['p4', 'changes', '-l']
        if max_results:
            command_args.extend(['-m', str(max_results)])

        command_args.append(path)

        def stdout_callback(line):
            match = self.change_re.match(line)
            if match:
                date = match.group('date')
                formatted_time = time.strptime(date, '%Y/%m/%d')
                date_ts = int(time.mktime(formatted_time))
                changes.append({'change_number': int(match.group('num')),
                    'change_date': date_ts,
                    'user': match.group('user'),
                    'description': []})
            else:
                changes[-1]['description'].append(line)

        try:
            return_code = processutils.timed_subprocess(command_args,
                run_timeout=timeout, kill_timeout=30,
                stdout_func=stdout_callback, stderr_func=errors.append,
                env=self.env_vars)
            for change in changes:
                change['description'] = ''.join(change['description'])
        except processutils.ExecuteError as err:
            raise PerforceException(err)

        if return_code:
            raise PerforceException(
                'Error encountered while running p4 changes.'
                ' Return code: %s, error: %s' % (return_code,
                ''.join(errors)))

        return changes

    def get(self, path):
        """Returns a string containing the data of the file(s).

        :Parameters:
            `path`: Location of path to print.

        :Return: Output of the file contents.
        :Raises: PerforceException
        """

        command = 'p4 print -q %s' % (path,)
        try:
            output = processutils.execute(command, read_output=True,
                timeout=600, env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)

        return output

    def client(self, clientname=''):
        """Returns the client specification and view.

        :Parameters:
            `clientname`: Client workspace name to view. Default: ''

        :Return: Output of the client spec as a string.
        :Raises: PerforceException
        """

        command = 'p4 client -o %s' % (clientname,)
        try:
            output = processutils.execute(command, read_output=True,
                timeout=600, env=self.env_vars)
        except processutils.ExecuteError as err:
            raise PerforceException(err)

        return output

    def depot_path(self):
        """Returns the (assumed) root source depot path.

        :Return: Depot path from the first view spec mapping source to client.
        """

        depot = None
        clientstr = self.client()
        # Look for depot path in View section, for only the very first mapping
        matchObj = re.search(r'(?<=View:\n)\s*(.*) ', clientstr)
        if matchObj:
            depot = matchObj.group(1)

        return depot
