"""The classcmd module is similar to the standard python cmd
module with the exception that command implementations happen in
callable classes rather than in do_<command>() methods.

To use this class, simply import and inherit from it, then define the
help and usage strings.  Place all callable command classes in an
appropriate package.  For example:

  import shared.util.cmdutils
  class echoargs(shared.util.cmdutils.ClassBasedCmd):
      help = '''My command.'''
      usage = '''Usage: echoargs [[arg1] [arg2] [argN]]'''
      def __call__(self, args):
          for arg in args:
              print(arg)

:Status: $Id: //prod/main/_is/shared/python/util/cmdutils.py#9 $
:Authors: jwescott, ohmelevs
"""

import cmd
import optparse
import shlex
import sys


class ClassCmdError(Exception):
    """Base class for classcmd errors."""
    pass


class RequiredArgumentsMissingError(ClassCmdError):
    """Exception class for missing required arguments."""
    def __init__(self, arguments):
        self.arguments = arguments


class RequiredArgumentMissingError(RequiredArgumentsMissingError):
    """Exception class for missing required arguments."""
    def __init__(self, arg):
        RequiredArgumentsMissingError.__init__(self, (arg,))


class UnknownCommandError(ClassCmdError):
    """Exception class for unknown commands."""
    def __init__(self, command):
        self.command = command


class UsageError(ClassCmdError):
    """Exception class for command usage errors."""
    pass


class ClassBasedCmd(cmd.Cmd):
    """Subclass this class in your own module.  See module help for
    details."""

    CMD_FAILURE = 1
    CMD_SUCCESS = 0

    def __init__(self, commands_package, commands):
        cmd.Cmd.__init__(self)
        self.commands_package = commands_package
        self.commands = commands
        self.stopped = False
        self.rc = None


    def emptyline(self):
        """Function to handle empty line.

        By default do nothing.  Can be overridden.

       :Return:
           None
        """
        pass

    def default(self, line):
        """Function to call when wrong command is specified.

        Can be overridden.

        :Parameters:
            - `line`: command line.

        :Return:
            None
        """
        self.setrc(self.CMD_FAILURE)
        self.stdout.write('*** Unknown command: %s\n' % line)

    def setrc(self, rc):
        """Sets up return code of last executed command.

        :Parameters:
            - `rc`: return code to set.

        :Return:
            None
        """
        self.rc = rc

    def getrc(self):
        """Function to get code of last executed command.

        :Return:
            Return code.
        """
        return self.rc

    def onecmd(self, line):
        """Function to execute a command.

        Overrides cmd.Cmd.onecmd() to use classes rather than
        methods for command execution.

        :Parameters:
            - `line`: command line.

        :Return:
            Result of the command execution.
        """
        try:
            self.rc = self.CMD_SUCCESS
            command, args, line = self.parseline(line)
            if not line:
                return self.emptyline()
            if command is None:
                return self.default(line)
            self.lastcmd = line
            if command == '':
                return self.default(line)
            elif command == 'help':
                return self.do_help(args)
            else:
                instance = self._get_command_class(command)()
                try:
                    lexed_args = shlex.split(args)
                except ValueError as e:
                    raise ClassCmdError(e)
                try:
                    return instance(lexed_args)
                except Exception:
                    self.setrc(self.CMD_FAILURE)
                    raise
        except UnknownCommandError as e:
            self.setrc(self.CMD_FAILURE)
            self.stdout.write('*** Unknown command: %s\n' % e.command)
        except RequiredArgumentsMissingError as e:
            if len(e.arguments) == 1:
                s = '<%s>' % (e.arguments[0])
            else:
                l = ['<%s>' % (arg) for arg in e.arguments]
                s = ', '.join(l[:-1]) + ', and %s' % (l[-1])
            self.setrc(self.CMD_FAILURE)
            self.stdout.write('*** Required argument(s) missing: %s\n' % (s))
        except ClassCmdError as e:
            self.setrc(self.CMD_FAILURE)
            self.stdout.write('*** Command Error: %s\n' % str(e))

    def postcmd(self, stop, line):
        """Hook method executed just after a command dispatch is finished.

        :Parameters:
            - `stop`: stop flag.
            - `line`: command line.

        :Return:
            Stop flag.
        """
        self.stopped = stop
        return cmd.Cmd.postcmd(self, stop, line)

    def do_help(self, arg):
        """Help command.

        Overrides cmd.Cmd.do_help() to use classes rather than
        methods for looking up the documentation.

        :Parameters:
            - `arg`: command line arguments.

        :Return:
            Help info.
        """
        no_help = 'No help available.'
        try:
            if arg:
                args = arg.split()
                if len(args) == 1:
                    command_class = self._get_command_class(args[0])
                    help_info = no_help
                    if hasattr(command_class, 'help'):
                        help_info = getattr(command_class, 'help')
                    self.stdout.write('%s\n' % str(help_info))

                    if hasattr(command_class, 'usage'):
                        self.stdout.write('%s\n' % str(command_class.usage))

                elif len(args) == 2:
                    # Subcommand.
                    cmdargs = [args[1], '--help']
                    instance = self._get_command_class(args[0])()
                    return instance(cmdargs)
            else:
                self.stdout.write("""Usage:
  help
  help <command>

Commands:
""")
                for c in self.commands:
                    command_class = self._get_command_class(c)
                    help_info = no_help
                    if hasattr(command_class, 'help'):
                        help_info = command_class.help
                    self.stdout.write('  %s -- %s\n' % (c, help_info))
                self.stdout.write('\n')
        except UnknownCommandError as e:
            self.setrc(self.CMD_FAILURE)
            return self.default(e.command)

    def _get_command_class(self, cmd):
        """Find a command class.

        Command classes must be found in the commands package as they are
        expected to be typed.
        """
        cmd_module = '%s.%s' % (self.commands_package, cmd)
        try:
            module = __import__(cmd_module, {}, {}, [cmd])
            if hasattr(module, cmd):
                return getattr(module, cmd)
            else:
                raise UnknownCommandError(cmd)
        except ImportError:
            raise UnknownCommandError(cmd)


class Option(optparse.Option):
    """Subclass of optparse.Option."""

    def take_action(self, action, dest, opt, value, values, parser):
        """Overridden option function to add help to the options.

        :Parameters:
            - `arg`: command line arguments.

        :Return:
            Help string.
        """
        if action == 'help':
            parser.print_help()
            values.help = True
        else:
            optparse.Option.take_action(self, action, dest, opt,
                                        value, values, parser)

STD_HELP_OPTION = Option('-h', '--help',
                         action='help',
                         help='show this help message')


class OptionGroup(optparse.OptionGroup):
    """Subclass of optparse.OptionGroup."""
    pass


class OptionParser(optparse.OptionParser):
    """Subclass of optparse.OptionParser."""

    def __init__(self,
                 usage=None,
                 option_list=None,
                 option_class=Option,
                 version=None,
                 conflict_handler='error',
                 description=None,
                 formatter=None,
                 add_help_option=False,
                 prog=None):
        optparse.OptionParser.__init__(self, usage, option_list, option_class,
                                       version, conflict_handler, description,
                                       formatter, add_help_option, prog)
        self.add_option(STD_HELP_OPTION)

    def get_default_values(self):
        """Function to get default values.

        :Return:
            Default optparse.Values
        """
        values = optparse.Values(self.defaults)
        values.help = False
        return values

    def error(self, msg):
        """Print a usage message incorporating 'msg' to stderr and exit.

        If you override this in a subclass, it should not return -- it
        should either exit or raise an exception.

        :Parameters:
            - `msg`: message.

        :Return:
            None
        """
        self.print_usage(sys.stderr)
        if '_get_prog_name' in dir(self):
            raise UsageError('%s: %s' % (self._get_prog_name(), msg))
        if 'get_prog_name' in dir(self):
            raise UsageError('%s: %s' % (self.get_prog_name(), msg))
        raise UsageError('%s' % (msg,))

# EOF
