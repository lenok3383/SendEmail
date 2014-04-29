"""MethodMock class that keeps method data.

:Status: $Id: //prod/main/_is/shared/python/testing/vmock/methodmock.py#3 $
:Author: vburenin
"""

import inspect

from shared.testing.vmock import matchers
from shared.testing.vmock.mockerrors import CallSequenceError
from shared.testing.vmock.mockerrors import InterfaceError
from shared.testing.vmock.mockerrors import UnexpectedCall

class MethodMock(object):

    """Method mock.

    Method mock object records all method calls with specific parameters,
    and store them in appropriate queue that depends on type of Mock Call.
    """

    def __init__(self, method_name, arg_spec, mock_control, orig_method,
                 orig_obj, display_name=None):
        """Constructor.

        :param method_name: Mocked method name.
        :param arg_spec: Argument specification for mocked method.
        :param mock_control: Parent MockControl object.
        :param orig_method: Reference to the original method.
        :param orig_obj: Reference to the original object of mocked method.
        :param display_name: Name that will be displayed as class/module,
                name of mocked method/function
        """
        # Mocked method/function name.
        self._method_name = method_name

        # Method/Function argument specification.
        arg, vararg, varkwarg, defaults = arg_spec
        # If it is class method, let skip first 'self' parameter.
        if inspect.ismethod(orig_method):
            arg = arg[1:]

        # Build map for default values.
        new_defaults = {}
        if defaults:
            for v_name, v_val in zip(arg[-len(defaults):], defaults):
                new_defaults[v_name] = v_val

        self.__arg_spec = (arg, vararg, varkwarg, new_defaults)

        # Parent Mock Control.
        self._mc = mock_control
        # Original mocked object.
        self.__orig_obj = orig_obj
        # Pointer to the original mocked method.
        self.__orig_method = orig_method
        # Name to be displayed for this method mock.
        self.__display_name = display_name

    def __call__(self, *args, **kwargs):
        """Record or execute expected call.

        Behavior of MethodMock object call depends on current mode.
        In record mode it saves all calls and then reproduces them
        in replay mode.

        :param args, kwargs: Parameters are variable and depend on mocked
                method or function.
        """

        # Each mock call record error if such has happened, since it may be
        # handled by function which you are testing.  So, each next call of
        # other mocks will throw saved error.
        self._mc._check_error()
        if self._mc._is_recording():
            return self._save_call(args, kwargs)
        else:
            return self._make_call(args, kwargs)

    def __str__(self):
        if self.__display_name:
            return '(MethodMock): %s.%s' % (self.__display_name,
                                            self._method_name)
        else:
            return '(%s): MethodMock.%s' % (object.__str__(self),
                                            self._method_name)

    @property
    def method_name(self):
        """Mocked method name"""
        return self._method_name

    def _verify_interface(self, args, kwargs):
        """Verify mock call with original function interface.

        This method verify that is expected call fits the original
        function interface.

        :param args: Args how is method mock expected to be called.
        :param kwargs: Keyword args how is method mock expected to be called.
        """

        var_names, varargs, varkw, defaults = self.__arg_spec
        vars_to_init = set(var_names)

        # This variable is a dictionary for debug purpose. Do not remove it.
        # If something wrong, it helps to test this method.
        # After some long period of time when we are sure that everything
        # works really well, it can be replaced just by 'set'.

        init_vars = {}

        if args and isinstance(args[0], matchers._AnyArgsMatcher):
            if len(args) > 1:
                raise InterfaceError('_AnyArgsMatcher can be alone only')
            else:
                return

        # Check if there are no more than allowed number of args.
        if len(args) > len(var_names) and varargs is None:
            raise InterfaceError('%s() takes exactly %d arguments (%d given)' %
                                (self._method_name, len(var_names), len(args)))

        # Init all args,
        # Number of expected args and received args may be different, because
        # part of them may be initialized as keywords. At the end we should
        # check only the list of not initialized variables.

        for var_name, var_val in zip(var_names, args):
            init_vars[var_name] = var_val
            vars_to_init.remove(var_name)

        # Go through kwargs to make sure if variable is not initialized already
        for var_name, var_val in kwargs.iteritems():
            if isinstance(var_val, matchers._AnyArgsMatcher):
                raise InterfaceError('_AnyArgsMatcher can not be a keyword')
            if var_name in init_vars:
                raise InterfaceError(
                        '%s() got multiple values for keyword argument \'%s\''
                        % (self._method_name, var_name,))

            if var_name in vars_to_init:
                init_vars[var_name] = var_val
                vars_to_init.remove(var_name)
            elif varkw is None:
                raise InterfaceError(
                        '%s() got an unexpected keyword argument \'%s\''
                        % (self._method_name, var_name))

        # Initialize defaults if not initialized
        for var_name in vars_to_init.copy():
            if var_name in defaults:
                init_vars[var_name] = defaults[var_name]
                vars_to_init.remove(var_name)

        if vars_to_init:
            at_lease = len(var_names) - len(defaults)
            given = at_lease - len(vars_to_init)
            raise InterfaceError(
                    '%s() takes at least %s non-keyword arguments (%s given)'
                    % (self._method_name, at_lease, given))

    def _restore_original(self):
        """Restore original method/function"""
        setattr(self.__orig_obj, self._method_name, self.__orig_method)

    def _save_call(self, args, kwargs):
        """Save current call"""
        self._verify_interface(args, kwargs)
        return self._mc._get_new_action(self, args, kwargs)

    def _make_call(self, a_args, a_kwargs):
        """Mock call.

        Method performs a call of mocked method and returns an appropriate
        value. It checks what is going to be call, stub first and mock second.
        If there are no such call stub or expected call in the expectation
        queue. The CallSequenceError or UnexpectedCall exceptions will be
        raised.

        :params a_args: Actual call arguments.
        :params a_kwargs: Actual call keyword arguments.
        :return: Expected result.
        :raise: CallSequenceError or UnexpectedCall if call is unexpected.
        """

        # Find stub first.
        e_data = self._mc._find_stub(self, a_args, a_kwargs)

        # If there are no stubs, get call from the queue of expectors
        if e_data is None:
            e_data = self._mc._pop_current_record()

        # Failure if there are no stubs and expectors in the queue.
        if e_data is None:
            error = CallSequenceError(
                        'No more calls are expected. \n'
                        'Actual call: %s, with args: %s' %
                        (str(self), self._args_to_str(a_args, a_kwargs)))
            self._mc._raise_error(error)

        if e_data.obj != self or not e_data._compare_args(a_args, a_kwargs):
            err_str = ('Unexpected method call.\n'
                       'Expected object: %s\n'
                       'Expected args: %s\n'
                       'Actual object: %s\n'
                       'Actual args: %s\n')
            fmt_params = (str(e_data.obj),
                          self._args_to_str(e_data.args, e_data.kwargs),
                          str(self), self._args_to_str(a_args, a_kwargs))
            error = UnexpectedCall(err_str % fmt_params)
            self._mc._raise_error(error)

        return e_data._get_result(*a_args, **a_kwargs)

    def _args_to_str(self, args, kwargs):
        """Format arguments in appropriate way."""
        num_types = (int, long)
        args_str = []
        kwargs_str = {}
        for arg in args:
            if isinstance(arg, num_types):
                args_str.append(arg)
            else:
                args_str.append(str(arg))

        for key in kwargs.keys():
            if isinstance(kwargs[key], num_types):
                kwargs_str[key] = kwargs[key]
            else:
                kwargs_str[key] = str(kwargs[key])

        return '(%s, %s)' % (args_str, kwargs_str)


class StaticMethodMock(MethodMock):

    """Used for immediate response after mocking."""

    def __call__(self, *args, **kwargs):
        self._mc._check_error()
        e_data = self._mc._find_static_mock(self, args, kwargs)
        if e_data is None:
            if self._mc._is_recording():
                self._verify_interface(args, kwargs)
                return self._mc._get_new_static_action(self, args, kwargs)
            else:
                self._mc._raise_error(CallSequenceError(
                    'There is no static mock for this call. \n'
                    'Actual call: %s, with args: %s' %
                    (str(self), self._args_to_str(args, kwargs))))
        else:
            return e_data._get_result(*args, **kwargs)
