"""MockControl mock management class.

:Status: $Id: //prod/main/_is/shared/python/testing/vmock/mockcontrol.py#3 $
:Author: vburenin
"""

import inspect

from shared.testing.vmock.methodmock import MethodMock, StaticMethodMock
from shared.testing.vmock.mockcallaction import MockCallAction
from shared.testing.vmock.mockerrors import (CallSequenceError, MockError,
                                             CallsNumberError)


NOT_MOCKABLE_METHODS = set([
    '__class__', '__del__', '__dict__',
    '__getattr__', '__init__', '__instancecheck__',
    '__new__', '__setattr__', '__subclasscheck__',
    '__weakref__', '__getattribute__'
])

PROPERTY_GET_PREFIX = '__fget__'
PROPERTY_SET_PREFIX = '__fset__'
PROPERTY_DEL_PREFIX = '__fdel__'


class FakeMockObject(object):

    """Class that is used to create fake object with virtual methods"""

    def __init__(self):
        self._class__properties__names = set()

    def __getattribute__(self, name):
        try:
            property_names = \
                object.__getattribute__(self, '_class__properties__names')
        except AttributeError:
            property_names = set()

        if name in property_names:
            return self.__getattribute__(PROPERTY_GET_PREFIX + name)()
        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        try:
            property_names = \
                object.__getattribute__(self, '_class__properties__names')
        except AttributeError:
            property_names = set()

        if name in property_names:
            self.__getattribute__(PROPERTY_SET_PREFIX + name)(value)
        else:
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        property_names = \
            object.__getattribute__(self, '_class__properties__names')
        if name in property_names:
            self.__getattribute__(PROPERTY_DEL_PREFIX + name)()
        else:
            object.__delattr__(self, name)

    def _mock_add_object_property_name(self, name):
        self._class__properties__names.add(name)

    def _mock_get_object_property_names(self):
        return self._class__properties__names


class MockControl(object):

    """This class creates mock objects such as class objects,
    class constructors, functions. It also controls all calls
    to mock methods, sequence of calls and restore everything
    back at the end.
    """

    def __init__(self):
        self.__exp_queue = []
        self.__stubs = {}
        self.__object_mocks = {}
        self.__record = True
        self.__play_pointer = 0
        self.__current_action = None
        self.__static_stubs = {}
        self.__error = None

    def mock_constructor(self, module, class_name,
                         arg_spec=None, display_name=None):
        """Mocks the constructor of the specified class.

        '__init__' method of the specified class will be mocked. To make this
        mock working, all call steps should be pre-recorded before going into
        replay mode. For example:

            mc = mockcontrol.MockControl()
            ctor_mock = mc.mock_constructor(helpers.utils, 'MyCoolClass')
            ctor_(1, 2).returns(SomeFakeObject)

            mc.replay()
            ....
            # Will print SomeFakeObject
            print ctor_mock(1,2)
            ....
            mc.verify()

        :param module: Module where you want to mock your class constructor.
        :param class_name: String name of the class.
        :param arg_spec: If constructor arguments are not known, you may
                specify your own in format of 'inspect.getargspec'.
        :param display_name: Name that will be used for mocked object
                when error happens. Instead 'MethodMock.__init__' you will
                see string like: display_name.__init__
        :return: Constructor mock object.
        """
        return self.__create_ctor_mock(module, class_name, arg_spec,
                                       False, display_name)

    def stub_constructor(self, module, class_name,
                         arg_spec=None, display_name=None):
        """Creates immediately responsive constructor stub of the specified class.

        '__init__' method of the specified class will be replaced by stub.
        Stub become working immediately after it is defined how it is expected
        to be called. You should switch to replay mode for that. For example:

            mc = mockcontrol.MockControl()
            ctor_mock = mc.stub_constructor(helpers.utils, 'MyCoolClass')
            ctor_(1, 2).returns(SomeFakeObject)

            # Will print SomeFakeObject
            print ctor_mock(1,2)

            mc.replay()
            ....
            ....
            mc.verify()

        :param module: Module where you want to stub your class constructor.
        :param class_name: String name of the class.
        :param arg_spec: If constructor arguments are not known, you may
                specify your own in format of 'inspect.getargspec'.
        :param display_name: Name that will be used for mocked object
                when error happens. Instead 'MethodMock.__init__' you will
                see string like: display_name.__init__
        :return: Constructor stub object.
        """
        return self.__create_ctor_mock(module, class_name, arg_spec,
                                       True, display_name)

    def mock_class(self, class_definition, display_name=None):
        """Creates new fake object with all mocked class methods.

        Rather than mocking some specific class methods, you may create
        fake object with all the class methods where each method is mocked.
        It doesn't mock original class, it creates a copy of that one.

        Because of Python specific some methods can not be mocked.
        All of them are defined in NOT_MOCKABLE_METHODS constant.
        So, it means they will not be copied at all.
        Pay attention, that we are not mocking __init__ method too,
        since it is a special case. To mock it, you should do it separately.

        mock_class also mocks 'properties'. It is really nice feature.

        For example:
            # Init mock control class.
            mc = mockcontrol.MockControl()
            # Mock whole class.
            class_mock = mc.mock_class(helpers.utils.MyCoolClass)

            # Mock class constructor.
            class_ctor_mock = mc.mock_constructor(helpers.utils, 'MyCoolClass')

            # Say that mocked constructor should return our fake object.

            class_ctor_mock(1, 2, 3).returns(class_mock)

            # Record steps.
            class_mock.do_something().returns(10)
            class_mock.make_tea(sugar_spoons=5).returns('sweet tea')
            class_mock.feed_hummingbirds('sweet water').returns('happy birds')

            # Let record steps for properties.
            class_mock.amount_of_hummingbirds = 6
            class_mock.add_one_more_bird()
            class_mock.amount_of_hummingbirds.returns(7)

            # Call delete of the property.
            del class_mock.amount_of_hummingbirds

            # Go to replat mode!
            mc.replay()

            cool_class = helpers.utils.MyCoolClass(1, 2, 3)

            # Will print 10.
            print cool_class.do_something()
            # Will print 'sweat tea'
            print cool_class.make_tea(sugar_spoons=5)
            # Will print 'happy birds'
            print cool_class.feed_hummingbirds()
            ....
            # This should be performed because of expected to be.
            cool_class.amount_of_hummingbirds = 6
            cool_class.add_one_more_bird()

            # Will print 7.
            print cool_class.amount_of_hummingbirds

            # 'del' must be called.
            del cool_class.amount_of_hummingbirds


            # Will fail, it is not expected call.
            print cool_class.do_something()

            mc.verify()

        :param class_definition: Class that you'd like to mock.
        :param display_name: Name that will be used for mocked object
                when error happens. Instead 'MethodMock.do_something' you will
                see string like: display_name.do_something
        :return: Fake object class.
        """

        return self.__create_object_mock(class_definition, False, display_name)

    def stub_class(self, class_definition, display_name=None):
        """Creates immediately responsive obj stub based on class definition.

        Rather than stubbing some specific class methods, you may create
        fake object with all the class methods where each method is a stub.
        It doesn't stub original class, it creates a copy of that one.

        Because of Python specific some methods can not be stubbed.
        All of them are defined in NOT_MOCKABLE_METHODS constant.
        So, it means they will not be copied at all.
        Pay attention, that we are not stubbing __init__ method too,
        since it is a special case. To stub it, use stub_constructo.

        For example:
            # Init mock control class.
            mc = mockcontrol.MockControl()

            # stub whole class.
            class_stub = mc.stub_class(helpers.utils.MyCoolClass)

            # Init stubs.

            class_stub.do_something().returns(10)
            # Will print 10.
            print class_stub.do_something()

            class_stub.make_tea(sugar_spoons=5).returns('sweet tea')
            # Will print 'sweat tea'
            print class_mock.make_tea(sugar_spoons=5)

            mc.replay()
            ....
            mc.verify()

        :param class_definition: Class that you'd like to stub.
        :param display_name: Name that will be used for stubbed object
                when error happens. Instead 'MethodMock.do_something' you will
                see string like: display_name.do_something
        :return: Fake object class.
        """
        return self.__create_object_mock(class_definition, True, display_name)

    def mock_obj(self, obj, display_name=None):
        """Creates mocked object mock based on obj definition.

        This method works absolutely like mock_class, but it scans
        not a class definition, it scans instantiated object with all its
        attributes which could be added after instantiation.

        Look mock_class help for examples.

        :param obj: object.
        :param display_name: Name that will be used for mocked object
                when error happens. Instead 'MethodMock.do_something' you will
                see string like: display_name.do_something
        :return: Fake object based on object.
        """
        return self.__create_object_mock(obj, False, display_name)

    def stub_obj(self, obj, display_name=None):
        """Creates immediately responsive obj stub based on obj definition.

        This method works absolutely like stub_class, but it scans
        not a class definition, it scans instantiated object with all its
        attributes which could be added after instantiation.

        Look stub_class help for examples.

        :param obj: object.
        :param display_name: Name that will be used for stubbed object
                when error happens. Instead 'MethodMock.do_something' you will
                see string like: display_name.do_something
        :return: Fake object based on object.
        """
        return self.__create_object_mock(obj, True, display_name)

    def mock_method(self, obj, method_name, arg_spec=None, display_name=None):
        """Create method/function mock.

        import time
        mc = mockcontrol.MockControl()
        mc.mock_method(time, 'time')().returns(10)

        mc.replay()

        # Will print 10
        time.time()

        mc.verify()

        :param obj: The module/object/class where function of method should be
                mocked.
        :param method_name: String method name.
        :param arg_spec: If func/method arguments are not known, you may
                specify your own in format of 'inspect.getargspec'.
        :param display_name: Name that will be used for mocked object
                when error happens. Instead 'MethodMock.method_name' you will
                see string like: display_name.method_name
        """
        return self.__create_mock(obj, method_name, arg_spec,
                                  False, display_name)

    def stub_method(self, obj, method_name, arg_spec=None, display_name=None):
        """Create method/function immediately responsive stub.

        import random
        mc = mockcontrol.MockControl()
        mc.stub_method(random, 'randint')(1, 2).returns(10)
        mc.stub_method(random, 'randint')(2, 4).returns(5)
        # Will print 5
        random.randint(2, 4)
        # Will print 10
        random.randint(1, 2)

        :param obj: The module/object/class where function of method should be
                stubbed.
        :param method_name: String method name.
        :param arg_spec: If func/method arguments are not known, you may
                specify your own in format of 'inspect.getargspec'.
        :param display_name: Name that will be used for stubbed object
                when error happens. Instead 'MethodMock.method_name' you will
                see string like: display_name.method_name
        """
        return self.__create_mock(obj, method_name, arg_spec,
                                  True, display_name)

    def replay(self):
        """Switches from recording to replay mode."""
        self.__save_current_action()
        self.__record = False

    def verify(self):
        """Do post execution verification."""
        # Verify possible handled vmock errors.
        self._check_error()

        # Verify expectation queue.
        next_expected = self._pop_current_record()
        if next_expected is not None:
            err_txt = 'There are more steps to call. ' \
                      'Current call is: ' + str(next_expected)
            raise CallSequenceError(err_txt)

        # Verify stubs.
        errors = []
        for stub in self.__stubs.itervalues():
            for action in stub:
                error_text = action.get_call_error()
                if error_text:
                    errors.append(str('%s - %s' % (str(action), error_text)))

        # Verify static stubs.
        for stub in self.__static_stubs.itervalues():
            for action in stub:
                error_text = action.get_call_error()
                if error_text:
                    errors.append(str('%s - %s' % (str(action), error_text)))

        if errors:
            raise CallsNumberError('\n'.join(errors))

    def tear_down(self):
        """Restore all mocked function/method/classes back."""
        for methods in self.__object_mocks.itervalues():
            for method_data in methods.itervalues():
                if method_data is not None:
                    method_data._restore_original()

    def _raise_error(self, error):
        """Generated errors must be stored, otherwise if test code
        contains except/finally clauses incorrect error can be generated at
        the end because mock control error may be handled in those clauses.
        """
        self.__error = error
        raise error

    def _check_error(self):
        """Raise an error if it happened already but was handled."""
        if self.__error is not None:
            raise self.__error

    def _get_new_action(self, obj, args, kwargs):
        """Saves current action and creates new one."""
        assert self.__record, 'The play mode is set'
        self.__save_current_action()
        self.__current_action = MockCallAction(obj, args, kwargs)
        return self.__current_action

    def _get_new_static_action(self, obj, args, kwargs):
        """Creates new static action."""

        assert self.__record, 'The play mode is set'

        static_action = MockCallAction(obj, args, kwargs)
        # Default static action can be called any times times.
        static_action.anyorder().anytimes()

        # Check if there is no stubs already exists.
        for action in self.__static_stubs.get(obj, []):
            if static_action._compare_args(action.args, action.kwargs):
                raise ValueError('Static stub already exists!')

        # Create container for MethodMock stubs.
        if obj not in self.__static_stubs:
            self.__static_stubs[obj] = []

        self.__static_stubs[obj].append(static_action)
        return static_action

    def _is_recording(self):
        """Check if we are in recording mode."""
        return self.__record


    def _find_stub(self, mock_obj, args, kwargs):
        """Find existing stub and increase call counter."""
        for action in self.__stubs.get(mock_obj, []):
            if action._compare_args(args, kwargs):
                return action
        return None

    def _find_static_mock(self, mock_obj, args, kwargs):
        """Find existing stub and increase call counter."""
        for action in self.__static_stubs.get(mock_obj, []):
            if action._compare_args(args, kwargs):
                return action
        return None

    def _pop_current_record(self):
        """Pop next record from the expectation queue."""
        assert not self.__record, "MockControl is still in record mode"
        try:
            if self.__play_pointer > 0:
                exp_call = self.__exp_queue[self.__play_pointer - 1]
                if not (exp_call._is_times_limit()):
                    return exp_call

            self.__play_pointer += 1
            return self.__exp_queue[self.__play_pointer - 1]
        except IndexError:
            return None

    def __create_ctor_mock(self, module, class_name, arg_spec,
                           static, display_name):
        """Creates mock for class constructor."""
        if arg_spec is not None:
            return self.__create_mock(module, class_name, arg_spec,
                                      static, display_name)

        class_def = getattr(module, class_name)
        try:
            init_spec = inspect.getargspec(class_def.__init__)
            init_spec = inspect.ArgSpec(init_spec[0][1:], init_spec[1],
                                        init_spec[2], init_spec[3])
        except TypeError:
            init_spec = inspect.ArgSpec([], 'args', 'kwargs', None)
        except AttributeError:
            init_spec = inspect.ArgSpec([], None, None, None)

        return self.__create_mock(module, class_name, init_spec,
                                  static, display_name)

    def __create_object_mock(self, obj_class, static, display_name):
        """Scans an object interface and mock each found method"""

        is_class = inspect.isclass(obj_class)
        if is_class:
            obj_to_scan = obj_class
        else:
            obj_to_scan = obj_class.__class__

        fake_object = FakeMockObject()

        for attr in inspect.classify_class_attrs(obj_to_scan):
            if attr.name in NOT_MOCKABLE_METHODS:
                continue

            # Just copy original attribute into the fake object.
            obj_attr = getattr(obj_class, attr.name)
            setattr(fake_object, attr.name, obj_attr)

            if (attr.kind == 'property'):
                fake_object._mock_add_object_property_name(attr.name)

                f_get = PROPERTY_GET_PREFIX + attr.name
                f_set = PROPERTY_SET_PREFIX + attr.name
                f_del = PROPERTY_DEL_PREFIX + attr.name

                setattr(fake_object, f_get, obj_attr)
                setattr(fake_object, f_set, obj_attr)
                setattr(fake_object, f_del, obj_attr)

                if static:
                    mock_func = self.stub_method
                else:
                    mock_func = self.mock_method

                mock_func(fake_object, f_set, display_name=display_name)
                mock_func(fake_object, f_get, display_name=display_name)
                mock_func(fake_object, f_del, display_name=display_name)
                continue

            if attr.kind in ('method', 'static method'):
                if static:
                    self.stub_method(fake_object, attr.name,
                              display_name=display_name)
                else:
                    self.mock_method(fake_object, attr.name,
                              display_name=display_name)
                continue

        # If class, there are nothing to do. Just exit.
        if is_class:
            return fake_object

        # Continue to scan object to find possible new attributes.
        mocked_attr_names = set(dir(fake_object))
        property_names = fake_object._mock_get_object_property_names()
        for attr_name in dir(obj_class):
            # Skip all already mocked method and properties.
            if attr_name in mocked_attr_names or attr_name in property_names:
                continue

            obj_attr = getattr(obj_class, attr_name)
            setattr(fake_object, attr_name, obj_attr)
            if inspect.ismethod(obj_attr) or inspect.isfunction(obj_attr):
                if static:
                    self.stub_method(fake_object, attr.name,
                              display_name=display_name)
                else:
                    self.mock_method(fake_object, attr.name,
                              display_name=display_name)
                continue

        return fake_object

    def __create_mock(self, obj, method_name, arg_spec, static, display_name):
        """Creates mock for function or class/object method."""

        assert isinstance(method_name, str), 'Method name must be a string'

        if not hasattr(obj, method_name):
            raise ValueError('Object does not have such method: %s' % \
                             (method_name,))

        if obj not in self.__object_mocks:
            self.__object_mocks[obj] = {}

        if method_name not in self.__object_mocks[obj]:
            self.__object_mocks[obj][method_name] = None

        old_method = getattr(obj, method_name)

        if isinstance(old_method, MethodMock):
            raise MockError('Method \'%s\'is already mocked!' % (method_name,))

        if arg_spec is None:
            try:
                method_args = inspect.getargspec(old_method)
            except TypeError:
                method_args = inspect.ArgSpec([], 'args', 'kwargs', None)
        else:
            assert isinstance(arg_spec, inspect.ArgSpec), \
                'Incorrect arg_spec type, must be inspect.ArgSpec'
            method_args = arg_spec

        if static:
            new_mock = StaticMethodMock(method_name,
                                        method_args,
                                        self, old_method,
                                        obj, display_name=display_name)
        else:
            new_mock = MethodMock(method_name,
                                  method_args,
                                  self, old_method,
                                  obj, display_name=display_name)
        self.__object_mocks[obj][method_name] = new_mock

        setattr(obj, method_name, new_mock)

        return new_mock

    def __add_record(self, call_action):
        """Adds new MockCall action to an appropriate storage."""

        assert self.__record, 'The play mode is set'

        # Check if there is no stubs already exists.
        for stub_action in self.__stubs.get(call_action.obj, []):
            if call_action._compare_args(stub_action.args, stub_action.kwargs):
                raise ValueError('Stub already exists!')

        if call_action.is_ordered:

            # If action is stub we should check if such doesn't exist already
            # in the expectation queue.

            for e_call in self.__exp_queue:
                if call_action._compare_args(e_call.args, e_call.kwargs):
                    raise ValueError('Pattern exists in the expect queue')
            # Create container for MethodMock stubs.
            if call_action.obj not in self.__stubs:
                self.__stubs[call_action.obj] = []
            self.__stubs[call_action.obj].append(call_action)
        else:
            # Save as sequence if it is not a stub.
            self.__exp_queue.append(call_action)

    def __save_current_action(self):
        """Saves current action if new one is requested."""
        if self.__current_action is not None:
            self.__add_record(self.__current_action)
