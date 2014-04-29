"""Module for dealing with task execution in separate treads. It takes care
about all data sharing and concurrency problems.

Includes two main classes:
    - SimpleTaskDispatcher - execute a task using one of the threads from
                             the thread-pool.
    - SimpleJobExecutor - execute a number of small tasks in separate threads
                          and return the overall result.

:Status: $Id: //prod/main/_is/shared/python/util/threadutils.py#7 $
:Authors: ted, bejung, rbodnarc
"""

import atexit
import collections
import sys
import threading
import time


# named tuple for tasks
Task = collections.namedtuple('Task', ('function', 'args', 'kwargs'))

# named tuple for task queue status
Status = collections.namedtuple('Status', ('threads',
                                          'tasks_in_queue',
                                          'tasks_in_processing'))


class ThreadError(Exception):
    """Base class for threading errors."""


class TaskQueueIsFullError(ThreadError):
    """Exception raised if tasks' queue reach its maximum size."""


class ConcurrentDispatcherError(ThreadError):
    """Exception raised if executor threads aren't ready to start."""


class UnknownTaskException(ThreadError):
    """Exception raised if a specified task handler is not found."""


class TimeoutException(ThreadError):
    """Exception raised if some operation's timeout reached."""


class _WorkerThread(threading.Thread):

    """Subclass of threading.Thread which is able to monitor our task queue.
    """

    def __init__(self, worker_id, work_queue, stop_event, exit_callback,
                 start_event, current_task_list, max_worker_idle_period=60,
                 poll_interval=0.1):
        """Constructor for _WorkerThread.

        :Parameters:
            - `worker_id`: Our internal id of this thread.
            - `work_queue`: Queue with tasks which must be processed.
            - `stop_event`: threading.Event object which signals that thread
              should be stopped.
            - `exit_callback`: Callback function which should be called before
              the thread termination.
            - `start_event`: threading.Event object which should be set when
              task execution started.
            - `current_task_list`: List containing markers for all currently
              executed tasks.  I.e. that when we start execution we should add
              one element to it, and after execution we should pop one element
              from it.
            - `max_worker_idle_period`: Maximum time for a worker to
              remain idle before exiting.
            - `poll_interval`: Time in seconds to recheck queue state.
        """
        super(_WorkerThread, self).__init__(
            name='_WorkerThread-%d' % (worker_id,))
        self._queue = work_queue
        self._stop_event = stop_event
        self._exit_callback = exit_callback
        self._timeout = max_worker_idle_period
        self._id = worker_id
        self._poll_interval = poll_interval
        self._start_event = start_event
        self._current_task_list = current_task_list

    def run(self):
        """Start work as one of thread-pool's threads.

        Thread will continue monitoring tasks' queue until it catches
        stop_event signal or tasks' queue is empty for a long time.
        """
        try:
            while not self._stop_event.is_set():
                poll_start = time.time()
                while not self._queue:
                    # if another thread grabbed last event
                    # we still need to signal the main thread
                    # to continue
                    if not self._start_event.is_set():
                        self._start_event.set()
                    time.sleep(self._poll_interval)
                    if self._stop_event.is_set() or \
                        time.time() - poll_start >= self._timeout:
                        return
                try:
                    # append to current task list first to avoid
                    # race condition when figuring out if we have
                    # finished processing
                    self._current_task_list.append(True)
                    task = self._queue.pop()
                    if not self._start_event.is_set():
                        self._start_event.set()
                    try:
                        task.function(*task.args, **task.kwargs)
                    finally:
                        # this should never fail
                        self._current_task_list.pop()
                except IndexError:
                    # in case we pop from an empty queue
                    self._current_task_list.pop()
        finally:
            self._exit_callback(self._id)


class SimpleTaskDispatcher(object):

    """SimpleTaskDispatcher accepts task assignments.

    The typical use case is that you have a big number of tasks that can be
    executed in parallel, but you don't want to deal with thread pool and
    task queue.

    It is very suitable for data loading applications or background data
    processing applications.

    # define a function for taskA
    def process_taskA(parameters):
        ....

    # define a function for taskB
    def process_taskB(parameters):
        ....

    # create a dispatcher that allows 5 parallel tasks
    my_dispatcher = SimpleTaskDispatcher(5)

    # initialize the dispatcher
    my_dispatcher.register_handler('taskA', process_taskA)
    my_dispatcher.register_handler('taskB', process_taskB)

    ....

    while more_tasks():
        task = next_task()
        my_dispatcher.add_task(task.type, task.parameters)

    # also you may add a task to the dispatcher without registering
    # any handlers
    my_dispatcher.add_task(process_taskB, param1, param2=param2)

    # Before the end of the application, it's a good idea to clean up
    # task queue.  There is a race condition here, you can't be certain
    # the system is empty if another thread is still adding tasks.
    my_dispatcher.wait_until_done()

    # Even if you don't care to wait until all tasks are done, you
    # should call stop_all to stop the system and clean up the threads
    # created.
    my_dispatcher.stop_all()
    """

    def __init__(self, max_num_workers=1, max_worker_idle_period=60,
                 max_queue_size=10000):
        """Constructor for SimpleTaskDispatcher.

        :Parameters:
            - `max_num_workers`: Maximum number of worker threads to
              process tasks.
            - `max_worker_idle_period`: Maximum time for a worker to
              remain idle before exiting.
            - `max_queue_size`: Maximum number of tasks which may be
              queued up for processing.
        """
        self._handlers = dict()
        self._handlers_lock = threading.RLock()

        self._executor_thread = None
        self._executor_lock = threading.RLock()

        self._workers = dict()
        self._workers_lock = threading.RLock()
        self._workers_semaphore = threading.Semaphore(max_num_workers)

        self._work_queue = collections.deque()
        self._max_worker_idle_period = max_worker_idle_period
        self._max_work_queue_size = max_queue_size

        self._tasks_finished = threading.Event()
        self._tasks_finished.set()
        self._current_task_list = list()

        self._stop_event = threading.Event()
        atexit.register(self.stop_all)

        self._start_executor_thread()

    def _start_executor_thread(self):
        """Start executor thread as a daemon.

        If such a thread was started before, try to join it.

        :Exceptions:
            - `ConcurrentDispatcherError`: If join timeout reached and
              previous executed thread is still alive.
        """
        if self._stop_event.is_set():
            return
        with self._executor_lock:
            # We don't want to instantiate several executor threads
            if self._executor_thread:
                self._executor_thread.join(10)
                if self._executor_thread.is_alive():
                    raise ConcurrentDispatcherError('Dispatcher currently '
                                                    'running, could not start')
            self._executor_thread = threading.Thread(target=self._executor,
                                                     name='ExecutorThread')
            self._executor_thread.daemon = True
            self._executor_thread.start()

    def _executor(self):
        """Manage thread pool.

        Function monitors our tasks queue and if it is not empty (i.e. none
        of the workers picked up some task(s)) then creates a new worker thread.
        """
        counter = 0
        while not self._stop_event.is_set():
            if self._tasks_finished.is_set():
                # Don't check tasks queue if no task were added.
                time.sleep(0.01)
                continue
            if self._work_queue and \
                self._workers_semaphore.acquire(blocking=False):
                with self._workers_lock:
                    start_event = threading.Event()
                    worker = _WorkerThread(counter,
                                           self._work_queue,
                                           self._stop_event,
                                           self._worker_exit,
                                           start_event,
                                           self._current_task_list,
                                           self._max_worker_idle_period)
                    self._workers[counter] = worker
                    counter += 1
                    worker.daemon = True
                    worker.start()
                    # wait until the worker thread has started processing
                    start_event.wait()
            else:
                with self._workers_lock:
                    if not self._work_queue and \
                        not self._current_task_list:
                        self._tasks_finished.set()

    def register_handler(self, task_name, task_function):
        """Register a task handler function, and map to its name.

        :Parameters:
            - `task_name`: A name used to identify the task_function.
            - `task_function`: A function that represents a task.
        """
        with self._handlers_lock:
            self._handlers[task_name] = task_function

    def lookup_handler(self, task_name):
        """Lookup a task handler function.

        :Parameters:
            - `task_name`: The name of the task. The task must already
              be registered.

        :Return:
            A registered handler function.

        :Exceptions:
            - `UnknownTaskException`: If the task_name does not map to
              a previously registered function.
        """
        with self._handlers_lock:
            try:
                return self._handlers[task_name]
            except KeyError:
                raise UnknownTaskException(
                    'Unknown task name %s, please register handler first' % (
                    task_name,))

    def add_task(self, task, *args, **kwargs):
        """Add a task to the dispatcher.

        If the task queue is full, the function will block until the task
        may be added to the queue.

        :Parameters:
            - `task`: The name of the task which has been registered before
              or just a callback function object.
            - `*args`: Parameters for the task function.
            - `**kwargs`: Keyword parameters for the task function.

        :Exceptions:
            - `UnknownTaskException`: If the task does not map to
              a previously registered function and isn't callable.
        """
        # first check if we may add any more tasks
        if self._stop_event.is_set():
            return

        task_function = self._get_task_function(task)

        while len(self._work_queue) >= self._max_work_queue_size:
            time.sleep(0.1)

        with self._workers_lock:
            self._tasks_finished.clear()
            self._work_queue.appendleft(
                Task(task_function, args, kwargs))

    def add_task_no_wait(self, task, *args, **kwargs):
        """Add a task to the dispatcher without any blocking.

        :Parameters:
            - `task`: The name of the task which has been registered before
              or just a callback function object.
            - `*args`: Parameters for the task function.
            - `**kwargs`: Keyword parameters for the task function.

        :Exceptions:
            - `UnknownTaskException`: If the task does not map to
              a previously registered function and isn't callable.
            - `TaskQueueFullException`: Raised if the tasks' queue is full.
        """
        # first check if we may add any more tasks
        if self._stop_event.is_set():
            return

        task_function = self._get_task_function(task)

        if len(self._work_queue) >= self._max_work_queue_size:
            raise TaskQueueIsFullError('Task queue is full: %s'
                                       % (len(self._work_queue),))

        with self._workers_lock:
            self._tasks_finished.clear()
            self._work_queue.appendleft(
                Task(task_function, args, kwargs))

    def _get_task_function(self, task):
        """Helper for analyzing 'task' parameter for 'add_task' function.

        :Parameters:
            - `task`: Object to be analyzed.
        :Return:
            If passed object has '__call__' method, return itself.  Otherwise
            use lookup_handler function to get function object.
        """
        if hasattr(task, '__call__'):
            return task
        elif isinstance(task, str):
            return self.lookup_handler(task)
        else:
            raise UnknownTaskException('Don\'t know how to handle task %s'
                                       % (task,))

    def _worker_exit(self, worker_id):
        """Notify the dispatcher that a thread has finished its work.

        Used as a callback function by _WorkerThread objects.

        :Parameters:
            - `worker_id`: Thread's identifier.
        """
        self._workers_semaphore.release()
        try:
            self._workers.pop(worker_id)
        except KeyError:
            pass

    def status(self):
        """Get a status of the tasks execution.

        :Return:
            - Named tuple with numbers of 'threads', 'tasks_in_queue' and
             'tasks_in_processing'.
        """
        return Status(self.num_worker_threads(),
                      self.queue_size(),
                      len(self._current_task_list))

    def queue_size(self):
        """Get the number of tasks waiting for the processing.

        :Return:
            An integer number.
        """
        return len(self._work_queue)

    def num_worker_threads(self):
        """Get the number of worker threads.

        :Return:
            An integer number.
        """
        return len(self._workers)

    def num_idle_worker_threads(self):
        """Get the number of idle worker threads.

        :Return:
            An integer number.
        """
        return max(0, self.num_worker_threads() - len(self._current_task_list))

    def wait_until_done(self, timeout=None):
        """Wait until the dispatcher has finished processing all current tasks.

        Note that you can't rely on the return value unless you prevent tasks
        from being added after wait_until_done is called.

        :Parameters:
            - `timeout`: If not None, will force call to return even if
              processes have not finished. Default is None.

        :Return:
            If timeout is not None, return False if the tasks did not
            complete in time, else return True.
        """
        start_time = time.time()
        if timeout:
            self._tasks_finished.wait(timeout)
            return time.time() - start_time < timeout
        else:
            self._tasks_finished.wait()
            return True

    def stop_all(self, timeout=10):
        """Stop the dispatcher completely.

        This is generally called implicitly at exit.  However calling
        it multiple times will not hurt.

        Parameters:
            - `timeout`: Amount of time to wait for executor/processing threads
               to join.  If a thread is processing it will likely take longer
               than the timeout value, since there are several threads which
               must join.
        """
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        if self._executor_thread:
            self._executor_thread.join(timeout)

        while self._workers:
            worker_id, thread = self._workers.popitem()
            thread.join(timeout)

    def restart(self):
        """Clean stop signal and start new workers."""
        self._stop_event.clear()
        self._start_executor_thread()


class _SimpleJob(object):

    """Class which represents execution of single tasks as one job."""

    def __init__(self):
        """Constructor for _SimpleJob."""
        self._cv = threading.Condition()
        self._results = dict()
        self._counter = 0

    def register_task(self):
        """Inform _SimpleJob about some started task.

        :Return:
            Identifier for started task.
        """
        self._cv.acquire()
        try:
            task_id = self._counter
            self._counter += 1
            return task_id
        finally:
            self._cv.release()

    def register_result(self, task_id, result):
        """Inform _SimpleJob about some finished task.

        :Parameters:
            - `task_id`: Task identifier given when registering the task.
            - `result`: Result of execution.

        :Return:
            Identifier for the started task.
        """
        self._cv.acquire()
        try:
            self._results[task_id] = result
            self._cv.notifyAll()
        finally:
            self._cv.release()

    def get_results(self, timeout=None):
        """Get results for all registered tasks.

        The function will block until all results were gathered.

        :Parameters:
            `timeout`: Maximum time to wait till all results were gathered.
            If not specified, wait until done.

        :Return:
            A dictionary with ids of tasks and their results.

        :Exceptions:
            - `TimeoutException`: If timeout value reached.
        """
        start_time = time.time()
        self._cv.acquire()
        try:
            while len(self._results) != self._counter:
                if timeout is not None and \
                   time.time() - start_time >= timeout:
                    raise TimeoutException('Execution timeout reached.')
                self._cv.wait(1)
        finally:
            self._cv.release()

        return self._results


class SimpleJobExecutor(object):

    """Class for executing and consolidating results of a group of tasks.

    SimpleTaskDispatcher is suitable class to fire up a list of tasks.
    However, it is difficult to collect the results of a group of related
    tasks.  So, SimpleJobExecutor is aimed to address this issue.  The idea
    is to provide a job wrapper on top of SimpleTaskDispatcher.  It is suitable
    for a big job that can be executed as many small tasks.

    Example::

        def task_slicer(summer_list):
            tasks = []
            slice_size = 10
            start_pos = 0
            next_pos = 0
            size = len(summer_list)
            while next_pos < size:
                start_pos = next_pos
                next_pos += slice_size
                if next_pos > size:
                    next_pos = size
                task = summer_list[start_pos:next_pos]
                tasks.append(((task,), {}))
            return tasks

        def task_executor(summer_elem):
            return reduce(operator.add, summer_elem)

        def results_consolidator(d):
            return reduce(operator.add, d.values())


        super_summer = SimpleJobExecutor(
                SimpleTaskDispatcher(8),
                task_executor,
                task_slicer,
                results_consolidator)

        print super_summer(range(100000))
    """

    def __init__(self, task_dispatcher, task_executor, task_slicer,
                 results_consolidator=None, timeout=None):
        """Constructor for SimpleJobExecutor.

        :Parameters:
            - `task_dispatcher`: An instance of SimpleTaskDispatcher.
            - `task_executor`: Callback that can execute a small
              piece of the job.
            - `task_slicer`: Callback that can slice a big job into many
              small pieces/tasks.  The returned results should fit
              the parameter list of the taskExecutor.
            - `results_consolidator`: Callback to consolidator of the
              results returned by all small tasks.  If None, the default
              implementation will be used, which collects results into
              a list.
            - `timeout`: Maximum time to wait till all results were gathered.
              If not specified, wait until done.
        """
        self._task_slicer = task_slicer
        self._task_executor = task_executor
        if results_consolidator:
            self._results_consolidator = results_consolidator
        else:
            self._results_consolidator = self._default_results_consolidator

        self._task_dispatcher = task_dispatcher
        self._timeout = timeout

    def __call__(self, *args, **kwargs):
        """Start execution of the job.

        :Parameters:
            - `*args`: Parameters for the job.  Will be processed
              by task_slicer.
            - `**kwargs`: Keyword parameters for the job.  Will be processed
              by task_slicer.

        :Return:
            Result of the execution, processed by results_consolidator.
        """
        job = _SimpleJob()
        task_list = self._task_slicer(*args, **kwargs)
        for task_args, task_kwargs in task_list:
            task_id = job.register_task()
            self._task_dispatcher.add_task(self._execute_task,
                                           job, task_id,
                                           task_args, task_kwargs)
        return self._results_consolidator(job.get_results(self._timeout))

    def _execute_task(self, job, task_id, task_args, task_kwargs):
        """Executor for a single task.

        :Parameters:
            - `job`: _SimpleJob object.
            - `task_id`: An identifier got while registering the task.
            - `task_args`: Arguments for the task.
            - `task_kwargs`: Keyword arguments for the task.
        """
        result = self._task_executor(*task_args, **task_kwargs)
        job.register_result(task_id, result)

    def _default_results_consolidator(self, results):
        """Default results consolidator.

        :Parameters:
            - `results`: A dictionary with execution results.

        :Return:
            Execution results as a list.
        """
        retval = list()
        for key in sorted(results.keys()):
            retval.append(results[key])
        return retval

def timeout_func(func, args=(), kwargs={}, timeout=1):
    """ Timeout a Python function call.

    WARNING: This function doesn't actually kill a thread after a timeout,
    it leaves it running until it finishes normally.
    :param func: function to call
    :param args: *args parameters to the function
    :param kwargs: **kwargs parameters to the function
    :param timeout: execution timeout in seconds (float)
    :return: function's output
    :raise TimeoutException: if execution timeout is reached
    """
    class InterruptableThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result = None
            self.exc = None

        def run(self):
            try:
                self.result = func(*args, **kwargs)
            except Exception:
                self.save_exception()

        def save_exception(self):
            exc_info = sys.exc_info()
            self.exc = {'value': exc_info[1],
                        'traceback': exc_info[2]}

    it = InterruptableThread()
    it.start()
    it.join(timeout)
    if it.isAlive():
        raise TimeoutException('Execution timeout is reached.')
    elif it.exc:
        raise it.exc['value'], None, it.exc['traceback']
    else:
        return it.result
