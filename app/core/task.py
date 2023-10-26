'''
Contains all classes about tasking and executing.
'''

from concurrent import futures
import threading
from typing import Any, Callable, List, Optional, Type, TypeVar
from typing_extensions import Self

from .common import PrettyGoodInstance


R = TypeVar('R')


class _Task(PrettyGoodInstance):
    ''' A simple task. '''

    def __init__(self, execution: Callable, *args, **kwargs):
        self._execution = execution
        self._args = args
        self._kwargs = kwargs
        self._self_validate()

    def _self_validate(self):
        if self._execution is None:
            raise RuntimeError('Argument `execution` could not be null')

    def __str__(self):
        return f'(task){self._execution}[{self._args}][{self._kwargs}]'

    def run(self):
        ''' Executes the associated function with the already-assigned parameters. '''
        return self._execution(*self._args, **self._kwargs)


class Runner(PrettyGoodInstance):
    ''' A simple runner. '''

    def __init__(self, name: str, debug: bool = False):
        '''
        Creates a runner with a specific name.

        Parameters:
            name (str): to specify the name of the runner
            debug (bool): to toggle the DEBUG-level for logging (default: False)
        '''
        self._name = name
        self._self_validate()
        self._enable_logger(debug=debug)
        self._post_init()

    def _self_validate(self):
        if self._name is None or len(self._name) == 0:
            raise RuntimeError('Argument `name` could not be null or empty')

    def _post_init(self):
        ''' Logs stuff after the initialization. '''
        self._logger.debug('Initialized %s \'%s\'', self.__class__.__name__, self.name())

    def run(self, task: _Task):
        ''' Runs a specific task with its given arguments. '''
        try:
            self._logger.debug('Started running task %s', task)
            return task.run()
        finally:
            self._logger.debug('Finished task %s', task)


class SyncRunner(Runner):
    ''' Yet another runner, but it supports synchronization. '''
    _lock: threading.Lock

    def __init__(self, name: str, debug: bool = False):
        self._lock = threading.Lock()
        super().__init__(name=name, debug=debug)

    def run(self, task: _Task):
        err: Optional[Exception] = None
        self._logger.debug('Acquiring lock for task %s', task)
        with self._lock:
            self._logger.debug('Acquired lock for task %s', task)
            try:
                return Runner.run(self, task)
            except Exception as ex:
                err = ex
        self._logger.debug('Released lock from task %s', task)
        if err is not None: raise err

    def sync(self, func: Callable):
        '''
        Marks a function as synchronizable.

        Usage:
        ```
        runner = SyncRunner(...)

        @runner.sync
        def sync_func(param):
            ...
        ```
        '''
        self._logger.debug('Marked function %s as synchronizable', func)
        def _wrapper(*args, **kwargs):
            return self.run(_Task(func, *args, **kwargs))
        return _wrapper


class SequentialExecution:
    ''' A preparation stage of sequential execution. '''
    _task: Optional[Callable]
    _result_ignored: bool = False
    _executed: bool = False
    _lock: threading.Lock

    def __init__(self, task: Optional[Callable], *args, **kwargs):
        def _constructor(task: Optional[Callable], *args, **kwargs):
            return SequentialExecution(task, *args, **kwargs)
        self._task = None
        self._executed = False
        self._result_ignored = False
        self._lock = threading.Lock()
        self._new_execution = _constructor
        if task is not None:
            self.run(task, *args, **kwargs)

    def get(self, type: Type[R] = Any) -> R: # NOSONAR
        ''' Executes the associated task to retrieve its result. '''
        with self._lock:
            if self._task is None:
                raise RuntimeError('No associated task')
            if self._executed:
                raise RuntimeError('Task has been already executed')
            return self._task()

    def run(self, execution: Callable, *args, **kwargs):
        ''' Prepares to execute a task. '''
        with self._lock:
            if self._task is not None:
                raise RuntimeError('Task existed')
            def _execute():
                try:
                    task = _Task(execution, *args, **kwargs)
                    return task.run()
                finally:
                    self._executed = True
            self._task = _execute
            return self

    def execute(self):
        ''' Executes the associated task. '''
        self.get()

    def ignore_result(self):
        ''' Ignores the result of the associated task. '''
        with self._lock:
            self._result_ignored = True
            return self

    def then(self, execution: Callable, *args, **kwargs) -> Self:
        '''
        Prepares to execute a task after the associated task.
        The result after executing the associated task will be passed into this task
        unless we call `ignore_result` before this function.
        '''
        def _get():
            return execution(self.get(), *args, **kwargs)
        def _execute():
            self.execute()
            return execution(*args, **kwargs)
        return self._new_execution(_execute if self._result_ignored else _get)


class ParallelExecution(SequentialExecution):
    ''' A preparation stage of parallel execution. '''
    _tasks: List[Callable]
    _capacity: int

    def __init__(self, capacity: int, task: Optional[Callable], *args, **kwargs):
        super().__init__(task, *args, **kwargs)
        def _constructor(task: Optional[Callable], *args, **kwargs):
            return ParallelExecution(self._capacity, task, *args, **kwargs)
        self._new_execution = _constructor
        self._tasks = []
        self._capacity = capacity

    def get(self, type = Any):
        with self._lock:
            if not self._executed:
                with futures.ThreadPoolExecutor(max_workers=self._capacity) as executor:
                    execution_futures: List[futures.Future] = []
                    for task in self._tasks:
                        execution_futures.append(executor.submit(task)) # type: ignore
                    try:
                        futures.wait(execution_futures)
                        return tuple([future.result() for future in execution_futures])
                    finally:
                        self._executed = True

    def run(self, execution: Callable, *args, **kwargs):
        with self._lock:
            def _execute():
                return execution(*args, **kwargs)
            self._tasks.append(_execute)
            return self


class FutureExecutor:
    ''' An executor implementation that is inspired from the Java Stream APIs. '''

    @staticmethod
    def run(execution: Callable, *args, **kwargs):
        ''' Prepares to execute a task in the future. '''
        return SequentialExecution(execution, *args, **kwargs)

    @staticmethod
    def parallel(capacity: int = 10):
        ''' Prepares to execute several tasks concurrently. '''
        return ParallelExecution(capacity=capacity, task=None)