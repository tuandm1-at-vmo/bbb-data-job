'''
Contains classes about connections and pooling mechanisms.
'''

from concurrent import futures
import threading
import time
from typing import Any, Callable, List, Optional, Set, Type, TypeVar

from .common import NotImplementedYet, PrettyGoodInstance


class Connection(PrettyGoodInstance):
    ''' An abstract class for any connection. '''
    _running: bool = False

    def __enter__(self): # type: ignore
        pass

    def __exit__(self, type, value, traceback): # type: ignore
        pass

    def is_running(self):
        ''' Checks if the connection is running or not. '''
        return self._running

    def connect(self):
        ''' Tries to connect with pre-defined configurations. '''
        pass

    def close(self):
        ''' Tries to close the connection. '''
        pass

    def execute(self, *args, **kwargs) -> Any:
        raise NotImplementedYet()

    def fetch(self, *args, **kwargs) -> Any:
        raise NotImplementedYet()

    def insert(self, *args, **kwargs) -> Any:
        raise NotImplementedYet()

    def update(self, *args, **kwargs) -> Any:
        raise NotImplementedYet()

    def delete(self, *args, **kwargs) -> Any:
        raise NotImplementedYet()


class _PoolWrapperConnection(Connection):
    ''' A connection proxy for connection pool. '''
    _connection: Connection
    _pool_name: str

    def __init__(self, connection: Connection, pool_name: str = 'null'):
        self._connection = connection
        self._self_validate()
        self._name = connection.name()
        self._connection._running = True
        self._pool_name = pool_name

    def _self_validate(self):
        if self._connection is None:
            raise RuntimeError('Argument `connection` could not be null')

    def __str__(self) -> str:
        return str(self._connection)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def is_running(self):
        return self._connection.is_running()

    def close(self):
        ''' Returns the connection to the pool. '''
        self._connection._running = False

    def execute(self, *args, **kwargs):
        return self._connection.execute(*args, **kwargs)

    def fetch(self, *args, **kwargs):
        return self._connection.fetch(*args, **kwargs)

    def insert(self, *args, **kwargs):
        return self._connection.insert(*args, **kwargs)

    def update(self, *args, **kwargs):
        return self._connection.update(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._connection.delete(*args, **kwargs)


C = TypeVar('C', bound=Connection)


class ConnectionPool(PrettyGoodInstance):
    ''' A basic implementation for our connection pools. '''
    _capacity: int
    _connections: List[Connection]
    _initializer: Callable[[], Connection]
    _lock: threading.Lock
    _waiting_threads: Set[Optional[int]]
    _closed: bool

    def __init__(self, name: str, initializer: Callable[[], Connection], capacity: int = 5, debug: bool = False):
        '''
        Creates a connection pool with its name and capacity.

        Paramaters:
            name (str): the name of the connection pool
            initializer (Callable): to specify how the connection pool creates a new connection
            capacity (int): its capacity (default: 1)
            debug (bool): to specify whether the DEBUG-level logging is enabled or not (default: False)
        '''
        self._name = name
        self._initializer = initializer
        self._capacity = capacity
        self._lock = threading.Lock()
        self._waiting_threads = set()
        self._closed = False
        self._self_validate()
        self._enable_logger(debug=debug)
        self._init_connections(debug=debug)
        self._start_polling()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _self_validate(self):
        if self._name is None or len(self._name) == 0:
            raise RuntimeError('Argument `name` could not be null or empty')
        if self._initializer is None:
            raise RuntimeError('Argument `initializer` could not be null')
        if self._capacity < 1:
            raise RuntimeError('Argument `capacity` could not less than 1')

    def _start_polling(self, interval=10):
        with self._lock:
            def _poll():
                current_thread_id = threading.current_thread().ident
                while self._closed is not True:
                    total_connections = len(self._connections)
                    total_running_connections = sum([(1 if conn.is_running() else 0) for conn in self._connections])
                    total_idle_connections = total_connections - total_running_connections
                    total_waiting_threads = len(self._waiting_threads)
                    self._logger.info('Pool stats [total=%s,running=%s,idle=%s,waiting=%s]', total_connections, total_running_connections, total_idle_connections, total_waiting_threads)
                    time.sleep(interval)
                self._logger.debug('Poller stopped [thread_id=%s,interval=%ss]', current_thread_id, interval)
            poller = threading.Thread(target=_poll)
            poller.start()
            self._logger.debug('Poller started [thread_id=%s,interval=%ss]', poller.ident, interval)

    def _init_connections(self, debug: bool = False):
        ''' Initializes all connections. '''
        self._connections = []
        def _appender(connection_index: int):
            current_thread_id = threading.current_thread().ident
            connection_name = f'{self.name()}-{connection_index}'.lower()
            try:
                connection = self._initializer()
                connection._name = connection_name
                connection._enable_logger(debug=debug)
                self._logger.debug('Opening connection %s [thread_id=%s]', connection, current_thread_id)
                connection.connect()
                self._logger.debug('Connection %s opened [thread_id=%s]', connection, current_thread_id)
                self._connections.append(connection)
            except Exception as ex:
                self._logger.error('Connection %s could not be opened [thread_id=%s]: %s', connection_name, current_thread_id, ex)
        self._logger.info('Opening connections [total=%i]', self._capacity)
        with futures.ThreadPoolExecutor(max_workers=self._capacity) as executor:
            conn_init_futures: List[futures.Future] = []
            for i in range(self._capacity):
                conn_init_futures.append(executor.submit(_appender, i))
            futures.wait(conn_init_futures)

    def restart(self):
        ''' Restarts the connection pools after closing all existing connections. '''
        self._logger.debug('Restarting connection pool %s', self)
        self.close()
        self._init_connections()

    def close(self):
        '''
        Tries to close all connections.

        Returns: a list of `Exception`s caught during the closing process.
        '''
        with self._lock:
            if self._closed: return []
            self._closed = True
        def _close(connection: Connection):
            try:
                self._logger.debug('Closing connection %s', connection)
                connection.close()
                self._logger.debug('Connection %s closed', connection)
            except Exception as ex:
                self._logger.error('Connection %s could not be closed: %s', connection, ex)
        self._logger.debug('Closing connections [total=%i]', self._capacity)
        threads: List[threading.Thread] = []
        for connection in self._connections:
            threads.append(threading.Thread(target=_close, args=(connection,)))
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self._logger.info('Connections closed [total=%i]', self._capacity)

    def get_connection(self, timeout_ms: int = 0, connection_type: Type[C] = Connection) -> C: # NOSONAR
        '''
        Tries to retrieve an idle connection from the pool.

        Parameters:
            timeout_ms (int): the time-out setting in millisecond
        '''
        current_thread_id = threading.current_thread().ident
        self._logger.debug('Waiting to obtain a connection [thread_id=%s,timeout=%ims]', current_thread_id, timeout_ms)
        if self._closed:
            raise RuntimeError(f'Pool {self.name()} already closed [thread_id={current_thread_id},timeout={timeout_ms}ms]')
        self._waiting_threads.add(current_thread_id)
        with self._lock:
            waiting_start_time = time.time_ns()
            while True:
                for connection in self._connections:
                    if not connection.is_running():
                        self._logger.debug('Connection obtained: %s [thread_id=%s]', connection, current_thread_id)
                        self._waiting_threads.remove(current_thread_id)
                        return _PoolWrapperConnection(connection=connection, pool_name=self.name()) # type: ignore
                waiting_current_time = time.time_ns()
                waiting_elapsed_time_ms = (waiting_current_time - waiting_start_time) / 1000000
                if timeout_ms > 0 and waiting_elapsed_time_ms >  timeout_ms: break
        self._waiting_threads.remove(current_thread_id)
        raise RuntimeError(f'Cannot obtain any connection from pool {self.name()} [thread_id={current_thread_id},timeout={timeout_ms}ms]')