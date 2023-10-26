from typing import Any, Callable, List, Optional, Tuple, Type, TypeVar
import pymssql
import re

from ..core.common import Entity
from ..core.connection import Connection, ConnectionPool


class MssqlConfig:
    ''' Configurations for our MsSQL databases. '''

    host: str
    port: int
    user: str
    password: str
    database: str
    auth_database: str

    def __init__(self, host: str, port: int, user: str, password: str, database: str, auth_database: Optional[str] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.auth_database = auth_database if auth_database is not None else database


class CursorType:
    def execute(self, statement: str, *params): # type: ignore
        pass

    def executemany(self, statement: str, *params): # type: ignore
        pass

    def fetchall(self) -> List[Any]: # type: ignore
        pass


R = TypeVar('R', bound=Entity)


class MssqlConnection(Connection):
    ''' An MsSQL connection. '''

    def __init__(self, config: MssqlConfig) -> None:
        self.__conf = config
        self._self_validate()

    def _self_validate(self):
        if self.__conf is None:
            raise RuntimeError('config missing or invalid')

    def _check_connection(self):
        if self.__conn is None:
            raise RuntimeError('Connection closed or invalid')

    def connect(self):
        self.__conn = pymssql.connect( # type: ignore
            host=self.__conf.host,
            port=self.__conf.port,
            user=self.__conf.user,
            password=self.__conf.password,
            database=self.__conf.database,
        )

    def close(self):
        self._check_connection()
        self.__conn.close()

    def execute(self, task: Callable[[CursorType], R]) -> R:
        self._check_connection()
        if task is None:
            raise RuntimeError('Argument `task` could not be null')
        with self.__conn.cursor() as cursor:
            try:
                return task(cursor)
            finally:
                self.__conn.commit()

    def fetch(self, statement: str, params: tuple = (), rec_type: Type[R] = Entity) -> List[R]:
        self._check_connection()
        with self.__conn.cursor() as cursor:
            try:
                cursor.execute(statement, params)
                return [rec_type.load_from_tuple(*rec) for rec in cursor.fetchall()]
            finally:
                self.__conn.commit()

    def insert(self, table: str, entities: List[R] = [], columns: Optional[Tuple[str]] = None, ignore_cols: List[str] = []):
        if len(entities) == 0:
            raise RuntimeError('Argument `entities` could not be empty')
        self._check_connection()
        with self.__conn.cursor() as cursor:
            cols = columns if columns is not None else entities[0].columns(ignore_cols, wrap_in_box_brackets=True)
            placeholder = f'({", ".join(["%s" for _ in cols])})'
            statement = f'''
                INSERT INTO {table} ({", ".join(cols)})
                VALUES {placeholder}
            '''
            params = []
            for entity in entities:
                params.append(entity.values(columns=cols))
            self._logger.debug('Executing [total_cols=%s,total_recs=%s]: %s', len(cols), len(entities), re.sub('\\s+', ' ', statement).strip())
            try:
                cursor.executemany(statement, params)
                self.__conn.commit()
            except Exception as ex:
                self.__conn.rollback()
                raise ex


class MssqlPool(ConnectionPool):
    ''' An MsSQL connection pool. '''

    def __init__(self, name: str, config: MssqlConfig, capacity: int = 5, debug: bool = False):
        def _create_connection():
            return MssqlConnection(config=config)
        super().__init__(name=name, initializer=_create_connection, capacity=capacity, debug=debug)