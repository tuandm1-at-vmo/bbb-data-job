from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase

from ..core.common import Entity
from ..core.connection import Connection, ConnectionPool


class MongoConfig:
    ''' Configurations for our MongoDB databases. '''

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


T = TypeVar('T')
D = TypeVar('D', bound=Entity)


class MongoConnection(Connection):
    ''' A MongoDB connection. '''
    __db: MongoDatabase

    def __init__(self, config: MongoConfig) -> None:
        self.__conf = config
        self._self_validate()

    def _self_validate(self):
        if self.__conf is None:
            raise RuntimeError('config missing or invalid')

    def _check_db(self, and_client: bool = False):
        if (self.__db is None) or (and_client and self.__db.client is None):
            raise RuntimeError('Connection closed or invalid')

    def connect(self):
        client = MongoClient(
            host=f'mongodb://{self.__conf.user}:{self.__conf.password}@{self.__conf.host}/{self.__conf.auth_database}',
            port=self.__conf.port,
        )
        self.__db = client.get_database(name=self.__conf.database)

    def close(self):
        self._check_db(and_client=True)
        self.__db.client.close()

    def execute(self, task: Callable[[MongoDatabase], T]) -> T:
        self._check_db()
        if task is None:
            raise RuntimeError('Argument `task` could not be null')
        return task(self.__db)

    def fetch(self,
              collection: str,
              filter: Dict[str, Any] = {},
              projection: Optional[Dict[str, Any]] = None,
              skip: int = 0,
              limit: int = 20,
              sort: List[Tuple[str, int]] = [],
              doc_type: Type[Union[D, Dict[str, Any]]] = Entity) -> List[D]:
        self._check_db()
        cursor = self.__db.get_collection(name=collection).find(filter=filter, projection=projection).skip(skip).limit(limit)
        if len(sort) > 0:
            cursor = cursor.sort(sort)
        self._logger.debug('Fetching [collection=%s,filter=%s,skip=%s,limit=%s]', collection, filter, skip, limit)
        return [doc_type(doc) for doc in cursor] # type: ignore

    def update(self,
               collection: str,
               filter: Dict[str, Any] = {},
               value: Dict[str, Any] = {}):
        self._check_db()
        self._logger.debug('Updating [collection=%s,filter=%s]', collection, filter)
        self.__db.get_collection(name=collection).update_many(filter=filter, update=value)

    def insert(self,
               collection: str,
               docs: Union[List[D], List[Dict[str, Any]]] = []):
        self._check_db()
        self._logger.debug('Inserting documents [collection=%s,total=%s]', collection, len(docs))
        self.__db.get_collection(collection).insert_many(documents=docs)


class MongoPool(ConnectionPool):
    ''' A MongoDB connection pool. '''

    def __init__(self, name: str, config: MongoConfig, capacity: int = 5, debug: bool = False):
        def _create_connection():
            return MongoConnection(config=config)
        super().__init__(name=name, initializer=_create_connection, capacity=capacity, debug=debug)