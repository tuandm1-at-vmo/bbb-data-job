'''
This job is for automatically synchronizing data from our MongoDB databases into our SQL Server databases.
'''

from app.core.task import FutureExecutor
from app.db.mongo import MongoPool
from app.db.mssql import MssqlPool
from app.config import MongoConfig, MssqlConfig
from app.config import get_logger

from . import transfer_bicycles, transfer_components, transfer_images


if __name__ == '__main__':
    logger = get_logger(__package__)
    with \
        MssqlPool(name='sql', config=MssqlConfig, capacity=20, debug=True) as sql, \
        MongoPool(name='mongo', config=MongoConfig, capacity=10, debug=True) as mongo:
            try:
                FutureExecutor.parallel() \
                    .run(transfer_bicycles.start, sql, mongo) \
                    .run(transfer_components.start, sql, mongo) \
                    .run(transfer_images.start, sql, mongo) \
                    .execute()
                logger.debug('Job finished')
            except Exception as err:
                logger.error(err, exc_info=True)