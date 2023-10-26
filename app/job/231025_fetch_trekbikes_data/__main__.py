from app.core.task import FutureExecutor
from app.db.mongo import MongoPool
from app.config import MongoConfig
from app.config import get_logger
from app.util.args import parse

from . import convert_bikes_to_xmls, fetch_bikes


if __name__ == '__main__':
    logger = get_logger(__package__)
    args = parse({
        'source_collection': str,
        'target_collection': str,
        'target_year': int,
    })
    source_collection = args.get('source_collection', str)
    target_collection = args.get('target_collection', str)
    target_year = args.get('target_year', int)
    with \
        MongoPool(name='mongo', config=MongoConfig, capacity=50, debug=True) as mongo:
            try:
                logger.debug('Started fetching bikes into collection %s then converting to XMLs into collection %s [year=%s]', source_collection, target_collection, target_year)
                FutureExecutor.run(fetch_bikes.start, mongo, source_collection, target_year) \
                    .ignore_result() \
                    .then(convert_bikes_to_xmls.start, mongo, source_collection, target_collection, target_year) \
                    .execute()
                logger.debug('Job finished')
            except Exception as err:
                logger.error(err, exc_info=True)