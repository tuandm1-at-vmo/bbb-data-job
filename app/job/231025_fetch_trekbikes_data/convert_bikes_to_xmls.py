from datetime import datetime
import math
from typing import List

from app.config import get_logger
from app.core.task import FutureExecutor
from app.db.mongo import MongoConnection, MongoDatabase, MongoPool

from .entity import TrekBike, TrekXML
from .smartetailing import SmartEtailingService


logger = get_logger(__package__)


def count_bikes(mongo: MongoPool, source_collection: str, year: int):
    conn = mongo.get_connection(connection_type=MongoConnection)
    with conn:
        filter_query = {
            'year': year,
            'deleted': {
                '$ne': True,
            },
            'error': None,
        }
        def _count(db: MongoDatabase):
            return db.get_collection(source_collection) \
                .count_documents(filter=filter_query)
        total = conn.execute(_count)
        logger.debug('Total bicycles fetched: %s', total)
        return total


def fetch_bikes_on_page(page_number: int, page_size: int, mongo: MongoPool, source_collection: str, year: int):
    conn = mongo.get_connection(connection_type=MongoConnection)
    with conn:
        filter_query = {
            'year': year,
            'deleted': {
                '$ne': True,
            },
            'error': None,
        }
        projection = {
            'id': 1,
            'data': 1,
        }
        return conn.fetch(collection=source_collection,
                          filter=filter_query,
                          projection=projection,
                          skip=(page_number*page_size),
                          limit=page_size,
                          doc_type=TrekBike)


def convert_to_xml(bikes: List[TrekBike], order: int, mongo: MongoPool, target_collection: str, year: int):
    if len(bikes) == 0: return []
    conn = mongo.get_connection(connection_type=MongoConnection)
    with conn:
        filter_query = {
            'year': year,
            'deleted': {
                '$ne': True,
            },
        }
        new_value = {
            '$set': {
                'deleted': True,
                'deletedAt': datetime.now(),
            }
        }
        conn.update(collection=target_collection, filter=filter_query, value=new_value)
        ids = [bike.get('id', int) for bike in bikes]
        docs = [TrekXML({
            'year': year,
            'order': order,
            'includes': ids,
            'total': len(ids),
            'content': SmartEtailingService.create_xml(bikes=bikes),
            'createdAt': datetime.now(),
        })]
        conn.insert(collection=target_collection, docs=docs)
        return ids


def fetch_bikes_then_convert(total_documents: int, page_size: int, mongo: MongoPool, source_collection: str, target_collection: str, year: int):
    total_pages = math.ceil(total_documents / page_size)
    if total_pages <= 0: return []
    executor = FutureExecutor.parallel(capacity=total_pages)
    for i in range(total_pages):
        def convert(page_number):
            return FutureExecutor.run(fetch_bikes_on_page, page_number, page_size, mongo, source_collection, year) \
                .then(convert_to_xml, page_number, mongo, target_collection, year) \
                .get()
        executor = executor.run(convert, i)
    return executor.get()


def start(mongo: MongoPool, source_collection: str, target_collection: str, year: int, page_size: int = 50):
    converted = FutureExecutor.run(count_bikes, mongo, source_collection, year) \
        .then(fetch_bikes_then_convert, page_size, mongo, source_collection, target_collection, year) \
        .get()
    logger.debug('Total bicycles converted: %s', sum([len(docs) for docs in converted]))