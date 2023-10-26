from bson import ObjectId
import math
from typing import List

from app.core.task import FutureExecutor
from app.db.mongo import MongoConnection, MongoDatabase, MongoPool
from app.db.mssql import MssqlConnection, MssqlPool
from app.config import get_logger

from .entity import SEBicycle


logger = get_logger(__package__)

COLLECTION = 'se_bicycle'
TABLE = 'bbb_data_source.se_bicycle'


def count_untransfered_documents(pool: MongoPool):
    conn = pool.get_connection(timeout_ms=30000, connection_type=MongoConnection)
    with conn:
        collection = COLLECTION
        query_filter = {
            '_bbb_transfered': {
                '$ne': True,
            },
        }
        def _count(db: MongoDatabase):
            return db.get_collection(collection) \
                .count_documents(filter=query_filter)
        count = conn.execute(_count)
        logger.debug('Total untransfered documents: %s', count)
        return count


def fetch_untransfered_documents_by_page(page: int, size: int, pool: MongoPool):
    conn = pool.get_connection(timeout_ms=30000, connection_type=MongoConnection)
    with conn:
        query_filter = {
            '_bbb_transfered': {
                '$ne': True,
            },
        }
        docs = conn.fetch(collection=COLLECTION,
                          filter=query_filter,
                          skip=(page*size),
                          limit=size,
                          doc_type=SEBicycle)
        logger.debug('Total documents fetched [page=%s,size=%s]: %s', page, size, len(docs))
        return docs


def insert_records_from_untransfered_documents(docs: List[SEBicycle], pool: MssqlPool):
    if len(docs) == 0: return []
    conn = pool.get_connection(timeout_ms=30000, connection_type=MssqlConnection)
    with conn:
        for doc in docs:
            if doc.get('status') is None: doc['status'] = 'New'
            if doc.get('source') is None: doc['source'] = 'BBB'
            doc['bbb_sku'] = doc.get('sku', str)
            doc['sku'] = -1
        conn.insert(table=TABLE, entities=docs, ignore_cols=['_bbb_transfered', '_id'])
        return docs


def mark_transfered_documents(docs: List[SEBicycle], pool: MongoPool):
    if len(docs) == 0: return []
    conn = pool.get_connection(timeout_ms=30000, connection_type=MongoConnection)
    with conn:
        query_filter = {
            '_id': {
                '$in': [ObjectId(doc._id) for doc in docs],
            },
        }
        query_update = {
            '$set': {
                '_bbb_transfered': True,
            },
        }
        conn.update(collection=COLLECTION, filter=query_filter, value=query_update)
        return docs


def prepare_to_fetch_untransfered_documents(total: int, page_size: int, sql: MssqlPool, mongo: MongoPool):
    total_pages = math.ceil(total / page_size)
    if total_pages == 0: return ()
    def _task(page: int):
        return FutureExecutor.run(fetch_untransfered_documents_by_page, page, page_size, mongo) \
            .then(insert_records_from_untransfered_documents, sql) \
            .then(mark_transfered_documents, mongo) \
            .get()
    executor = FutureExecutor.parallel(capacity=total_pages)
    for i in range(total_pages):
        executor = executor.run(_task, i)
    logger.debug('Fetching untransfered documents [total_pages=%s,page_size=%s]', total_pages, page_size)
    return executor.get()


def start(sql: MssqlPool, mongo: MongoPool) -> tuple[List[SEBicycle]]:
    transfered = FutureExecutor.run(count_untransfered_documents, mongo) \
        .then(prepare_to_fetch_untransfered_documents, 100, sql, mongo) \
        .get()
    logger.debug('Total transfered documents: %s', sum([len(doc) for doc in transfered]))
    return transfered