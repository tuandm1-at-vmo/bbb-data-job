import os
from urllib.parse import quote_plus

from ..db.mongo import MongoConfig


config: MongoConfig
''' MongoDB configurations loaded from existing environment variables. '''

config = MongoConfig(
    host=os.getenv('MONGO_HOST') or '127.0.0.1',
    port=int(os.getenv('MONGO_PORT') or 27017),
    user=quote_plus(os.getenv('MONGO_USER') or 'root'),
    password=quote_plus(os.getenv('MONGO_PASSWORD') or ''),
    database=os.getenv('MONGO_DATABASE') or '',
)
