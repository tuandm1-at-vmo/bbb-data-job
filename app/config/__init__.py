from datetime import datetime
import logging
import os
import threading

from .mongo import config as MongoConfig
from .mssql import config as MssqlConfig


today = datetime.today().strftime('%Y-%m-%d')

logging.basicConfig(filename=os.path.join('logs', f'{today}.log'),
                    filemode='a',
                    format='%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)


def get_logger(package: str):
    ''' Returns the logger for a specific package. '''
    current_thread_id = threading.current_thread().ident
    return logging.getLogger(f'{package}<{current_thread_id}>')
