import os

from ..db.mssql import MssqlConfig


config: MssqlConfig
''' SQL Server configurations loaded from existing environment variables. '''

config = MssqlConfig(
    host=os.getenv('MSSQL_HOST') or '127.0.0.1',
    port=int(os.getenv('MSSQL_PORT') or 1433),
    user=os.getenv('MSSQL_USER') or 'admin',
    password=os.getenv('MSSQL_PASSWORD') or '',
    database=os.getenv('MSSQL_DATABASE') or '',
)