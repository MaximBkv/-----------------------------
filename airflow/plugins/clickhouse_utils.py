import logging
from typing import List, Dict, Any
from clickhouse_driver import Client
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """Менеджер подключений к ClickHouse"""
    
    def __init__(self, host: str = 'clickhouse', port: int = 9000, 
                 database: str = 'analytics', user: str = 'default', 
                 password: str = 'clickhouse'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client = None
    
    def connect(self):
        """Установка соединения с ClickHouse"""
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                settings={'use_numpy': False}
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}/{self.database}")
            return self.client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def disconnect(self):
        """Закрытие соединения"""
        if self.client:
            self.client.disconnect()
            logger.info("Disconnected from ClickHouse")
    
    @contextmanager
    def get_connection(self):
        """Context manager для работы с соединением"""
        try:
            self.connect()
            yield self.client
        finally:
            self.disconnect()


def insert_batch(table: str, data: List[Dict[str, Any]], 
                 connection: ClickHouseConnection = None):
    """Вставка батча данных в таблицу ClickHouse"""
    if connection is None:
        connection = ClickHouseConnection()
    
    with connection.get_connection() as client:
        if not data:
            logger.warning(f"No data to insert into {table}")
            return 0
        
        columns = list(data[0].keys())
        values = [[row.get(col) for col in columns] for row in data]
        
        try:
            client.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
                values
            )
            logger.info(f"Inserted {len(data)} rows into {table}")
            return len(data)
        except Exception as e:
            logger.error(f"Failed to insert data into {table}: {e}")
            raise


def execute_query(query: str, params: Dict = None, 
                  connection: ClickHouseConnection = None):
    """Выполнение SQL запроса"""
    if connection is None:
        connection = ClickHouseConnection()
    
    with connection.get_connection() as client:
        try:
            result = client.execute(query, params or {})
            logger.info(f"Query executed successfully: {query[:100]}...")
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise


def get_table_count(table: str, connection: ClickHouseConnection = None):
    """Получение количества записей в таблице"""
    query = f"SELECT count() FROM {table}"
    result = execute_query(query, connection=connection)
    return result[0][0] if result else 0


def optimize_table(table: str, connection: ClickHouseConnection = None):
    """Оптимизация таблицы (принудительное слияние партиций)"""
    query = f"OPTIMIZE TABLE {table} FINAL"
    execute_query(query, connection=connection)
    logger.info(f"Table {table} optimized")


def check_duplicates(table: str, key_columns: List[str], 
                     connection: ClickHouseConnection = None):
    """Проверка наличия дубликатов по ключевым колонкам"""
    key_cols = ', '.join(key_columns)
    query = f"""
        SELECT {key_cols}, count() as cnt
        FROM {table}
        GROUP BY {key_cols}
        HAVING cnt > 1
        LIMIT 10
    """
    result = execute_query(query, connection=connection)
    
    if result:
        logger.warning(f"Found {len(result)} duplicate groups in {table}")
        return result
    else:
        logger.info(f"No duplicates found in {table}")
        return []


def deduplicate_table(table: str, key_columns: List[str], 
                      connection: ClickHouseConnection = None):
    """Дедупликация таблицы через CREATE TABLE AS SELECT"""
    if connection is None:
        connection = ClickHouseConnection()
    
    with connection.get_connection() as client:
        temp_table = f"{table}_temp"
        key_cols = ', '.join(key_columns)
        
        try:
            # Создаем временную таблицу с уникальными записями
            query = f"""
                CREATE TABLE {temp_table} AS {table}
            """
            client.execute(query)
            
            query = f"""
                INSERT INTO {temp_table}
                SELECT * FROM {table}
                GROUP BY {key_cols}
            """
            client.execute(query)
            
            # Заменяем оригинальную таблицу
            client.execute(f"DROP TABLE {table}")
            client.execute(f"RENAME TABLE {temp_table} TO {table}")
            
            logger.info(f"Table {table} deduplicated successfully")
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            client.execute(f"DROP TABLE IF EXISTS {temp_table}")
            raise


def get_latest_timestamp(table: str, timestamp_column: str, 
                        connection: ClickHouseConnection = None):
    """Получение последней временной метки из таблицы"""
    query = f"SELECT max({timestamp_column}) FROM {table}"
    result = execute_query(query, connection=connection)
    return result[0][0] if result and result[0][0] else None
