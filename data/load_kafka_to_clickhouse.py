#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer
from clickhouse_driver import Client

# Конфигурация
kafka_bootstrap_servers = ['kafka:29092']
clickhouse_client = Client('clickhouse', host='clickhouse', port=9000, 
                          database='analytics', user='default', password='clickhouse')

def consume_and_load(topic, table):
    print(f"Обработка топика {topic} в таблицу {table}...")
    
    # Consumer конфигурация
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'{topic}_consumer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000  # 30 секунд
    )
    
    batch = []
    total_consumed = 0
    
    try:
        # Читаем доступные сообщения
        for message in consumer:
            batch.append(message.value)
            total_consumed += 1
            
            # Батч размером 100 записей
            if len(batch) >= 100:
                insert_batch(table, batch)
                print(f"Вставлено {len(batch)} записей в {table}")
                batch = []
        
        # Вставка остатка
        if batch:
            insert_batch(table, batch)
            print(f"Вставлено {len(batch)} финальных записей в {table}")
        
        print(f"{topic}: обработано {total_consumed} записей")
        return total_consumed
        
    finally:
        consumer.close()

def insert_batch(table, data):
    """Вставка батча в ClickHouse"""
    if not data:
        return 0
    
    # Определение колонок на основе таблицы
    columns = list(data[0].keys())
    values = [[row.get(col) for col in columns] for row in data]
    
    # Добавляем timestamp вставки
    columns.append('inserted_at')
    for row in values:
        row.append(time.strftime('%Y-%m-%d %H:%M:%S'))
    
    try:
        query = f"INSERT INTO.analytics.{table} ({', '.join(columns)}) VALUES"
        clickhouse_client.execute(query, values)
        return len(values)
    except Exception as e:
        print(f"Ошибка вставки в {table}: {e}")
        return 0

def main():
    print("Начало загрузки данных из Kafka в ClickHouse...")
    
    # Очищаем таблицы
    print("Очистка staging таблиц...")
    clickhouse_client.execute("TRUNCATE TABLE analytics.events_browser_raw")
    clickhouse_client.execute("TRUNCATE TABLE analytics.events_device_raw")
    clickhouse_client.execute("TRUNCATE TABLE analytics.events_geo_raw")
    clickhouse_client.execute("TRUNCATE TABLE analytics.events_location_raw")
    
    # Загрузка данных
    topics = [
        ('browser_events', 'events_browser_raw'),
        ('device_events', 'events_device_raw'),
        ('geo_events', 'events_geo_raw'),
        ('location_events', 'events_location_raw')
    ]
    
    total_loaded = 0
    for topic, table in topics:
        count = consume_and_load(topic, table)
        total_loaded += count
    
    print(f"\nВсего загружено: {total_loaded} записей")
    
    # Проверяем количество записей в staged таблицах
    print("\nПроверка загруженных данных:")
    for table in ['events_browser_raw', 'events_device_raw', 'events_geo_raw', 'events_location_raw']:
        count = clickhouse_client.execute(f"SELECT count() FROM analytics.{table}")[0][0]
        print(f"{table}: {count} записей")
    
    print("Загрузка завершена!")

if __name__ == "__main__":
    main()
