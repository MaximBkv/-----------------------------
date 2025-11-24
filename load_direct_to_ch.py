#!/usr/bin/env python3
import json
import time
from clickhouse_driver import Client

# Конфигурация ClickHouse
clickhouse_client = Client(host='clickhouse', port=9000, database='analytics', user='default', password='clickhouse')

def load_jsonl_to_clickhouse(file_path, table):
    print(f"Загрузка {file_path} в {table}...")
    
    # Очистка таблицы
    clickhouse_client.execute(f"TRUNCATE TABLE analytics.{table}")
    
    batch = []
    total = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line.strip())
                data['inserted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                
                # Конвертация в список для ClickHouse
                batch.append(list(data.values()))
                
                # Батч вставка каждые 100 записей
                if len(batch) >= 100:
                    columns = ",".join(data.keys())
                    clickhouse_client.execute(f"INSERT INTO.analytics.{table} ({columns},inserted_at) VALUES", batch)
                    print(f"  Вставлено {len(batch)} записей")
                    batch = []
                    total += len(batch)
                
            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга строка {line_num}: {e}")
        
        # Вставка остатка
        if batch:
            columns = ",".join(data.keys())
            clickhouse_client.execute(f"INSERT INTO.analytics.{table} ({columns},inserted_at) VALUES", batch)
            total += len(batch)
            print(f"  Вставлено финальных {len(batch)} записей")
    
    print(f"Загружено {total} записей")
    return total

def main():
    print("Прямая загрузка данных в ClickHouse...")
    
    # Файлы и таблицы
    files = [
        ('/opt/airflow/data/samples/browser_events.jsonl', 'events_browser_raw'),
        ('/opt/airflow/data/samples/device_events.jsonl', 'events_device_raw'),
        ('/opt/airflow/data/samples/geo_events.jsonl', 'events_geo_raw'),
        ('/opt/airflow/data/samples/location_events.jsonl', 'events_location_raw')
    ]
    
    total_loaded = 0
    for file_path, table in files:
        count = load_jsonl_to_clickhouse(file_path, table)
        total_loaded += count
    
    print(f"\nВсего загружено: {total_loaded} записей")
    
    # Проверка
    print("\nПроверка загруженных данных:")
    for table in ['events_browser_raw', 'events_device_raw', 'events_geo_raw', 'events_location_raw']:
        count = clickhouse_client.execute(f"SELECT count() FROM analytics.{table}")[0][0]
        print(f"  {table}: {count} записей")
    
    print("\nЗагрузка staging данных завершена!")

if __name__ == "__main__":
    main()