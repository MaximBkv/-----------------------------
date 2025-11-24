#!/usr/bin/env python3
import json
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

bootstrap_servers = ['kafka:29092']

def load_jsonl_to_kafka(file_path, topic, key_field=None):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    
    print(f"Загрузка файла {file_path} в топик {topic}...")
    
    try:
        total = 0
        success = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    message = json.loads(line.strip())
                    key = message.get(key_field) if key_field else None
                    
                    future = producer.send(topic, key=key, value=message)
                    record_metadata = future.get(timeout=10)
                    
                    total += 1
                    success += 1
                    
                    if total % 100 == 0:
                        print(f"Отправлено {total} сообщений...")
                    
                except Exception as e:
                    print(f"Ошибка строка {line_num}: {e}")
        
        producer.flush()
        print(f"Загрузка завершена: {total} всего, {success} успешно")
        return success
        
    finally:
        producer.close()

def main():
    files = [
        ('/opt/airflow/data/samples/browser_events.jsonl', 'browser_events', 'event_id'),
        ('/opt/airflow/data/samples/device_events.jsonl', 'device_events', 'click_id'),
        ('/opt/airflow/data/samples/geo_events.jsonl', 'geo_events', 'click_id'),
        ('/opt/airflow/data/samples/location_events.jsonl', 'location_events', 'event_id')
    ]
    
    print("Загрузка данных в Kafka...")
    
    for file_path, topic, key_field in files:
        count = load_jsonl_to_kafka(file_path, topic, key_field)
        print(f"{topic}: {count} записей")
    
    print("Загрузка завершена!")

if __name__ == "__main__":
    main()
