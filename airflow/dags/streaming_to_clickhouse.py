from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from clickhouse_utils import insert_batch, ClickHouseConnection

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'streaming_to_clickhouse',
    default_args=default_args,
    description='Потоковая загрузка данных из Kafka в ClickHouse staging слой',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['streaming', 'kafka', 'clickhouse', 'etl'],
)


def consume_and_load(topic: str, table: str, batch_size: int = 1000, 
                     timeout_ms: int = 30000):
    """
    Чтение данных из Kafka топика и загрузка в ClickHouse
    
    Args:
        topic: имя Kafka топика
        table: имя таблицы ClickHouse
        batch_size: размер батча для вставки
        timeout_ms: таймаут чтения из Kafka
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'{topic}_consumer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=timeout_ms
    )
    
    batch = []
    total_consumed = 0
    total_inserted = 0
    
    try:
        for message in consumer:
            batch.append(message.value)
            total_consumed += 1
            
            if len(batch) >= batch_size:
                inserted = insert_batch(f'analytics.{table}', batch)
                total_inserted += inserted
                logger.info(f"Inserted {inserted} records from {topic} to {table}")
                batch = []
        
        # Вставка остатка
        if batch:
            inserted = insert_batch(f'analytics.{table}', batch)
            total_inserted += inserted
            logger.info(f"Inserted remaining {inserted} records from {topic} to {table}")
        
        logger.info(
            f"Completed: consumed {total_consumed}, inserted {total_inserted} "
            f"from {topic} to {table}"
        )
        
        return {
            'topic': topic,
            'table': table,
            'consumed': total_consumed,
            'inserted': total_inserted
        }
    
    finally:
        consumer.close()


def consume_browser_events():
    """Потребление browser events"""
    return consume_and_load('browser_events', 'events_browser_raw')


def consume_device_events():
    """Потребление device events"""
    return consume_and_load('device_events', 'events_device_raw')


def consume_geo_events():
    """Потребление geo events"""
    return consume_and_load('geo_events', 'events_geo_raw')


def consume_location_events():
    """Потребление location events"""
    return consume_and_load('location_events', 'events_location_raw')


def validate_ingestion(**context):
    """Валидация успешности загрузки данных"""
    ti = context['ti']
    
    results = {
        'browser': ti.xcom_pull(task_ids='consume_browser_events'),
        'device': ti.xcom_pull(task_ids='consume_device_events'),
        'geo': ti.xcom_pull(task_ids='consume_geo_events'),
        'location': ti.xcom_pull(task_ids='consume_location_events'),
    }
    
    total_consumed = sum(r['consumed'] for r in results.values() if r)
    total_inserted = sum(r['inserted'] for r in results.values() if r)
    
    logger.info(f"Ingestion summary: {total_consumed} consumed, {total_inserted} inserted")
    
    for name, result in results.items():
        if result:
            logger.info(f"{name}: {result}")
        else:
            logger.warning(f"{name}: no data processed")
    
    return {
        'total_consumed': total_consumed,
        'total_inserted': total_inserted,
        'results': results
    }


# Tasks
consume_browser = PythonOperator(
    task_id='consume_browser_events',
    python_callable=consume_browser_events,
    dag=dag,
)

consume_device = PythonOperator(
    task_id='consume_device_events',
    python_callable=consume_device_events,
    dag=dag,
)

consume_geo = PythonOperator(
    task_id='consume_geo_events',
    python_callable=consume_geo_events,
    dag=dag,
)

consume_location = PythonOperator(
    task_id='consume_location_events',
    python_callable=consume_location_events,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_ingestion',
    python_callable=validate_ingestion,
    provide_context=True,
    dag=dag,
)

# Dependencies - параллельное потребление из всех топиков
[consume_browser, consume_device, consume_geo, consume_location] >> validate
