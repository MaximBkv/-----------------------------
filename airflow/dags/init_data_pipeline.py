from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from kafka_producer import load_jsonl_to_kafka

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'init_data_pipeline',
    default_args=default_args,
    description='Инициализация: загрузка сэмпл-данных в Kafka',
    schedule_interval=None,
    catchup=False,
    tags=['initialization', 'kafka', 'data-load'],
)


def create_kafka_topics():
    """Создание топиков Kafka"""
    import subprocess
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'bash', '/scripts/kafka-init.sh'],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"Kafka topics created: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create Kafka topics: {e.stderr}")
        raise


def load_browser_events():
    """Загрузка browser events в Kafka"""
    result = load_jsonl_to_kafka(
        '/opt/airflow/data/samples/browser_events.jsonl',
        'browser_events',
        key_field='event_id'
    )
    return result


def load_device_events():
    """Загрузка device events в Kafka"""
    result = load_jsonl_to_kafka(
        '/opt/airflow/data/samples/device_events.jsonl',
        'device_events',
        key_field='click_id'
    )
    return result


def load_geo_events():
    """Загрузка geo events в Kafka"""
    result = load_jsonl_to_kafka(
        '/opt/airflow/data/samples/geo_events.jsonl',
        'geo_events',
        key_field='click_id'
    )
    return result


def load_location_events():
    """Загрузка location events в Kafka"""
    result = load_jsonl_to_kafka(
        '/opt/airflow/data/samples/location_events.jsonl',
        'location_events',
        key_field='event_id'
    )
    return result


def verify_data_load(**context):
    """Проверка успешности загрузки данных"""
    import logging
    
    logger = logging.getLogger(__name__)
    ti = context['ti']
    
    results = {
        'browser': ti.xcom_pull(task_ids='load_browser_events'),
        'device': ti.xcom_pull(task_ids='load_device_events'),
        'geo': ti.xcom_pull(task_ids='load_geo_events'),
        'location': ti.xcom_pull(task_ids='load_location_events'),
    }
    
    total_loaded = sum(r['success'] for r in results.values())
    total_failed = sum(r['failed'] for r in results.values())
    
    logger.info(f"Data load summary: {total_loaded} successful, {total_failed} failed")
    
    for name, result in results.items():
        logger.info(f"{name}: {result}")
    
    if total_failed > 0:
        logger.warning(f"Some records failed to load: {total_failed}")


# Tasks
create_topics = PythonOperator(
    task_id='create_kafka_topics',
    python_callable=create_kafka_topics,
    dag=dag,
)

load_browser = PythonOperator(
    task_id='load_browser_events',
    python_callable=load_browser_events,
    dag=dag,
)

load_device = PythonOperator(
    task_id='load_device_events',
    python_callable=load_device_events,
    dag=dag,
)

load_geo = PythonOperator(
    task_id='load_geo_events',
    python_callable=load_geo_events,
    dag=dag,
)

load_location = PythonOperator(
    task_id='load_location_events',
    python_callable=load_location_events,
    dag=dag,
)

verify = PythonOperator(
    task_id='verify_data_load',
    python_callable=verify_data_load,
    provide_context=True,
    dag=dag,
)

# Dependencies
create_topics >> [load_browser, load_device, load_geo, load_location] >> verify
