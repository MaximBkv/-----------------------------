from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from clickhouse_utils import execute_query, optimize_table, get_table_count

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'build_dwh_layer',
    default_args=default_args,
    description='Построение DWH слоя: витрины, справочники, агрегаты',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['dwh', 'transformation', 'clickhouse'],
)


def build_fact_events():
    """
    Построение факт-таблицы событий через JOIN staging таблиц
    """
    query = """
        INSERT INTO analytics.fact_events
        SELECT
            b.event_id,
            b.event_timestamp,
            b.event_type,
            b.click_id,
            
            -- Browser data
            b.browser_name,
            b.browser_user_agent,
            b.browser_language,
            
            -- Device data
            d.os,
            d.os_name,
            d.os_timezone,
            d.device_type,
            d.device_is_mobile,
            d.user_custom_id,
            d.user_domain_id,
            
            -- Geo data
            g.geo_latitude,
            g.geo_longitude,
            g.geo_country,
            g.geo_timezone,
            g.geo_region_name,
            g.ip_address,
            
            -- Location data
            l.page_url,
            l.page_url_path,
            l.referer_url,
            l.referer_medium,
            l.utm_medium,
            l.utm_source,
            l.utm_content,
            l.utm_campaign,
            
            now() as created_at
        FROM analytics.events_browser_raw b
        LEFT JOIN analytics.events_device_raw d ON b.click_id = d.click_id
        LEFT JOIN analytics.events_geo_raw g ON b.click_id = g.click_id
        LEFT JOIN analytics.events_location_raw l ON b.event_id = l.event_id
        WHERE b.event_id NOT IN (
            SELECT event_id FROM analytics.fact_events
        )
    """
    
    result = execute_query(query)
    count = get_table_count('analytics.fact_events')
    logger.info(f"fact_events built successfully, total records: {count}")
    return count


def update_dim_users():
    """
    Обновление справочника пользователей
    """
    query = """
        INSERT INTO analytics.dim_users
        SELECT
            user_domain_id,
            any(user_custom_id) as user_custom_id,
            min(event_timestamp) as first_seen,
            max(event_timestamp) as last_seen,
            count() as total_events,
            any(device_type) as device_type,
            any(os_name) as os_name
        FROM analytics.fact_events
        WHERE user_domain_id != ''
        GROUP BY user_domain_id
    """
    
    execute_query("TRUNCATE TABLE analytics.dim_users")
    result = execute_query(query)
    count = get_table_count('analytics.dim_users')
    logger.info(f"dim_users updated, total users: {count}")
    return count


def update_dim_geo():
    """
    Обновление справочника геолокаций
    """
    query = """
        INSERT INTO analytics.dim_geo
        SELECT
            concat(geo_country, '_', geo_region_name) as geo_id,
            geo_country,
            geo_region_name,
            any(geo_latitude) as geo_latitude,
            any(geo_longitude) as geo_longitude,
            any(geo_timezone) as geo_timezone
        FROM analytics.fact_events
        WHERE geo_country != ''
        GROUP BY geo_country, geo_region_name
    """
    
    execute_query("TRUNCATE TABLE analytics.dim_geo")
    result = execute_query(query)
    count = get_table_count('analytics.dim_geo')
    logger.info(f"dim_geo updated, total locations: {count}")
    return count


def optimize_tables():
    """
    Оптимизация таблиц для сжатия и улучшения производительности
    """
    tables = [
        'analytics.fact_events',
        'analytics.dim_users',
        'analytics.dim_geo',
        'analytics.agg_events_hourly',
        'analytics.agg_traffic_sources'
    ]
    
    for table in tables:
        try:
            optimize_table(table)
            logger.info(f"Optimized {table}")
        except Exception as e:
            logger.warning(f"Failed to optimize {table}: {e}")
    
    return len(tables)


def verify_dwh_layer(**context):
    """
    Проверка целостности DWH слоя
    """
    ti = context['ti']
    
    fact_count = ti.xcom_pull(task_ids='build_fact_events')
    users_count = ti.xcom_pull(task_ids='update_dim_users')
    geo_count = ti.xcom_pull(task_ids='update_dim_geo')
    
    # Проверка агрегатов
    hourly_count = get_table_count('analytics.agg_events_hourly')
    traffic_count = get_table_count('analytics.agg_traffic_sources')
    
    summary = {
        'fact_events': fact_count,
        'dim_users': users_count,
        'dim_geo': geo_count,
        'agg_events_hourly': hourly_count,
        'agg_traffic_sources': traffic_count
    }
    
    logger.info(f"DWH layer summary: {summary}")
    
    # Проверка наличия данных
    if fact_count == 0:
        logger.warning("fact_events table is empty")
    
    return summary


def clear_staging_old_data():
    """
    Очистка старых данных из staging таблиц (старше 7 дней)
    """
    tables = [
        'events_browser_raw',
        'events_device_raw',
        'events_geo_raw',
        'events_location_raw'
    ]
    
    total_deleted = 0
    
    for table in tables:
        try:
            query = f"""
                ALTER TABLE analytics.{table}
                DELETE WHERE inserted_at < now() - INTERVAL 7 DAY
            """
            execute_query(query)
            logger.info(f"Cleared old data from {table}")
        except Exception as e:
            logger.error(f"Failed to clear {table}: {e}")
    
    return total_deleted


# Tasks
build_fact = PythonOperator(
    task_id='build_fact_events',
    python_callable=build_fact_events,
    dag=dag,
)

update_users = PythonOperator(
    task_id='update_dim_users',
    python_callable=update_dim_users,
    dag=dag,
)

update_geo = PythonOperator(
    task_id='update_dim_geo',
    python_callable=update_dim_geo,
    dag=dag,
)

optimize = PythonOperator(
    task_id='optimize_tables',
    python_callable=optimize_tables,
    dag=dag,
)

verify = PythonOperator(
    task_id='verify_dwh_layer',
    python_callable=verify_dwh_layer,
    provide_context=True,
    dag=dag,
)

clear_staging = PythonOperator(
    task_id='clear_staging_old_data',
    python_callable=clear_staging_old_data,
    dag=dag,
)

# Dependencies
build_fact >> [update_users, update_geo] >> optimize >> verify >> clear_staging
