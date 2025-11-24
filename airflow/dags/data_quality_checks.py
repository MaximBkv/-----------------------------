from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from clickhouse_utils import (
    execute_query, 
    get_table_count, 
    check_duplicates,
    get_latest_timestamp
)

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Проверка качества данных в аналитической платформе',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['data-quality', 'monitoring', 'validation'],
)


def check_fact_table_freshness():
    """
    Проверка свежести данных в fact_events
    Данные должны быть не старше 1 часа
    """
    latest_ts = get_latest_timestamp('analytics.fact_events', 'event_timestamp')
    
    if not latest_ts:
        logger.warning("fact_events table is empty")
        return {'status': 'warning', 'message': 'Table is empty'}
    
    from datetime import datetime as dt
    age_hours = (dt.now() - latest_ts).total_seconds() / 3600
    
    logger.info(f"Latest event timestamp: {latest_ts}, age: {age_hours:.2f} hours")
    
    if age_hours > 2:
        logger.error(f"Data is too old: {age_hours:.2f} hours")
        return {'status': 'failed', 'age_hours': age_hours, 'latest_ts': str(latest_ts)}
    
    return {'status': 'passed', 'age_hours': age_hours, 'latest_ts': str(latest_ts)}


def check_null_values():
    """
    Проверка критичных полей на NULL значения
    """
    checks = [
        ('analytics.fact_events', 'event_id'),
        ('analytics.fact_events', 'event_timestamp'),
        ('analytics.fact_events', 'click_id'),
        ('analytics.dim_users', 'user_domain_id'),
    ]
    
    results = []
    
    for table, column in checks:
        query = f"SELECT count() FROM {table} WHERE {column} IS NULL OR {column} = ''"
        result = execute_query(query)
        null_count = result[0][0] if result else 0
        
        total = get_table_count(table)
        null_percent = (null_count / total * 100) if total > 0 else 0
        
        status = 'passed' if null_count == 0 else 'warning'
        
        results.append({
            'table': table,
            'column': column,
            'null_count': null_count,
            'total_count': total,
            'null_percent': null_percent,
            'status': status
        })
        
        if null_count > 0:
            logger.warning(
                f"{table}.{column} has {null_count} ({null_percent:.2f}%) NULL values"
            )
    
    return results


def check_duplicates_in_fact():
    """
    Проверка дубликатов в fact_events по event_id
    """
    duplicates = check_duplicates('analytics.fact_events', ['event_id'])
    
    if duplicates:
        logger.error(f"Found {len(duplicates)} duplicate event_ids in fact_events")
        return {'status': 'failed', 'duplicate_count': len(duplicates), 'samples': duplicates[:5]}
    
    logger.info("No duplicates found in fact_events")
    return {'status': 'passed', 'duplicate_count': 0}


def check_referential_integrity():
    """
    Проверка ссылочной целостности между staging и fact таблицами
    """
    checks = []
    
    # Проверка browser events
    query = """
        SELECT count() 
        FROM analytics.events_browser_raw b
        LEFT JOIN analytics.fact_events f ON b.event_id = f.event_id
        WHERE f.event_id IS NULL
    """
    result = execute_query(query)
    orphaned_browser = result[0][0] if result else 0
    checks.append({
        'check': 'browser_events_orphaned',
        'count': orphaned_browser,
        'status': 'passed' if orphaned_browser == 0 else 'warning'
    })
    
    # Проверка device events
    query = """
        SELECT count() 
        FROM analytics.events_device_raw d
        WHERE d.click_id NOT IN (SELECT DISTINCT click_id FROM analytics.fact_events)
    """
    result = execute_query(query)
    orphaned_device = result[0][0] if result else 0
    checks.append({
        'check': 'device_events_orphaned',
        'count': orphaned_device,
        'status': 'passed' if orphaned_device == 0 else 'warning'
    })
    
    logger.info(f"Referential integrity checks: {checks}")
    return checks


def check_data_consistency():
    """
    Проверка логической согласованности данных
    """
    checks = []
    
    # Проверка: все browser events должны иметь соответствующие location events
    query = """
        SELECT count()
        FROM analytics.fact_events
        WHERE page_url IS NULL OR page_url = ''
    """
    result = execute_query(query)
    missing_location = result[0][0] if result else 0
    
    checks.append({
        'check': 'missing_location_data',
        'count': missing_location,
        'status': 'passed' if missing_location == 0 else 'warning'
    })
    
    # Проверка: device_is_mobile должно соответствовать device_type
    query = """
        SELECT count()
        FROM analytics.fact_events
        WHERE (device_is_mobile = true AND device_type != 'Mobile')
           OR (device_is_mobile = false AND device_type = 'Mobile')
    """
    result = execute_query(query)
    inconsistent_device = result[0][0] if result else 0
    
    checks.append({
        'check': 'device_type_inconsistency',
        'count': inconsistent_device,
        'status': 'passed' if inconsistent_device == 0 else 'warning'
    })
    
    logger.info(f"Data consistency checks: {checks}")
    return checks


def generate_quality_report(**context):
    """
    Генерация итогового отчета по качеству данных
    """
    ti = context['ti']
    
    freshness = ti.xcom_pull(task_ids='check_freshness')
    null_checks = ti.xcom_pull(task_ids='check_nulls')
    duplicate_check = ti.xcom_pull(task_ids='check_duplicates')
    integrity_checks = ti.xcom_pull(task_ids='check_referential_integrity')
    consistency_checks = ti.xcom_pull(task_ids='check_consistency')
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'freshness': freshness,
        'null_values': null_checks,
        'duplicates': duplicate_check,
        'referential_integrity': integrity_checks,
        'data_consistency': consistency_checks
    }
    
    # Подсчет общего количества проблем
    issues_count = 0
    
    if freshness and freshness.get('status') == 'failed':
        issues_count += 1
    
    if null_checks:
        issues_count += sum(1 for c in null_checks if c['null_count'] > 0)
    
    if duplicate_check and duplicate_check.get('duplicate_count', 0) > 0:
        issues_count += 1
    
    if integrity_checks:
        issues_count += sum(1 for c in integrity_checks if c['status'] != 'passed')
    
    if consistency_checks:
        issues_count += sum(1 for c in consistency_checks if c['status'] != 'passed')
    
    report['total_issues'] = issues_count
    report['overall_status'] = 'passed' if issues_count == 0 else 'failed'
    
    logger.info(f"Data quality report: {report}")
    logger.info(f"Total issues found: {issues_count}")
    
    if issues_count > 10:
        raise AirflowException(f"Data quality check failed with {issues_count} issues")
    
    return report


# Tasks
check_freshness = PythonOperator(
    task_id='check_freshness',
    python_callable=check_fact_table_freshness,
    dag=dag,
)

check_nulls = PythonOperator(
    task_id='check_nulls',
    python_callable=check_null_values,
    dag=dag,
)

check_duplicates_task = PythonOperator(
    task_id='check_duplicates',
    python_callable=check_duplicates_in_fact,
    dag=dag,
)

check_integrity = PythonOperator(
    task_id='check_referential_integrity',
    python_callable=check_referential_integrity,
    dag=dag,
)

check_consistency = PythonOperator(
    task_id='check_consistency',
    python_callable=check_data_consistency,
    dag=dag,
)

generate_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    provide_context=True,
    dag=dag,
)

# Dependencies
[check_freshness, check_nulls, check_duplicates_task, check_integrity, check_consistency] >> generate_report
