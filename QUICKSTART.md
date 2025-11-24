# Быстрый старт

## Предварительная проверка

```bash
# Проверка Docker
docker --version
docker-compose --version

# Проверка свободного места (минимум 20GB)
df -h .

# Проверка свободной памяти (минимум 8GB)
vm_stat | head -5
```

## Запуск за 3 шага

### 1. Запуск всех сервисов

```bash
docker-compose up -d
```

Ожидайте 2-3 минуты, пока все контейнеры инициализируются.

### 2. Проверка статуса

```bash
docker-compose ps
```

Все сервисы должны быть в статусе `Up` или `healthy`.

### 3. Инициализация Kafka топиков

```bash
docker exec kafka bash /scripts/kafka-init.sh
```

## Доступ к интерфейсам

- **Airflow**: http://localhost:8080 (admin/admin)
- **Superset**: http://localhost:8088 (admin/admin)
- **ClickHouse HTTP**: http://localhost:8123
- **Kafka**: localhost:9092

## Запуск ETL процесса

1. Откройте Airflow UI: http://localhost:8080

2. Включите все DAGs (переключатель слева от названия)

3. Запустите вручную DAG `init_data_pipeline`:
   - Нажмите на название DAG
   - Нажмите кнопку "Trigger DAG" (▶️)
   - Дождитесь завершения (все задачи станут зелеными)

4. Остальные DAGs запустятся автоматически по расписанию:
   - `streaming_to_clickhouse` - каждые 5 минут
   - `build_dwh_layer` - каждые 15 минут
   - `data_quality_checks` - каждый час

## Проверка данных

### Проверка в ClickHouse

```bash
# Подключение к ClickHouse
docker exec -it clickhouse clickhouse-client

# Проверка загруженных данных
SELECT count() FROM analytics.fact_events;
SELECT count() FROM analytics.events_browser_raw;

# Выход
exit
```

### Проверка в Kafka

```bash
# Список топиков
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Проверка сообщений
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic browser_events \
  --from-beginning \
  --max-messages 5
```

## Создание дашборда в Superset

1. Откройте Superset: http://localhost:8088

2. Добавьте подключение к базе:
   - Settings → Database Connections → + Database
   - Выберите "ClickHouse"
   - SQLAlchemy URI: `clickhouse+native://default:clickhouse@clickhouse:9000/analytics`
   - Test Connection → Connect

3. Создайте датасет:
   - Data → Datasets → + Dataset
   - Database: ClickHouse
   - Schema: analytics
   - Table: fact_events

4. Создайте первый чарт:
   - Откройте датасет fact_events
   - Charts → + Create Chart
   - Выберите тип визуализации
   - Настройте метрики и фильтры

## Устранение проблем

### Контейнеры не запускаются

```bash
# Просмотр логов
docker-compose logs -f

# Перезапуск конкретного сервиса
docker-compose restart <service_name>

# Полный перезапуск
docker-compose down
docker-compose up -d
```

### Airflow DAG не появляется

```bash
# Проверка логов scheduler
docker-compose logs -f airflow-scheduler

# Проверка синтаксиса DAG
docker exec airflow-webserver python /opt/airflow/dags/init_data_pipeline.py
```

### ClickHouse не принимает подключения

```bash
# Проверка статуса
docker exec clickhouse clickhouse-client -q "SELECT 1"

# Перезапуск
docker-compose restart clickhouse
```

### Нехватка памяти

```bash
# Остановка ненужных контейнеров
docker stop $(docker ps -q --filter "name=other_containers")

# Очистка Docker кэша
docker system prune -f

# Увеличение памяти для Docker Desktop
# Docker Desktop → Settings → Resources → Memory → 8GB+
```

## Остановка платформы

```bash
# Остановка без удаления данных
docker-compose stop

# Остановка с удалением контейнеров (данные сохраняются в volumes)
docker-compose down

# Полная очистка включая данные
docker-compose down -v
```

## Следующие шаги

1. Изучите README.md для подробной документации
2. Исследуйте Airflow DAGs и их задачи
3. Создайте дашборды в Superset
4. Настройте алерты на основе data quality checks
5. Экспериментируйте с запросами в ClickHouse

## Полезные команды

```bash
# Проверка использования ресурсов
docker stats

# Просмотр всех volumes
docker volume ls

# Проверка размера volumes
docker system df -v

# Просмотр сети
docker network ls
docker network inspect data-platform_data-platform
```
