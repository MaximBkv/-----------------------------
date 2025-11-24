# Аналитическая платформа на базе Kafka + ClickHouse + Airflow + Superset

Проект представляет собой полнофункциональную аналитическую платформу для обработки потоковых данных веб-аналитики с использованием современного стека data engineering инструментов.

## Архитектура решения

### Стек технологий
- **Apache Kafka** - потоковая обработка данных
- **ClickHouse** - колоночная СУБД для аналитики
- **Apache Airflow** - оркестрация ETL процессов
- **Apache Superset** - визуализация и дашборды
- **PostgreSQL** - метадата для Airflow/Superset
- **Docker Compose** - контейнеризация всех компонентов

### Схема данных

#### Входные данные (JSONL)
1. `browser_events.jsonl` - события браузера
2. `device_events.jsonl` - данные об устройствах
3. `geo_events.jsonl` - геолокация
4. `location_events.jsonl` - URL и источники трафика

#### Kafka топики
- `browser_events` - 3 партиции
- `device_events` - 3 партиции
- `geo_events` - 3 партиции
- `location_events` - 3 партиции

#### ClickHouse таблицы

**Staging слой** (сырые данные, TTL 90 дней):
- `events_browser_raw`
- `events_device_raw`
- `events_geo_raw`
- `events_location_raw`

**DWH слой** (витрины и справочники):
- `fact_events` - агрегированная таблица всех событий (TTL 365 дней)
- `dim_users` - справочник пользователей
- `dim_geo` - справочник геолокаций
- `agg_events_hourly` - агрегаты по часам (materialized view)
- `agg_traffic_sources` - агрегаты по источникам трафика (materialized view)

## Структура проекта

```
.
├── docker-compose.yml          # Конфигурация всех сервисов
├── kafka/
│   └── kafka-init.sh          # Инициализация Kafka топиков
├── clickhouse/
│   ├── init-db.sql            # Схема БД ClickHouse
│   └── clickhouse-config.xml  # Конфигурация ClickHouse
├── airflow/
│   ├── dags/
│   │   ├── init_data_pipeline.py        # Загрузка сэмпл-данных
│   │   ├── streaming_to_clickhouse.py   # Потоковая загрузка
│   │   ├── build_dwh_layer.py           # Построение витрин
│   │   └── data_quality_checks.py       # Проверка качества
│   ├── plugins/
│   │   ├── kafka_producer.py            # Kafka producer утилиты
│   │   └── clickhouse_utils.py          # ClickHouse утилиты
│   └── requirements.txt                 # Python зависимости
├── superset/
│   └── superset-init.sh                 # Инициализация Superset
├── data/
│   └── samples/                         # JSONL файлы с данными
└── README.md
```

## Запуск платформы

### Предварительные требования
- Docker Desktop 4.x+
- Docker Compose 2.x+
- Минимум 8 GB RAM
- 20 GB свободного дискового пространства

### Шаг 1: Запуск всех сервисов

```bash
docker-compose up -d
```

Это запустит:
- Zookeeper (порт 2181)
- Kafka (порт 9092)
- ClickHouse (порты 8123, 9000)
- PostgreSQL (внутренний)
- Airflow Webserver (порт 8080)
- Airflow Scheduler
- Superset (порт 8088)

### Шаг 2: Проверка статуса сервисов

```bash
docker-compose ps
```

Все контейнеры должны быть в статусе `Up` или `healthy`.

### Шаг 3: Инициализация Kafka топиков

```bash
docker exec kafka bash /scripts/kafka-init.sh
```

### Шаг 4: Доступ к интерфейсам

**Airflow UI**
- URL: http://localhost:8080
- Логин: `admin`
- Пароль: `admin`

**Superset UI**
- URL: http://localhost:8088
- Логин: `admin`
- Пароль: `admin`

**ClickHouse**
- HTTP: http://localhost:8123
- Native: localhost:9000
- Пользователь: `default`
- Пароль: `clickhouse`

## Работа с платформой

### Запуск ETL процесса

1. Откройте Airflow UI (http://localhost:8080)

2. Запустите DAG `init_data_pipeline` (вручную):
   - Загружает сэмпл-данные из JSONL в Kafka топики
   - Выполняется один раз для начальной загрузки

3. DAG `streaming_to_clickhouse` (автоматически каждые 5 минут):
   - Читает данные из Kafka
   - Записывает в staging таблицы ClickHouse
   - Запускается автоматически

4. DAG `build_dwh_layer` (автоматически каждые 15 минут):
   - Строит витрины через JOIN staging таблиц
   - Обновляет справочники пользователей и геолокаций
   - Оптимизирует таблицы
   - Очищает старые данные из staging

5. DAG `data_quality_checks` (автоматически каждый час):
   - Проверка свежести данных
   - Проверка на дубликаты
   - Проверка NULL значений
   - Проверка ссылочной целостности

### Описание Airflow DAGs

#### init_data_pipeline
**Назначение**: Начальная загрузка данных из JSONL файлов в Kafka  
**Расписание**: Manual trigger  
**Задачи**:
- `create_kafka_topics` - создание топиков
- `load_browser_events` - загрузка событий браузера
- `load_device_events` - загрузка данных устройств
- `load_geo_events` - загрузка геоданных
- `load_location_events` - загрузка данных локаций
- `verify_data_load` - проверка успешности загрузки

#### streaming_to_clickhouse
**Назначение**: Потоковая загрузка из Kafka в ClickHouse staging  
**Расписание**: Каждые 5 минут  
**Задачи**:
- `consume_browser_events` - чтение browser_events
- `consume_device_events` - чтение device_events
- `consume_geo_events` - чтение geo_events
- `consume_location_events` - чтение location_events
- `validate_ingestion` - валидация загруженных данных

#### build_dwh_layer
**Назначение**: Построение DWH слоя и витрин  
**Расписание**: Каждые 15 минут  
**Задачи**:
- `build_fact_events` - построение факт-таблицы через JOIN
- `update_dim_users` - обновление справочника пользователей
- `update_dim_geo` - обновление справочника геолокаций
- `optimize_tables` - оптимизация таблиц
- `verify_dwh_layer` - проверка целостности DWH
- `clear_staging_old_data` - очистка старых данных (>7 дней)

#### data_quality_checks
**Назначение**: Проверка качества данных  
**Расписание**: Каждый час  
**Задачи**:
- `check_freshness` - проверка актуальности данных
- `check_nulls` - проверка NULL значений в критичных полях
- `check_duplicates` - проверка дубликатов
- `check_referential_integrity` - проверка ссылочной целостности
- `check_consistency` - проверка логической согласованности
- `generate_quality_report` - генерация отчета

### Работа с ClickHouse

#### Подключение через CLI

```bash
docker exec -it clickhouse clickhouse-client
```

#### Примеры SQL запросов

Количество событий по типам:
```sql
SELECT event_type, count() 
FROM analytics.fact_events 
GROUP BY event_type;
```

Топ-10 стран по количеству событий:
```sql
SELECT geo_country, count() as events 
FROM analytics.fact_events 
GROUP BY geo_country 
ORDER BY events DESC 
LIMIT 10;
```

Статистика по источникам трафика:
```sql
SELECT 
    utm_source,
    utm_campaign,
    countMerge(total_events) as total_events,
    uniqMerge(unique_users) as unique_users
FROM analytics.agg_traffic_sources
GROUP BY utm_source, utm_campaign
ORDER BY total_events DESC;
```

События по часам:
```sql
SELECT 
    event_hour,
    event_type,
    countMerge(total_events) as events,
    uniqMerge(unique_users) as users
FROM analytics.agg_events_hourly
WHERE event_hour >= now() - INTERVAL 24 HOUR
GROUP BY event_hour, event_type
ORDER BY event_hour DESC;
```

### Работа с Superset

1. Войдите в Superset (http://localhost:8088)

2. Добавьте подключение к ClickHouse:
   - Settings → Database Connections → + Database
   - SQLAlchemy URI: `clickhouse+native://default:clickhouse@clickhouse:9000/analytics`
   - Test Connection → Connect

3. Создайте датасеты:
   - Data → Datasets → + Dataset
   - Выберите базу данных и таблицу
   - Рекомендуемые таблицы:
     - `analytics.fact_events` - для детального анализа
     - `analytics.dim_users` - для анализа пользователей
     - `analytics.dim_geo` - для геоанализа

4. Создайте дашборды:

**Dashboard 1: Web Analytics Overview**
- Metric: Total Events (COUNT)
- Time series: Events by Hour
- Pie chart: Events by Type
- Bar chart: Top Browsers

**Dashboard 2: Traffic Sources**
- Table: UTM Source/Medium/Campaign
- Sunburst: utm_source → utm_campaign
- Line chart: Traffic over time by source

**Dashboard 3: Geo Distribution**
- Map: Events by Country
- Bar chart: Top 20 Regions
- Table: Country stats with device breakdown

**Dashboard 4: User Behavior**
- Funnel: Page path analysis
- Sankey: Referer → Page flow
- Cohort: User retention

## Мониторинг и отладка

### Логи сервисов

Airflow:
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

ClickHouse:
```bash
docker-compose logs -f clickhouse
```

Kafka:
```bash
docker-compose logs -f kafka
```

### Проверка данных в Kafka

Список топиков:
```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

Чтение сообщений из топика:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic browser_events \
  --from-beginning \
  --max-messages 10
```

### Проверка данных в ClickHouse

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM analytics.fact_events"
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM analytics.events_browser_raw"
```

## Масштабирование

### Увеличение партиций Kafka

Редактируйте `kafka/kafka-init.sh`, увеличьте количество партиций:
```bash
--partitions 6
```

### Горизонтальное масштабирование ClickHouse

В production окружении:
1. Настройте ClickHouse cluster с репликацией
2. Используйте `ReplicatedMergeTree` вместо `MergeTree`
3. Добавьте Zookeeper для координации

### Масштабирование Airflow

Замените LocalExecutor на CeleryExecutor:
```yaml
AIRFLOW__CORE__EXECUTOR: CeleryExecutor
```

Добавьте Redis и worker ноды.

## Остановка платформы

Остановка всех сервисов:
```bash
docker-compose down
```

Остановка с удалением данных:
```bash
docker-compose down -v
```

## Troubleshooting

### Kafka недоступен
```bash
docker-compose restart kafka zookeeper
```

### Airflow DAG не запускается
1. Проверьте логи scheduler
2. Убедитесь, что зависимости установлены
3. Проверьте синтаксис Python в DAG файле

### ClickHouse не принимает данные
1. Проверьте подключение: `docker exec -it clickhouse clickhouse-client`
2. Проверьте наличие базы: `SHOW DATABASES`
3. Проверьте права пользователя

### Superset не подключается к ClickHouse
1. Убедитесь, что драйвер установлен: `pip install clickhouse-sqlalchemy`
2. Проверьте URI подключения
3. Проверьте сетевое взаимодействие между контейнерами

## Дополнительная информация

### Производительность

- Kafka retention: 7 дней
- ClickHouse staging TTL: 90 дней
- ClickHouse DWH TTL: 365 дней
- Batch size Kafka consumer: 1000 записей
- Airflow scheduler interval: 5 минут (streaming), 15 минут (DWH)

### Безопасность

Для production использования рекомендуется:
1. Изменить все пароли по умолчанию
2. Настроить SSL/TLS для всех соединений
3. Использовать секреты вместо переменных окружения
4. Настроить аутентификацию и авторизацию
5. Ограничить доступ к портам через firewall

### Бэкап

ClickHouse:
```bash
docker exec clickhouse clickhouse-client -q "BACKUP DATABASE analytics TO Disk('backups', 'analytics_backup')"
```

Kafka (используйте Kafka MirrorMaker для репликации)

Airflow метадата:
```bash
docker exec postgres pg_dump -U airflow airflow > airflow_backup.sql
```

## Контакты и поддержка

Проект разработан как тестовое задание для позиции Data Engineer.

Автор решения использовал production-ready подходы и best practices индустрии data engineering.
