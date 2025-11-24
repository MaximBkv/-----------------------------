# Создание дашбордов в Superset

## Предварительные шаги

### 1. Загрузка данных в систему

Перейдите в Airflow: http://localhost:8080 (admin/admin)

Запустите DAG `init_data_pipeline`:
1. Найдите DAG `init_data_pipeline` в списке
2. Включите его (toggle слева)
3. Нажмите кнопку ▶️ (Trigger DAG) справа
4. Дождитесь завершения (5-10 минут)

Этот DAG загрузит ~4000 событий из JSONL файлов в Kafka.

### 2. Подождите обработки данных

После `init_data_pipeline` автоматически запустятся:
- `streaming_to_clickhouse` - переместит данные из Kafka в ClickHouse
- `build_dwh_layer` - построит витрины и агрегаты

Подождите 15-20 минут для полной обработки.

### 3. Проверьте данные в ClickHouse

```bash
# Проверка количества событий
docker exec clickhouse clickhouse-client -q "SELECT count() FROM analytics.fact_events"
```

Должно быть ~3000-4000 записей.

---

## Подключение ClickHouse к Superset

### Шаг 1: Откройте Superset
URL: http://localhost:8088  
Логин: admin  
Пароль: admin

### Шаг 2: Добавьте базу данных
1. Settings → Database Connections
2. Нажмите "+ Database"
3. Выберите "ClickHouse" из списка
4. Введите параметры:
   - **Display Name:** ClickHouse Analytics
   - **SQLAlchemy URI:** `clickhouse+native://default:clickhouse@clickhouse:9000/analytics`
5. Test Connection
6. Connect

---

## Создание датасетов

Datasets - это таблицы/views, на основе которых строятся визуализации.

### Датасет 1: fact_events (основные события)

1. Data → Datasets → + Dataset
2. Database: ClickHouse Analytics
3. Schema: analytics
4. Table: fact_events
5. Add

### Датасет 2: dim_users (пользователи)

1. Data → Datasets → + Dataset
2. Database: ClickHouse Analytics
3. Schema: analytics
4. Table: dim_users
5. Add

### Датасет 3: Hourly Events (агрегаты по часам)

Создайте Virtual Dataset с SQL:

```sql
SELECT 
    event_hour,
    event_type,
    geo_country,
    utm_source,
    countMerge(total_events) as total_events,
    uniqMerge(unique_users) as unique_users
FROM analytics.agg_events_hourly
GROUP BY event_hour, event_type, geo_country, utm_source
ORDER BY event_hour DESC
```

### Датасет 4: Traffic Sources (источники трафика)

```sql
SELECT 
    date,
    utm_source,
    utm_medium,
    utm_campaign,
    countMerge(total_events) as total_events,
    uniqMerge(unique_users) as unique_users
FROM analytics.agg_traffic_sources
GROUP BY date, utm_source, utm_medium, utm_campaign
ORDER BY date DESC, total_events DESC
```

---

## Dashboard 1: Web Analytics Overview

### Chart 1: Total Events (Big Number)

**Тип:** Big Number  
**Датасет:** fact_events  
**Metric:** COUNT(*)  
**Название:** Total Events

### Chart 2: Unique Users (Big Number)

**Тип:** Big Number  
**Датасет:** fact_events  
**Metric:** COUNT(DISTINCT user_domain_id)  
**Название:** Unique Users

### Chart 3: Events by Type (Pie Chart)

**Тип:** Pie Chart  
**Датасет:** fact_events  
**Dimension:** event_type  
**Metric:** COUNT(*)  
**Название:** Events by Type

### Chart 4: Events Timeline (Line Chart)

**Тип:** Time-series Line Chart  
**Датасет:** fact_events  
**Time Column:** event_timestamp  
**Metric:** COUNT(*)  
**Granularity:** Hour  
**Название:** Events Over Time

### Chart 5: Top Browsers (Bar Chart)

**Тип:** Bar Chart  
**Датасет:** fact_events  
**Dimension:** browser_name  
**Metric:** COUNT(*)  
**Sort:** Descending  
**Row Limit:** 10  
**Название:** Top 10 Browsers

### Chart 6: Device Types (Donut Chart)

**Тип:** Pie Chart (Donut)  
**Датасет:** fact_events  
**Dimension:** device_type  
**Metric:** COUNT(*)  
**Название:** Device Distribution

---

## Dashboard 2: Traffic Sources Analysis

### Chart 1: Traffic by Source (Table)

**Тип:** Table  
**Датасет:** Traffic Sources  
**Columns:**
- utm_source
- utm_medium
- utm_campaign
- total_events
- unique_users

**Metrics:**
- SUM(total_events)
- SUM(unique_users)

**Group By:**
- utm_source
- utm_medium
- utm_campaign

**Название:** Traffic Sources Breakdown

### Chart 2: Source Performance (Bar Chart)

**Тип:** Bar Chart  
**Датасет:** fact_events  
**Dimension:** utm_source  
**Metrics:**
- COUNT(*) as Events
- COUNT(DISTINCT user_domain_id) as Users

**Название:** Performance by Traffic Source

### Chart 3: Campaign Comparison (Grouped Bar)

**Тип:** Bar Chart (Grouped)  
**Датасет:** fact_events  
**Dimension:** utm_campaign  
**Group By:** utm_source  
**Metric:** COUNT(*)  
**Название:** Campaigns by Source

### Chart 4: Traffic Trend (Area Chart)

**Тип:** Area Chart  
**Датасет:** fact_events  
**Time Column:** event_timestamp  
**Breakdown:** utm_source  
**Metric:** COUNT(*)  
**Granularity:** Day  
**Название:** Traffic Trends

---

## Dashboard 3: Geographic Distribution

### Chart 1: Events by Country (Table)

**Тип:** Table  
**Датасет:** fact_events  
**Columns:**
- geo_country
- geo_region_name
- COUNT(*) as events
- COUNT(DISTINCT user_domain_id) as users

**Group By:**
- geo_country
- geo_region_name

**Sort:** events DESC  
**Row Limit:** 20  
**Название:** Top 20 Locations

### Chart 2: Country Distribution (Pie Chart)

**Тип:** Pie Chart  
**Датасет:** fact_events  
**Dimension:** geo_country  
**Metric:** COUNT(*)  
**Row Limit:** 10  
**Название:** Top 10 Countries

### Chart 3: Regional Breakdown (Treemap)

**Тип:** Treemap  
**Датасет:** fact_events  
**Dimensions:**
- geo_country (Primary)
- geo_region_name (Secondary)

**Metric:** COUNT(*)  
**Название:** Geographic Hierarchy

---

## Dashboard 4: User Behavior

### Chart 1: Top Pages (Bar Chart)

**Тип:** Bar Chart  
**Датасет:** fact_events  
**Dimension:** page_url_path  
**Metric:** COUNT(*)  
**Sort:** Descending  
**Row Limit:** 15  
**Название:** Most Visited Pages

### Chart 2: Referrer Analysis (Sunburst)

**Тип:** Sunburst  
**Датасет:** fact_events  
**Hierarchy:**
- referer_medium
- utm_source

**Metric:** COUNT(*)  
**Название:** Referrer Flow

### Chart 3: User Journey (Sankey)

Создайте Virtual Dataset:

```sql
SELECT 
    utm_source as source,
    page_url_path as target,
    count() as value
FROM analytics.fact_events
GROUP BY utm_source, page_url_path
HAVING value > 10
ORDER BY value DESC
LIMIT 50
```

**Тип:** Sankey Diagram  
**Source:** source  
**Target:** target  
**Value:** value  
**Название:** User Journey Flow

---

## Dashboard 5: Real-time Monitoring

### Chart 1: Last Hour Activity (Big Number with Trendline)

**Тип:** Big Number with Trendline  
**Датасет:** Hourly Events  
**Time Column:** event_hour  
**Time Range:** Last 24 hours  
**Metric:** SUM(total_events)  
**Название:** Activity Last 24h

### Chart 2: Hourly Distribution (Heatmap)

**Тип:** Heatmap  
**Датасет:** Hourly Events  
**X-Axis:** HOUR(event_hour)  
**Y-Axis:** event_type  
**Metric:** SUM(total_events)  
**Название:** Activity Heatmap

### Chart 3: Active Users (Line Chart)

**Тип:** Time-series Line Chart  
**Датасет:** Hourly Events  
**Time Column:** event_hour  
**Metric:** SUM(unique_users)  
**Granularity:** Hour  
**Time Range:** Last 48 hours  
**Название:** Active Users Trend

---

## Собираем дашборды

### Создание Dashboard:

1. Dashboards → + Dashboard
2. Название: "Web Analytics Overview"
3. Перетащите созданные чарты на дашборд
4. Расположите в удобном порядке
5. Добавьте Filters:
   - geo_country
   - utm_source
   - device_type
   - event_type
6. Save

### Рекомендуемая структура дашборда:

```
+------------------+------------------+
| Total Events     | Unique Users     |
+------------------+------------------+
| Events Timeline (full width)       |
+------------------------------------+
| Events by Type   | Device Types    |
+------------------+------------------+
| Top Browsers (full width)          |
+------------------------------------+
```

---

## Полезные SQL запросы для Custom Charts

### Conversion Funnel

```sql
SELECT 
    page_url_path as step,
    count() as users,
    round(100.0 * count() / (SELECT count() FROM analytics.fact_events), 2) as percentage
FROM analytics.fact_events
WHERE page_url_path IN ('/home', '/product_a', '/product_b', '/checkout')
GROUP BY page_url_path
ORDER BY users DESC
```

### Cohort Analysis (Daily)

```sql
SELECT 
    toDate(event_timestamp) as date,
    device_type,
    count() as events,
    uniq(user_domain_id) as unique_users,
    round(events / unique_users, 2) as events_per_user
FROM analytics.fact_events
WHERE event_timestamp >= today() - 7
GROUP BY date, device_type
ORDER BY date DESC, device_type
```

### UTM Performance Matrix

```sql
SELECT 
    utm_source,
    utm_medium,
    utm_campaign,
    count() as total_events,
    uniq(user_domain_id) as unique_users,
    uniq(click_id) as unique_sessions,
    round(total_events / unique_sessions, 2) as events_per_session
FROM analytics.fact_events
WHERE utm_source != ''
GROUP BY utm_source, utm_medium, utm_campaign
ORDER BY total_events DESC
LIMIT 20
```

### Geographic Performance

```sql
SELECT 
    geo_country,
    count() as events,
    uniq(user_domain_id) as users,
    round(events / users, 2) as events_per_user,
    countIf(event_type = 'conversion') as conversions,
    round(100.0 * conversions / events, 2) as conversion_rate
FROM analytics.fact_events
GROUP BY geo_country
HAVING events > 50
ORDER BY events DESC
LIMIT 15
```

---

## Автоматизация обновления

Superset автоматически обновляет данные при открытии дашборда.

Для настройки автообновления:
1. Dashboard → Settings (⚙️)
2. Auto Refresh → Enable
3. Interval → 5 minutes
4. Save

---

## Экспорт и Share

### Export Dashboard:
1. Dashboard → ... → Export
2. Формат: JSON
3. Сохраните файл

### Share Dashboard:
1. Dashboard → ... → Share
2. Copy Link или Schedule Email

---

## Troubleshooting

### Данных нет в чартах
```bash
# Проверьте данные в ClickHouse
docker exec clickhouse clickhouse-client -q "SELECT count() FROM analytics.fact_events"

# Запустите build_dwh_layer в Airflow
```

### Ошибка подключения к ClickHouse
```bash
# Проверьте ClickHouse
docker ps | grep clickhouse

# Переподключите базу в Superset
# Settings → Database Connections → Edit → Test Connection
```

### Агрегаты показывают 0
Используйте функции -Merge для агрегатных таблиц:
- `countMerge(total_events)` вместо `SUM(total_events)`
- `uniqMerge(unique_users)` вместо `COUNT(DISTINCT ...)`

---

## Следующие шаги

1. Запустите DAG `init_data_pipeline` в Airflow
2. Дождитесь обработки данных (15-20 минут)
3. Создайте подключение к ClickHouse в Superset
4. Создайте датасеты из таблиц analytics
5. Постройте чарты согласно инструкциям выше
6. Соберите дашборды

**Рекомендация:** Начните с Dashboard 1 (Web Analytics Overview), он самый простой и покажет основные метрики.
