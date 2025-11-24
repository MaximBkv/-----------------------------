-- Создание базы данных
CREATE DATABASE IF NOT EXISTS analytics;

-- Staging слой: сырые события браузера
CREATE TABLE IF NOT EXISTS analytics.events_browser_raw
(
    event_id String,
    event_timestamp DateTime64(6),
    event_type String,
    click_id String,
    browser_name String,
    browser_user_agent String,
    browser_language String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, event_id)
TTL toDateTime(event_timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Staging слой: сырые данные устройств
CREATE TABLE IF NOT EXISTS analytics.events_device_raw
(
    click_id String,
    os String,
    os_name String,
    os_timezone String,
    device_type String,
    device_is_mobile Bool,
    user_custom_id String,
    user_domain_id String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (click_id, user_domain_id)
TTL inserted_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Staging слой: сырые геоданные
CREATE TABLE IF NOT EXISTS analytics.events_geo_raw
(
    click_id String,
    geo_latitude String,
    geo_longitude String,
    geo_country String,
    geo_timezone String,
    geo_region_name String,
    ip_address String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (click_id, geo_country)
TTL inserted_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Staging слой: сырые данные локаций
CREATE TABLE IF NOT EXISTS analytics.events_location_raw
(
    event_id String,
    page_url String,
    page_url_path String,
    referer_url String,
    referer_medium String,
    utm_medium String,
    utm_source String,
    utm_content String,
    utm_campaign String,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (event_id, utm_source)
TTL inserted_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- DWH слой: справочник пользователей
CREATE TABLE IF NOT EXISTS analytics.dim_users
(
    user_domain_id String,
    user_custom_id String,
    first_seen DateTime,
    last_seen DateTime,
    total_events UInt64,
    device_type String,
    os_name String,
    PRIMARY KEY user_domain_id
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY user_domain_id
SETTINGS index_granularity = 8192;

-- DWH слой: справочник геолокаций
CREATE TABLE IF NOT EXISTS analytics.dim_geo
(
    geo_id String,
    geo_country String,
    geo_region_name String,
    geo_latitude String,
    geo_longitude String,
    geo_timezone String,
    PRIMARY KEY geo_id
)
ENGINE = ReplacingMergeTree()
ORDER BY geo_id
SETTINGS index_granularity = 8192;

-- DWH слой: факты событий
CREATE TABLE IF NOT EXISTS analytics.fact_events
(
    event_id String,
    event_timestamp DateTime64(6),
    event_type String,
    click_id String,
    
    -- Browser data
    browser_name String,
    browser_user_agent String,
    browser_language String,
    
    -- Device data
    os String,
    os_name String,
    os_timezone String,
    device_type String,
    device_is_mobile Bool,
    user_custom_id String,
    user_domain_id String,
    
    -- Geo data
    geo_latitude String,
    geo_longitude String,
    geo_country String,
    geo_timezone String,
    geo_region_name String,
    ip_address String,
    
    -- Location data
    page_url String,
    page_url_path String,
    referer_url String,
    referer_medium String,
    utm_medium String,
    utm_source String,
    utm_content String,
    utm_campaign String,
    
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, event_id, user_domain_id)
TTL toDateTime(event_timestamp) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- DWH слой: агрегаты по часам
CREATE TABLE IF NOT EXISTS analytics.agg_events_hourly
(
    event_hour DateTime,
    event_type String,
    browser_name String,
    device_type String,
    geo_country String,
    utm_source String,
    utm_campaign String,
    
    total_events AggregateFunction(count),
    unique_users AggregateFunction(uniq, String),
    unique_clicks AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_hour)
ORDER BY (event_hour, event_type, browser_name, device_type, geo_country, utm_source, utm_campaign)
TTL event_hour + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- DWH слой: агрегаты по источникам трафика
CREATE TABLE IF NOT EXISTS analytics.agg_traffic_sources
(
    date Date,
    utm_source String,
    utm_medium String,
    utm_campaign String,
    utm_content String,
    referer_medium String,
    
    total_events AggregateFunction(count),
    unique_users AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, utm_source, utm_medium, utm_campaign, utm_content, referer_medium)
TTL date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Материализованное представление для автоматического расчета hourly агрегатов
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.agg_events_hourly_mv
TO analytics.agg_events_hourly
AS
SELECT
    toStartOfHour(event_timestamp) AS event_hour,
    event_type,
    browser_name,
    device_type,
    geo_country,
    utm_source,
    utm_campaign,
    countState() AS total_events,
    uniqState(user_domain_id) AS unique_users,
    uniqState(click_id) AS unique_clicks
FROM analytics.fact_events
GROUP BY event_hour, event_type, browser_name, device_type, geo_country, utm_source, utm_campaign;

-- Материализованное представление для автоматического расчета traffic source агрегатов
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.agg_traffic_sources_mv
TO analytics.agg_traffic_sources
AS
SELECT
    toDate(event_timestamp) AS date,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    referer_medium,
    countState() AS total_events,
    uniqState(user_domain_id) AS unique_users,
    uniqState(click_id) AS unique_sessions
FROM analytics.fact_events
GROUP BY date, utm_source, utm_medium, utm_campaign, utm_content, referer_medium;
