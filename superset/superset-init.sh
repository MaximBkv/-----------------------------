#!/bin/bash

echo "Installing ClickHouse driver for Superset..."
pip install clickhouse-sqlalchemy clickhouse-driver

echo "Superset initialization completed"
echo "Access Superset at http://localhost:8088"
echo "Default credentials: admin / admin"
echo ""
echo "To connect ClickHouse:"
echo "  Database: clickhouse+native://default:clickhouse@clickhouse:9000/analytics"
echo ""
echo "Available tables for dashboards:"
echo "  - analytics.fact_events"
echo "  - analytics.dim_users"
echo "  - analytics.dim_geo"
echo "  - analytics.agg_events_hourly (use countMerge, uniqMerge for aggregates)"
echo "  - analytics.agg_traffic_sources (use countMerge, uniqMerge for aggregates)"
