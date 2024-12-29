#!/bin/bash
set -e
clickhouse client -n <<-EOSQL
CREATE DATABASE IF NOT EXISTS kafka_tables;

CREATE DATABASE IF NOT EXISTS demo_tables;

CREATE OR REPLACE TABLE kafka_tables.kafka_products_cdc (          
    "before.Value.id" UInt32,
    "before.Value.name" String,
    "before.Value.description" Nullable(String),
    "before.Value.price" Decimal(10, 2),
    "before.Value.cost_price" Decimal(10, 2),
    "before.Value.created_at" String,
    "before.Value.updated_at" String,
    "after.Value.id" UInt32,
    "after.Value.name" String,
    "after.Value.description" Nullable(String),
    "after.Value.price" Decimal(10, 2),
    "after.Value.cost_price" Decimal(10, 2),
    "after.Value.created_at" String,
    "after.Value.updated_at" String,
    op String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'mysql_demo_products',
         kafka_group_name = 'clickhouse_kafka_products',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         input_format_avro_allow_missing_fields = 1;

CREATE OR REPLACE TABLE kafka_tables.kafka_orders_cdc (          
    "before.Value.id" UInt32,
    "before.Value.customer_name" String,
    "before.Value.status" String,
    "before.Value.total_amount" Decimal(10, 2),
    "before.Value.created_at" String,
    "before.Value.updated_at" String,
    "after.Value.id" UInt32,
    "after.Value.customer_name" String,
    "after.Value.status" String,
    "after.Value.total_amount" Decimal(10, 2),
    "after.Value.created_at" String,
    "after.Value.updated_at" String,
    op String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'mysql_demo_orders',
         kafka_group_name = 'clickhouse_kafka_orders',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         input_format_avro_allow_missing_fields = 1;

CREATE OR REPLACE TABLE kafka_tables.kafka_order_items_cdc (          
    "before.Value.id" UInt32,
    "before.Value.order_id" UInt32,
    "before.Value.product_id" UInt32,
    "before.Value.quantity" UInt32,
    "before.Value.price" Decimal(10, 2),
    "before.Value.cost_price" Decimal(10, 2),
    "before.Value.created_at" String,
    "before.Value.updated_at" String,
    "after.Value.id" UInt32,
    "after.Value.order_id" UInt32,
    "after.Value.product_id" UInt32,
    "after.Value.quantity" UInt32,
    "after.Value.price" Decimal(10, 2),
    "after.Value.cost_price" Decimal(10, 2),
    "after.Value.created_at" String,
    "after.Value.updated_at" String,
    op String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'mysql_demo_order_items',
         kafka_group_name = 'clickhouse_kafka_order_items',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         input_format_avro_allow_missing_fields = 1;


CREATE OR REPLACE TABLE demo_tables.products (
    id UInt32,
    name String,
    description Nullable(String),
    price Decimal(10, 2), 
    cost_price Decimal(10, 2), 
    created_at DateTime,
    updated_at DateTime,
    op String,
    kafka_timestamp Int64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(kafka_timestamp)
ORDER BY id
PRIMARY KEY id;

CREATE OR REPLACE TABLE demo_tables.orders (
    id UInt32,
    customer_name String,
    status String,
    total_amount Decimal(10, 2), 
    created_at DateTime,
    updated_at DateTime,
    op String,
    kafka_timestamp Int64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(kafka_timestamp)
ORDER BY id
PRIMARY KEY id;

CREATE OR REPLACE TABLE demo_tables.order_items (
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity UInt32 DEFAULT 1,
    price Decimal(10, 2),
    cost_price Decimal(10, 2),
    created_at DateTime,
    updated_at DateTime,
    op String,
    kafka_timestamp Int64,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(kafka_timestamp)
ORDER BY id
PRIMARY KEY id;


CREATE MATERIALIZED VIEW demo_tables.products_mv
TO demo_tables.products
AS
SELECT
    if(op = 'd', "before.Value.id", "after.Value.id") AS id,
    if(op = 'd', "before.Value.name", "after.Value.name") AS name,
    if(op = 'd', "before.Value.description", "after.Value.description") AS description,
    if(op = 'd', "before.Value.price", "after.Value.price") AS price,
    if(op = 'd', "before.Value.cost_price", "after.Value.cost_price") AS cost_price,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.created_at", "after.Value.created_at")) AS created_at,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.updated_at", "after.Value.updated_at")) AS updated_at,
    op AS op,
    _timestamp AS kafka_timestamp,
    if(op = 'd', 1, 0) AS is_deleted
FROM kafka_tables.kafka_products_cdc
WHERE op IN ('c', 'r', 'u', 'd');

CREATE MATERIALIZED VIEW demo_tables.orders_mv
TO demo_tables.orders
AS
SELECT
    if(op = 'd', "before.Value.id", "after.Value.id") AS id,
    if(op = 'd', "before.Value.customer_name", "after.Value.customer_name") AS customer_name,
    if(op = 'd', "before.Value.status", "after.Value.status") AS status,
    if(op = 'd', "before.Value.total_amount", "after.Value.total_amount") AS total_amount,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.created_at", "after.Value.created_at")) AS created_at,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.updated_at", "after.Value.updated_at")) AS updated_at,
    op AS op,
    _timestamp AS kafka_timestamp,
    if(op = 'd', 1, 0) AS is_deleted
FROM kafka_tables.kafka_orders_cdc
WHERE op IN ('c', 'r', 'u', 'd');

CREATE MATERIALIZED VIEW demo_tables.order_items_mv
TO demo_tables.order_items
AS
SELECT
    if(op = 'd', "before.Value.id", "after.Value.id") AS id,
    if(op = 'd', "before.Value.order_id", "after.Value.order_id") AS order_id,
    if(op = 'd', "before.Value.product_id", "after.Value.product_id") AS product_id,
    if(op = 'd', "before.Value.quantity", "after.Value.quantity") AS quantity,
    if(op = 'd', "before.Value.price", "after.Value.price") AS price,
    if(op = 'd', "before.Value.cost_price", "after.Value.cost_price") AS cost_price,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.created_at", "after.Value.created_at")) AS created_at,
    parseDateTimeBestEffort(if(op = 'd', "before.Value.updated_at", "after.Value.updated_at")) AS updated_at,
    op AS op,
    _timestamp AS kafka_timestamp,
    if(op = 'd', 1, 0) AS is_deleted
FROM kafka_tables.kafka_order_items_cdc
WHERE op IN ('c', 'r', 'u', 'd');

CREATE OR REPLACE TABLE demo_tables.daily_sales_stats (
    date Date,
    total_orders AggregateFunction(uniq, UInt32),
    total_items_sold AggregateFunction(sum, UInt32),
    total_revenue AggregateFunction(sum, Decimal(10, 2)),
    total_cost AggregateFunction(sum, Decimal(10, 2)),
    total_profit AggregateFunction(sum, Decimal(10, 2)),
    average_order_value AggregateFunction(avg, Decimal(10, 2))
)
ENGINE = AggregatingMergeTree
ORDER BY date;
EOSQL
