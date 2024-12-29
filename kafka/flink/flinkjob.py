from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.get_config().set("pipeline.jars", 
    "file:///opt/flink/files/flink-sql-connector-kafka-3.2.0-1.18.jar;"
    "file:///opt/flink/files/flink-sql-avro-confluent-registry-1.18.1.jar;"
    "file:///opt/flink/files/flink-sql-connector-clickhouse-1.17.1-9.jar")

table_env.get_config().set("table.exec.source.idle-timeout", "5 min")

# CDC Tables
table_env.execute_sql("""
    CREATE TABLE orders_cdc (
        id BIGINT,
        customer_name STRING,
        status STRING,
        total_amount DECIMAL(10, 2),
        created_at STRING,
        updated_at TIMESTAMP(3),
        `kafka_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
        WATERMARK FOR updated_at AS updated_at - INTERVAL '5' MINUTES
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mysql_demo_orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink_orders',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-avro-confluent',
        'debezium-avro-confluent.url' = 'http://schema-registry:8081'
    )
""")

table_env.execute_sql("""
    CREATE TABLE order_items_cdc (
        id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity BIGINT,
        price DECIMAL(10, 2),
        cost_price DECIMAL(10, 2),
        created_at STRING,
        updated_at TIMESTAMP(3),
        `kafka_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
        WATERMARK FOR updated_at AS updated_at - INTERVAL '5' MINUTES
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mysql_demo_order_items',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink_order_items',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-avro-confluent',
        'debezium-avro-confluent.url' = 'http://schema-registry:8081'
    )
""")

# Deduplication views
table_env.execute_sql("""
    CREATE VIEW dedup_orders AS
    SELECT *
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM orders_cdc
    ) WHERE rn = 1
""")

table_env.execute_sql("""
    CREATE VIEW dedup_order_items AS
    SELECT *
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM order_items_cdc
    ) WHERE rn = 1
""")

# ClickHouse sink tables
table_env.execute_sql("""
    CREATE TABLE clickhouse_orders (
        id BIGINT,
        customer_name STRING,
        status STRING,
        total_amount DECIMAL(10, 2),
        created_at STRING,
        updated_at TIMESTAMP(3),
        kafka_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'clickhouse',
        'url' = 'clickhouse://clickhouse:8123',
        'database-name' = 'demo_tables',
        'table-name' = 'flink_orders',
        'sink.batch-size' = '1000',
        'sink.flush-interval' = '1s'
    )
""")

table_env.execute_sql("""
    CREATE TABLE clickhouse_order_items (
        id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity BIGINT,
        price DECIMAL(10, 2),
        cost_price DECIMAL(10, 2),
        created_at STRING,
        updated_at TIMESTAMP(3),
        kafka_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'clickhouse',
        'url' = 'clickhouse://clickhouse:8123',
        'database-name' = 'demo_tables',
        'table-name' = 'flink_order_items',
        'sink.batch-size' = '1000',
        'sink.flush-interval' = '1s'
    )
""")

# Insert deduplicated data into ClickHouse
table_env.execute_sql("""
    INSERT INTO clickhouse_orders
    SELECT id, customer_name, status, total_amount, 
           created_at, updated_at, kafka_timestamp
    FROM dedup_orders
""")

table_env.execute_sql("""
    INSERT INTO clickhouse_order_items
    SELECT id, order_id, product_id, quantity, price, 
           cost_price, created_at, updated_at, kafka_timestamp
    FROM dedup_order_items
""")

# Real-time analytics with deduplicated data
table_env.execute_sql("""
    CREATE TABLE clickhouse_sales_stats (
        date_key DATE,
        total_orders BIGINT,
        total_items_sold BIGINT,
        total_revenue DECIMAL(15, 2),
        total_cost DECIMAL(15, 2),
        total_profit DECIMAL(15, 2),
        average_order_value DECIMAL(15, 2),
        processing_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'clickhouse',
        'url' = 'clickhouse://clickhouse:8123',
        'database-name' = 'demo_tables',
        'table-name' = 'flink_daily_sales_stats',
        'sink.batch-size' = '100',
        'sink.flush-interval' = '1s'
    )
""")

table_env.execute_sql("""
    INSERT INTO clickhouse_sales_stats
    SELECT 
        TO_DATE(o.created_at) AS date_key,
        COUNT(DISTINCT o.id) AS total_orders,
        SUM(i.quantity) AS total_items_sold,
        SUM(i.quantity * i.price) AS total_revenue,
        SUM(i.quantity * i.cost_price) AS total_cost,
        SUM(i.quantity * (i.price - i.cost_price)) AS total_profit,
        SUM(i.quantity * i.price) / COUNT(DISTINCT o.id) AS average_order_value,
        CURRENT_TIMESTAMP AS processing_time
    FROM dedup_orders o
    JOIN dedup_order_items i 
    ON o.id = i.order_id
    WHERE o.status = 'completed'
    GROUP BY TO_DATE(o.created_at)
""")