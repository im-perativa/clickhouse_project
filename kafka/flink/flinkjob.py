from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()

table_env = TableEnvironment.create(env_settings)
table_env.get_config().set("pipeline.jars", 
    "file:///opt/flink/files/flink-sql-connector-kafka-3.2.0-1.18.jar;"
    "file:///opt/flink/files/flink-sql-avro-confluent-registry-1.18.1.jar;"
    "file:///opt/flink/files/flink-sql-connector-clickhouse-1.17.1-9.jar")

# 2. create source Table
table_env.execute_sql("""
    CREATE TABLE flink_orders_cdc (
        id BIGINT,
        customer_name STRING,
        status STRING,
        total_amount DECIMAL(10, 2),
        created_at STRING,
        updated_at STRING,
        `kafka_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mysql_demo_orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.security.protocol' = 'PLAINTEXT',
        'properties.group.id' = 'flink_orders',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-avro-confluent',
        'debezium-avro-confluent.url' = 'http://schema-registry:8081',
        'properties.allow.auto.create.topics' = 'false'
    );
""")

table_env.execute_sql("""
    CREATE TABLE flink_order_items_cdc (
        id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity BIGINT,
        price DECIMAL(10, 2),
        cost_price DECIMAL(10, 2),
        created_at STRING,
        updated_at STRING,
        `kafka_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mysql_demo_order_items',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.security.protocol' = 'PLAINTEXT',
        'properties.group.id' = 'flink_order_items',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-avro-confluent',
        'debezium-avro-confluent.url' = 'http://schema-registry:8081',
        'properties.allow.auto.create.topics' = 'false'
    );
""")

# 3. Create sink Table to print the summary data (using the 'print' connector)
table_env.execute_sql("""
    CREATE TABLE flink_orders_summary (
        date_key DATE,
        total_orders BIGINT,
        total_items_sold BIGINT
    ) WITH (
        'connector' = 'print'
    );
""")

# 4. Insert data into the summary table
table_env.execute_sql("""
    INSERT INTO flink_orders_summary
    SELECT 
        TO_DATE(o.created_at, 'yyyy-MM-dd HH:mm:ss.SSS') AS date_key,
        COUNT(DISTINCT o.id) AS total_orders,
        SUM(i.quantity) AS total_items_sold
    FROM flink_orders_cdc AS o
    JOIN flink_order_items_cdc AS i FOR SYSTEM_TIME AS OF o.order_time
    ON o.id = i.order_id
    WHERE o.status = 'completed'
    GROUP BY TO_DATE(o.created_at, 'yyyy-MM-dd HH:mm:ss.SSS')
""").wait()