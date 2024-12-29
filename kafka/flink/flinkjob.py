from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()

table_env = TableEnvironment.create(env_settings)
table_env.get_config().set("pipeline.jars", "file://///opt/flink/files/flink-sql-connector-kafka-3.2.0-1.18.jar;file://///opt/flink/files/flink-sql-avro-confluent-registry-1.18.1.jar;file://///opt/flink/files/flink-sql-connector-clickhouse-1.17.1-9.jar")
table_env.get_config().set('restart-strategy.type', 'fixed-delay')
table_env.get_config().set('restart-strategy.fixed-delay.attempts', '3')
table_env.get_config().set('restart-strategy.fixed-delay.delay', '10000 ms')

# 2. create source Table
# table_env.execute_sql("""
#     CREATE TABLE t_orders (
#         op STRING
#     ) WITH (
#         'connector' = 'clickhouse',
#         'url' = 'clickhouse://localhost:8123',
#         'database-name' = 'demo_tables',
#         'table-name' = 'orders',
#         'sink.batch-size' = '500',
#         'sink.flush-interval' = '1000',
#         'sink.max-retries' = '3'
#     );
# """)
table_env.execute_sql("""
    CREATE TABLE flink_products_cdc (
        `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mysql_demo_products',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.security.protocol' = 'PLAINTEXT',
        'properties.group.id' = 'flink_products',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'avro-confluent',
        'properties.allow.auto.create.topics' = 'false',
        'avro-confluent.url' = 'http://schema-registry:8081'
    );
""")

# 3. create sink Table
table_env.execute_sql("""
    CREATE TABLE print_sink (
        `kafka_timestamp` TIMESTAMP_LTZ(3)
    ) WITH (
        'connector' = 'print'
    )
""")

table_env.execute_sql("""
    INSERT INTO print_sink
        SELECT * FROM flink_products_cdc
""")