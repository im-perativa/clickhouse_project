from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

with DAG(
        dag_id='daily_sales_metrics',
        description="Calculate daily sales metrics and update ClickHouse",
        schedule_interval="0 */6 * * *",
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:
    ClickHouseOperator(
        task_id='update_daily_sales_metrics',
        database='demo_tables',
        sql=(
            '''
            INSERT INTO demo_tables.daily_sales_stats 
            WITH latest_order_status AS (
                SELECT 
                    id,
                    argMax(status, kafka_timestamp) AS latest_status
                FROM 
                    demo_tables.orders
                GROUP BY id
            ),
            delta_order_items AS (
                SELECT
                    kafka_timestamp,
                    order_id,
                    quantity,
                    price,
                    cost_price,
                FROM 
                    demo_tables.order_items oi
                WHERE
                    kafka_timestamp <= {{ data_interval_end }}
                    AND kafka_timestamp >= {{ data_interval_start }}
            )
            SELECT
                toDate(kafka_timestamp) as date,
                uniqState(order_id) AS total_orders,
                sumState(quantity) AS total_items_sold,
                sumState(price * quantity) AS total_revenue,
                sumState(cost_price * quantity) AS total_cost,
                sumState((price * quantity) - (cost_price * quantity)) AS total_profit,
                avgState(price * quantity) AS average_order_value
            FROM 
                delta_order_items oi
            INNER JOIN 
                latest_order_status os ON oi.order_id = os.id
            WHERE 
                os.latest_status = 'completed'
            GROUP BY 
                date;
            ''',
            '''
            SELECT 
                date,
                uniqMerge(total_orders) AS total_orders,
                sumMerge(total_items_sold) AS total_items_sold,
                sumMerge(total_revenue) AS total_revenue,
                sumMerge(total_cost) AS total_cost,
                sumMerge(total_profit) AS total_profit,
                avgMerge(average_order_value) AS average_order_value 
            FROM 
                demo_tables.daily_sales_stats 
            GROUP BY
                date;
            '''
            # result of the last query is pushed to XCom
        ),
        # query_id is templated and allows to quickly identify query in ClickHouse logs
        query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
        clickhouse_conn_id='clickhouse_default',
    ) >> PythonOperator(
        task_id='print_result',
        python_callable=lambda task_instance:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='update_daily_sales_metrics')),
    )