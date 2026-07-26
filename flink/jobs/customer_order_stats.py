"""
Starter Flink job: per-customer order stats over the Debezium orders topic.

Reads the CDC `orders` topic straight from Kafka using Flink's built-in
`debezium-json` format (no manual envelope parsing needed), aggregates
order count + total quantity per customer over a 1-minute tumbling
processing-time window, and writes the results to a new Kafka topic.

Run inside the Flink image:
    flink run -py /opt/flink/jobs/customer_order_stats.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SOURCE_TOPIC = "dbserver1.inventory.orders"
SINK_TOPIC = "flink.orders.customer-stats"


def main() -> None:
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE orders (
            id INT,
            customer_id INT,
            quantity INT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-customer-order-stats',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    table_env.execute_sql(f"""
        CREATE TABLE customer_order_stats (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            customer_id INT,
            order_count BIGINT,
            total_quantity BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # Non-blocking submit: this script is the *submitting client*, run once
    # by flink-job-submit. The job itself keeps running on the cluster after
    # this process exits.
    table_env.execute_sql("""
        INSERT INTO customer_order_stats
        SELECT
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE)   AS window_end,
            customer_id,
            COUNT(*)      AS order_count,
            SUM(quantity) AS total_quantity
        FROM orders
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), customer_id
    """)


if __name__ == "__main__":
    main()
