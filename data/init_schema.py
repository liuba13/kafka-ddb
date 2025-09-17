from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "energy_kafka_db"

TABLES = {
    "power_metrics": """
        CREATE TABLE IF NOT EXISTS power_metrics (
            plant_id TEXT,
            timestamp TIMESTAMP,
            station_name TEXT,
            station_type TEXT,
            power_output_mw DOUBLE,
            voltage_kv DOUBLE,
            frequency_hz DOUBLE,
            efficiency_percent DOUBLE,
            kafka_version TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (plant_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        AND default_time_to_live = 604800;
    """,
    "power_by_region": """
        CREATE TABLE IF NOT EXISTS power_by_region (
            region TEXT,
            timestamp TIMESTAMP,
            plant_id TEXT,
            station_name TEXT,
            power_output_mw DOUBLE,
            PRIMARY KEY (region, timestamp, plant_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        AND default_time_to_live = 604800;
    """,
    "power_by_type": """
        CREATE TABLE IF NOT EXISTS power_by_type (
            station_type TEXT,
            timestamp TIMESTAMP,
            plant_id TEXT,
            power_output_mw DOUBLE,
            efficiency_percent DOUBLE,
            PRIMARY KEY (station_type, timestamp, plant_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        AND default_time_to_live = 604800;
    """,
    "power_hourly_aggregates": """
        CREATE TABLE IF NOT EXISTS power_hourly_aggregates (
            plant_id TEXT,
            hour_bucket TIMESTAMP,
            avg_power_mw DOUBLE,
            max_power_mw DOUBLE,
            min_power_mw DOUBLE,
            count_records INT,
            PRIMARY KEY (plant_id, hour_bucket)
        ) WITH CLUSTERING ORDER BY (hour_bucket DESC)
        AND default_time_to_live = 2592000;
    """,
    "power_alerts": """
        CREATE TABLE IF NOT EXISTS power_alerts (
            plant_id TEXT,
            timestamp TIMESTAMP,
            alert_type TEXT,
            severity TEXT,
            message TEXT,
            value DOUBLE,
            threshold DOUBLE,
            acknowledged BOOLEAN,
            PRIMARY KEY (plant_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        AND default_time_to_live = 2592000;
    """
}

def main():
    cluster = Cluster(["127.0.0.1"])  # замініть IP, якщо не локально
    session = cluster.connect()

    # створення keyspace, якщо не існує
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
        AND durable_writes = true;
    """)

    session.set_keyspace(KEYSPACE)

    # створення таблиць
    for name, ddl in TABLES.items():
        print(f"Створюємо таблицю {name}...")
        session.execute(SimpleStatement(ddl))

    # перевірка створених таблиць
    rows = session.execute(f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = '{KEYSPACE}';
    """)
    print("\nСписок таблиць у keyspace:", KEYSPACE)
    for row in rows:
        print("-", row.table_name)

    cluster.shutdown()

if __name__ == "__main__":
    main()
