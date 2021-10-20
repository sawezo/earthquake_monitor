# data definition language statements
import os


KAFKA_BROKER_URI = os.environ.get("KAFKA_BROKER_URI")
TOPIC = os.environ.get("TOPIC")

POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']


create_kafka_source_ddl = f"""
        CREATE TABLE quake_source (
            magnitude DOUBLE,
            significance DOUBLE,
            longitude DOUBLE,
            latitude DOUBLE,
            depth DOUBLE
        ) WITH (
            'connector.type' = 'kafka',
            'connector.version' = 'universal',
            'connector.topic' = 'quake',
            'connector.properties.bootstrap.servers' = 'kafka:9093',
            'connector.startup-mode' = 'latest-offset',
            'format.type' = 'json'
        )"""

create_postgres_sink_ddl = f"""
        CREATE TABLE quake_sink (
            magnitude DOUBLE,
            longitude DOUBLE,
            latitude DOUBLE
        ) WITH (
            'connector'= 'jdbc',
            'url' = 'jdbc:postgresql://postgres_db:5432/{POSTGRES_DB}',
            'table-name' = 'quake_sink',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}'
        )"""