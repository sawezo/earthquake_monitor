# data definition language statements


import os
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']


create_kafka_source_ddl = """
        CREATE TABLE quake_source (
            createTime VARCHAR,
            orderId BIGINT,
            payAmount DOUBLE,
            payPlatform INT,
            provinceId INT
        ) WITH (
            'connector.type' = 'kafka',
            'connector.version' = 'universal',
            'connector.topic' = 'quake',
            'connector.properties.bootstrap.servers' = 'kafka:9093',
            'connector.properties.group.id' = 'quake',
            'connector.startup-mode' = 'latest-offset',
            'format.type' = 'json'
        )"""

create_postgres_sink_ddl = f"""
        CREATE TABLE IF NOT EXISTS quake_sink (
            province VARCHAR ( 50 ),
            pay_amount DECIMAL,
            PRIMARY KEY (province) NOT ENFORCED              
        ) WITH (
            'connector'= 'jdbc',
            'url' = 'jdbc:postgresql://postgres_db:5432/{POSTGRES_DB}',
            'table-name' = 'quake_sink',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}'
        )"""