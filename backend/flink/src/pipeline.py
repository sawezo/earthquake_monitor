from pyflink.table.descriptors import Schema, Kafka, Json
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings

from ddl import create_kafka_source_ddl, create_postgres_sink_ddl

from pyflink.common import Configuration
from pyflink.util.java_utils import get_j_env_configuration



def kafka_to_postgres():
    """
    stream data from message bus (Kafka) to database sink (Postgres) using Flink
    """
    # setup the flink environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # create and configure table environment
    table_env = StreamTableEnvironment.create(stream_execution_environment=env)
    configuration = table_env.get_config().get_configuration()
    configuration.set_boolean("python.fn-execution.memory.managed", True)
    # when both allow-latency and size reached mini-batch is executed


    # creating tables (if nonexistant)
    table_env.execute_sql(create_kafka_source_ddl)
    table_env.execute_sql(create_postgres_sink_ddl)
    

    # run the stream processing job
    table_env.execute_sql("""
                          INSERT INTO quake_sink 
                            SELECT t.magnitude, t.longitude, t.latitude
                            FROM quake_source t
                          """)
        

# run job
if __name__ == '__main__':
    kafka_to_postgres()
    print("kafka ---> postgres job running", flush=True)