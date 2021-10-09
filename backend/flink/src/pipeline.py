from ddl import create_kafka_source_ddl, create_postgres_sink_ddl

from pyflink.table.udf import udf
from pyflink.table.descriptors import Schema, Kafka, Json
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings


provinces = ("Beijing", "Shanghai", "Hangzhou", "Shenzhen", "Jiangxi", "Chongqing", "Xizang")
@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def flatten_geojson(id):
    """
    process a document to a file for database insertion
    """
    return provinces[id]


def kafka_to_postgres():
    """
    stream data from message bus (Kafka) to database sink (Postgres) using Flink
    """
    # setup the flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    table_env = StreamTableEnvironment.create(stream_execution_environment=env)
    table_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)


    # creating tables (if nonexistant)
    table_env.execute_sql(create_kafka_source_ddl)
    table_env.execute_sql(create_postgres_sink_ddl)
    

    # functions
    table_env.register_function('flatten_geojson', flatten_geojson)


    # stream processing job
    table_env.from_path("quake_source") \
        .select("flatten_geojson(provinceId) as province, payAmount") \
        .group_by("province") \
        .select("province, sum(payAmount) as pay_amount") \
        .execute_insert("quake_sink")
    # can also use table_env.execute_sql("""INSERT INTO...""')/normal sql command


# run job
if __name__ == '__main__':
    kafka_to_postgres()
    print("KAFKA ===> POSTGRES: job running", flush=True)