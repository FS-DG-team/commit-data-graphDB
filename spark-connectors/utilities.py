from pyspark.sql import SparkSession
from enum import Enum
import json, os


config = json.load(open(os.environ["CONNECTORS_DEFAULT_OPTIONS"]))

NEO4J_FORMAT = "org.neo4j.spark.DataSource"
ARANGO_FORMAT = "com.arangodb.spark"
TIGERGRAPH_FORMAT = "jdbc"

class SparkConnector(Enum):
    NEO4J = "neo4j"
    ARANGO = "arangodb"
    TIGERGRAPH = "tigergraph"

def get_default_options(connector: SparkConnector):
    options = {}

    if connector == SparkConnector.NEO4J:
        for key in config[SparkConnector.NEO4J.value].keys():
            options[key] = config[SparkConnector.NEO4J.value][key]

    elif connector == SparkConnector.ARANGO:
        for key in config[SparkConnector.ARANGO.value].keys():
            options[key] = config[SparkConnector.ARANGO.value][key]

    elif connector == SparkConnector.TIGERGRAPH:
        for key in config[SparkConnector.TIGERGRAPH.value].keys():
            options[key] = config[SparkConnector.TIGERGRAPH.value][key]

    return options

def create_spark_session(app_name: str, connector: SparkConnector):
    build = SparkSession.builder \
        .appName(app_name)

    if connector == SparkConnector.NEO4J:
        pass
    elif connector == SparkConnector.ARANGO:
        pass
    elif connector == SparkConnector.TIGERGRAPH:
        build = build \
            .config("spark.driver.extraClassPath", "/connectors/tigergraph/jars/tigergraph-jdbc-driver-1.3.6.jar") \
    
    return build.getOrCreate()


def spark_write(connector, df, mode, options):
    write_df = df.write.mode(mode) \
        .options(**options)
    
    if connector == SparkConnector.NEO4J:
        write_df = write_df.format(NEO4J_FORMAT)

    elif connector == SparkConnector.ARANGO:
        write_df = write_df.format(ARANGO_FORMAT)

    elif connector == SparkConnector.TIGERGRAPH:
        write_df = write_df.format(TIGERGRAPH_FORMAT)
    
    write_df.save()