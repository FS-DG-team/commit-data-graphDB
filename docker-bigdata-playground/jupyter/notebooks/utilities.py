import inspect
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from enum import Enum
import yaml
import os
import github

SPARK_MASTER_URL = "spark://spark-master:7077"

NEO4J_FORMAT = "org.neo4j.spark.DataSource"
ARANGO_FORMAT = "com.arangodb.spark"
TIGERGRAPH_FORMAT = "jdbc"

fileDir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(fileDir, os.environ["CONNECTORS_DEFAULT_OPTIONS"])

with open(configPath, "r") as f:
    config = yaml.safe_load(f)


class SparkConnector(Enum):
    NEO4J = "neo4j"
    ARANGO = "arangodb"
    TIGERGRAPH = "tigergraph"


def get_default_options(connector: SparkConnector):
    # return { key : config [connector.value][key] for key in config[connector.value].keys() }
    options = {}

    for key in config[connector.value].keys():
        options[key] = config[connector.value][key]

    return options

# Create the spark session for a given connector


def create_spark_session(app_name: str, connector: SparkConnector):
    build = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER_URL) \
        .config("spark.executor.memory", "5G") \
        .config("spark.authenticate", "false")

    # Add the connector dependencies
    connectorsDepsDir = os.path.join(fileDir, "dependencies", connector.value)

    def lambdaMap(x): return os.path.join(connectorsDepsDir, x)
    def toListMap(func, lst): return list(map(func, lst))

    dependenciesFiles = os.listdir(connectorsDepsDir)
    dependencies = toListMap(lambdaMap, dependenciesFiles)
    dependenciesStr = ",".join(dependencies)

    build = build.config("spark.jars", dependenciesStr) \
        .config("spark.driver.extraClassPath", dependenciesStr)

    print("Added dependencies: \n", dependenciesFiles)
    return build.getOrCreate()


def spark_write(connector: SparkConnector, df: DataFrame, mode: str, options: dict):
    # Get the connector format
    format = globals()[f"{connector.name}_FORMAT"]

    df.write.mode(mode) \
        .format(format) \
        .options(**options) \
        .save()

    print(f"Dataframe saved to {connector.name}")


def spark_read(connector: SparkConnector, session: SparkSession, options: dir) -> DataFrame:
    # Get the connector format
    format = globals()[f"{connector.name}_FORMAT"]

    df = session.read \
        .format(format) \
        .options(**options) \
        .load()

    print(f"Dataframe loaded from {connector.value}")
    return df


def set_df_columns_nullable(spark, df, column_list, nullable=True):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod


# GitHub API Data integration


def objHandle(obj):
    try:
        return expandGithubObject(obj)
    except:
        return None


def expandGithubObject(object):
    if isinstance(object, list):
        return list(map(lambda x: objHandle(x), object))
    elif isinstance(object, github.PaginatedList.PaginatedList):
        return list(map(lambda x: objHandle(x), object))
    elif isinstance(object, github.NamedUser.NamedUser):
        return object.login
    elif isinstance(object, github.Branch.Branch):
        return object.name
    elif isinstance(object, github.GitRef.GitRef):
        return object.ref
    elif isinstance(object, github.IssueEvent.IssueEvent):
        return object.event
    elif isinstance(object, github.Label.Label):
        return object.name
    elif isinstance(object, github.StatsCodeFrequency.StatsCodeFrequency):
        return collectAttributes(object)
    elif isinstance(object, github.StatsCommitActivity.StatsCommitActivity):
        return collectAttributes(object)
    elif isinstance(object, github.StatsParticipation.StatsParticipation):
        return collectAttributes(object)
    elif isinstance(object, github.StatsPunchCard.StatsPunchCard):
        return collectAttributes(object)
    else:
        return object


def collectAttributes(object, features=None):
    def isAnAttribute(x): return not (x.startswith(
        '__') or x.startswith('_') or x.startswith('get'))

    attributes = list(filter(isAnAttribute, dir(object)))

    result = {}
    for attribute in attributes:
        if attribute not in features:
            continue
        try:
            result[attribute] = expandGithubObject(eval(f"object.{attribute}"))
        except Exception as e:
            print(f'{attribute}: {e}')

    return result


def collectMethodsValue(object):
    def isAGetMethod(x): return x.startswith('get')

    def isAZeroParamMethod(x):
        return len(inspect.getfullargspec(getattr(object, x)).args) == 1

    methods = list(
        filter(lambda x: isAGetMethod(x) and isAZeroParamMethod(x), dir(object))
    )

    result = {}
    for method in methods:
        try:
            result[method.replace("get_", "")] = expandGithubObject(
                getattr(object, method)())
        except Exception as e:
            print(f'{method}: {e}')

    return result
