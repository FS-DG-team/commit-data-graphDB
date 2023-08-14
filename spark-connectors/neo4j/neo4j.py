import sys

sys.path.append("/connectors")
from utilities import *

session = create_spark_session("Neo4j", SparkConnector.NEO4J)

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

options = get_default_options(SparkConnector.NEO4J)
options["node.keys"] = "id"
options["labels"] = ":Person"

spark_write(SparkConnector.NEO4J, df, "Overwrite", options=options)