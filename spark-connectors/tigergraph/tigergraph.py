import sys

sys.path.append("/connectors")
from utilities import *

session = create_spark_session("TigerGraph", SparkConnector.TIGERGRAPH)

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

options = get_default_options(SparkConnector.TIGERGRAPH)

options["dbtable"] = "vertex Person"

spark_write(SparkConnector.TIGERGRAPH, df, "Overwrite", options=options)


# https://docs.tigergraph.com/cloud/solutions/access-solution/jdbc
