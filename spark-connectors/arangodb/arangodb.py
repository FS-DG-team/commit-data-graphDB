import sys

sys.path.append("/connectors")
from utilities import *

session = create_spark_session("ArangoDB", SparkConnector.ARANGO)

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

options = get_default_options(SparkConnector.ARANGO)
options["table"] = "Persone"

spark_write(SparkConnector.ARANGO, df, "Append", options=options)
