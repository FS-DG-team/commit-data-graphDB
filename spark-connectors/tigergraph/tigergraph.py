import pyspark as spark
import os

username, password = "tigergraph", "tigergraph"


url = "jdbc:tg:http://tigergraph:14240"

# print(f"username: {username}, password: {password}")

session = spark.sql.SparkSession.builder \
    .appName("Spark TigerGraph") \
    .config("spark.driver.extraClassPath", "/connectors/tigergraph/jars/postgresql-42.5.0.jar:/connectors/tigergraph/jars/tigergraph-jdbc-driver-1.3.6.jar") \
    .getOrCreate()

# hdfs_path = "/dado/commits.json"
# df = spark.read.json(f"hdfs://namenode:9000{hdfs_path}")

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

df.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("driver", "com.tigergraph.jdbc.Driver") \
    .option("url", url) \
    .option("user", username) \
    .option("password", password) \
    .option("graph", "Test") \
    .option("dbtable", "vertex Persone") \
    .option("debug", "0") \
    .save()

# https://docs.tigergraph.com/cloud/solutions/access-solution/jdbc
