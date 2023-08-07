import pyspark as spark
import os

username, password = "root", "password"


url = "tigergraph:9000"

# print(f"username: {username}, password: {password}")

session = spark.sql.SparkSession.builder \
    .appName("Spark TigerGraph") \
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
    .option("dbtable", "vertex Account") \
    .option("debug", "1") \
    .option("trustStore", "trust.jks") \
    .option("trustStorePassword", "password") \
    .option("trustStoreType", "JKS") \
    .save()

# https://docs.tigergraph.com/cloud/solutions/access-solution/jdbc
