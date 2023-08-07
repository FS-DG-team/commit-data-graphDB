import pyspark as spark
import os

username, password = "root", "password"


endpoints = "arangodb:8529"

# print(f"username: {username}, password: {password}")

session = spark.sql.SparkSession.builder \
    .appName("Spark ArangoDB") \
    .getOrCreate()

# hdfs_path = "/dado/commits.json"
# df = spark.read.json(f"hdfs://namenode:9000{hdfs_path}")

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

# Write to Neo4j
df.write.format("com.arangodb.spark") \
    .mode("Append") \
    .option("endpoints", endpoints) \
    .option("username", username) \
    .option("password", password) \
    .option("database", "_system") \
    .option("collection", "BD") \
    .option("table", "Persone") \
    .option("createCollection", "true") \
    .save()
