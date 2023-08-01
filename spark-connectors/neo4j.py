import pyspark as spark
import os

credentials = os.environ["NEO4J_AUTH"]
username, password = credentials.split("/")


url = "bolt://neo4j:7687"

# print(f"username: {username}, password: {password}")

session = spark.sql.SparkSession.builder \
    .appName("Spark Neo4j") \
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
df.write.format("org.neo4j.spark.DataSource") \
    .mode("Overwrite") \
    .option("url", url) \
    .option("authentication.basic.username", username) \
    .option("authentication.basic.password", password) \
    .option("node.keys", "id") \
    .option("labels", ":Person") \
    .save()
