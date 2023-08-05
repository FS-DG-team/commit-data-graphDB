import pyspark as spark

session = spark.sql.SparkSession.builder \
    .appName("Spark RedisGraph") \
    .getOrCreate()

data = [(1, "John"),
        (2, "Alice"),
        (3, "Bob")]

# Create a DataFrame from the sample data
columns = ["id", "name"]
df = session.createDataFrame(data, columns)

# Write to ArangoDB
df.write \
    .format("org.apache.spark.sql.redis")\
    .option("host", "redisgraph")\
    .option("port", "6379")\
    .option("table", "foo")\
    .save()
