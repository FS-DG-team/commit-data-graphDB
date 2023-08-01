#!/usr/bin/env bash

/spark/bin/spark-submit \
    --jars /connectors/neo4j-connector-apache-spark_2.12-4.1.5_for_spark_3.jar \
    --master spark://spark-master:7077 connectors/neo4j.py