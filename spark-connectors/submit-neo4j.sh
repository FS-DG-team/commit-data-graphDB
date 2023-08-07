#!/usr/bin/env bash

/spark/bin/spark-submit \
    --jars $(echo /connectors/neo4j/jars/*.jar | tr ' ' ',') \
    --master spark://spark-master:7077 connectors/neo4j/neo4j.py