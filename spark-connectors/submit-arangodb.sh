#!/usr/bin/env bash

/spark/bin/spark-submit \
    --jars $(echo /connectors/arangodb/jars/*.jar | tr ' ' ',') \
    --master spark://spark-master:7077 /connectors/arangodb/arangodb.py