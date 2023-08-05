#!/usr/bin/env bash

/spark/bin/spark-submit \
    --jars /connectors/spark-redis_2.12-3.1.0.jar \
    --master spark://spark-master:7077 connectors/redisgraph.py