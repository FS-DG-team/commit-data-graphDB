#!/usr/bin/env bash

/spark/bin/spark-submit \
    --jars $(echo /connectors/tigergraph/jars/*.jar | tr ' ' ',') \
    --master spark://spark-master:7077 connectors/tigergraph/tigergraph.py