[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/big-data-europe/Lobby)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# Introduction to Docker Big Data Playground

This repository is directly forked and inspired from [Big Data Europe repositories](https://github.com/big-data-europe)

Docker Compose containing:
* [Apache Spark](https://spark.apache.org/) cluster running one Spark Master and multiple Spark workers
* [Hadoop](https://hadoop.apache.org/) HDFS cluster
* [Apache Hive ™](https://hive.apache.org/) distributed, fault-tolerant data warehouse system.
* [Jupyter Lab service](https://jupyter.org/) to test PySpark jobs

## Running Docker containers 

To start the docker big data playground repository:

    docker-compose up

### Example load data into HDFS

Copy a data file into the container:

    $ docker cp data/breweries.csv namenode:breweries.csv

Log into the container and put the file into HDFS:

    $ docker-compose exec spark-master bash
    > hdfs dfs -mkdir /data
    > hdfs dfs -mkdir /data/openbeer
    > hdfs dfs -mkdir /data/openbeer/breweries
    > hdfs dfs -put breweries.csv /data/openbeer/breweries/breweries.csv

### Example query HDFS from Spark

Go to http://localhost:8080 on your Docker host (laptop). Here you find the spark:// master address like:
  
    Spark Master at spark://5d35a2ea42ef:7077

Find the container ID of the spark master container, and connect to the spark scala shell:

    $ docker-compose exec spark-master bash
    # spark/bin/spark-shell --master spark://5d35a2ea42ef:7077

Inside the Spark scala shell execute this commands:

    > val df = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")
    > df.show()

### Example query from Hive

Load Data into
  
    $ docker-compose exec hive-server bash
    # /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
    > show databases;

    +----------------+
    | database_name  |
    +----------------+
    | default        |
    +----------------+
    1 row selected (0.335 seconds)

    > create database openbeer;
    > use openbeer;
    > CREATE EXTERNAL TABLE IF NOT EXISTS breweries(
          NUM INT,
          NAME CHAR(100),
          CITY CHAR(100),
          STATE CHAR(100),
          ID INT )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      STORED AS TEXTFILE
      location '/data/openbeer/breweries';

    > sleect * from breweries limit 10;

### Run example Big Data Pipeline in Jupyter Notebook

Connect to Jupyter Hub by accessing container logs:

    $ docker logs jupyter-notebooks

    > 2023-05-03 17:29:39     To access the server, open this file in a browser:
    > 2023-05-03 17:29:39         file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
    > 2023-05-03 17:29:39     Or copy and paste one of these URLs:
    > 2023-05-03 17:29:39         http://083d9da0d714:8888/lab?token=686167f3cee298e578315d50990c397ffd09b75cb5705cf3
    > 2023-05-03 17:29:39      or http://127.0.0.1:8888/lab?token=686167f3cee298e578315d50990c397ffd09b75cb5705cf3

Click on the last line in the logs, enter Jupyter Hub in your brower and follow instructions in the notebooks/BigDataPipeline.ipynb

# Other Info

## Expanding Docker Compose
Add the following services to your `docker-compose.yml` to increase spark worker nodes:
```yml
version: '3'
services:
  spark-worker-2:
    image: bde2020/spark-worker:3.3.0-hadoop3.3
    container_name: spark-worker-2
    depends_on:
      - spark-master
    ports:
      - "8082:8081"
    environment:
      - "SPARK_MASTER=spark://spark-master:7077"
```
