# Commit-data-graphDB
Final project for a university course on 'Big Data.' The project's objective is to compare various Graph Database Management System (GDBMS) solutions in terms of usability and time performance within the domain of GitHub data analysis. This involves defining different analytical scenarios and utilizing different batches of the original Big Query dataset.
The entire project is built within a Dockerized environment to ensure isolation and the potential for future scalability to a cluster of nodes with fault tolerance. Additionally, this project incorporates several technologies, including Spark for preprocessing, Neo4j, TigerGraph, and ArangoDB. 

[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/big-data-europe/Lobby)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository is directly forked and inspired from [Big Data Europe repositories](https://github.com/big-data-europe)

Docker Compose containing:
* [Apache Spark](https://spark.apache.org/) cluster running one Spark Master and multiple Spark workers
* [Hadoop](https://hadoop.apache.org/) HDFS cluster
* [Neo4j ™](https://neo4j.com/) Neo4j graph database soluction 1
* [TigerGraph ™](https://www.tigergraph.com/) TigerGraph graph database soluction 2
* [ArangoDB ™](https://www.arangodb.com/) ArangoDB graph database soluction 2
* [Jupyter Lab service](https://jupyter.org/) to test PySpark jobs

## Running Docker containers 

To start the docker big data playground repository:

    cd docker-bigdata-playground
    docker-compose up

### Example load data into HDFS

Move the dataset files into the path /datasets.

Log into the container and put the file into HDFS:

    $ docker-compose exec spark-master bash
    > ./scripts/exec-load-data.sh

### Run example Big Data Pipeline in Jupyter Notebook

Connect to Jupyter Hub by accessing container logs:

    $ docker logs jupyter-notebooks

    > 2023-05-03 17:29:39     To access the server, open this file in a browser:
    > 2023-05-03 17:29:39         file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
    > 2023-05-03 17:29:39     Or copy and paste one of these URLs:
    > 2023-05-03 17:29:39         http://083d9da0d714:8888/lab?token=686167f3cee298e578315d50990c397ffd09b75cb5705cf3
    > 2023-05-03 17:29:39      or http://127.0.0.1:8888/lab?token=686167f3cee298e578315d50990c397ffd09b75cb5705cf3

Click on the last line in the logs, enter Jupyter Hub in your brower and follow instructions in the notebooks/BigDataPipeline.ipynb

<h1>Proposed Big Data Pipeline</h1>
<p align="center"><img src="/plot_results/architecture.png" width="700" /></p>
<h1>Docker Architecture</h1>
<p align="center"><img src="/plot_results/docker_architecture.png" width="700" /></p>
<h1>Graph Schema Adopted</h1>
<p align="center"><img src="/plot_results/graph_schema.png" width="700" /></p>
<h1>Community Detection for the Scenario 4</h1>
<p align="center">
   <img src="/plot_results/OpenOrdLabelProp_10.png" width="500" />
   <img src="/plot_results/OpenOrdLabelProp_100.png" width="500" />
</p>
<p align="center">
   <img src="/plot_results/OpenOrdLabelProp_1000.png" width="500" />
   <img src="/plot_results/OpenOrdLabelProp_2000.png" width="500" />
</p>
<h1>Performance Results</h1>
<p align="center"><img src="/plot_results/wtime.png" width="700" /></p>
<p align="center"><img src="/plot_results/scenario1.png" width="700" /></p>
<p align="center"><img src="/plot_results/scenario2.png" width="700" /></p>
<p align="center"><img src="/plot_results/scenario3.png" width="700" /></p>
<p align="center"><img src="/plot_results/scenario4.png" width="700" /></p>
<p align="center"><img src="/plot_results/scenario5.png" width="700" /></p>
