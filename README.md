# Using DSE Analytics

## Starting up the cluster

Use the start.sh script to start a cluster of 2 datacenters.

### Normal Start

```bash
sh start.sh
```

### Clean Start

```bash
sh start.sh fresh
```

Note: This will clean up all the saved data.

## Using DSE Studio

Click on below link to access the dse-studio

[DSE-Studio](http://localhost:8080/)

## Starting up Jupyter Notebook

### Enter the analytics container

```bash
docker container exec -it analytics-seed bash
```

### Enter the work directory

```bash
cd /var/lib/spark/jupyter
```

### Start Jupyter Notebook

```bash
nohup jupyter notebook --ip=analytics-seed --port=8888 --NotebookApp.token='' --NotebookApp.password='' &

exit
```

## Using the spark shell

### Spark-sql

```bash
docker container exec -it analytics-seed dse spark-sql
```

### Spark-scala

```bash
docker container exec -it analytics-seed dse spark
```

### Spark-pysark

```bash
docker container exec -it analytics-seed dse pyspark
```
