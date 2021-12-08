# Partitioning

Resilient Distributed Datasets are collection of various data items that are so huge in size, that they cannot fit into a single node and have to be partitioned across various nodes. Spark automatically partitions RDDs and distributes the partitions across different nodes.

Partitioning allows spark to execute transformations on multiple partitions in parallel which allows completing the job faster. You can also write partitioned data into a file system (multiple sub-directories) for faster reads by downstream systems.

![partitions_1](images/part1.png)

### **Characteristics of Partitions in Apache Spark**

* Every machine in a spark cluster contains one or more partitions.
* The number of partitions in spark are configurable and having too few or too many partitions is not good.
* By default, it is set to the total number of cores on all the executor nodes.
* Data of each partition resides in a single machine and partitions in Spark do not span multiple machines.
* Spark/PySpark creates a task for each partition.Spark assigns one task per partition and each worker can process one task at a time.

### Spark Configuration

1. `spark.default.parallelism` : is the default number of partitions in `RDD`s returned by transformations like `join`, `reduceByKey`, and `parallelize` when not set explicitly by the user.configuration default value set to the number of cores on all nodes in a cluster, on local it is set to a number of cores on the system seems to only be working for raw `RDD` and is ignored when working with dataframes.

   If the task you are performing is not a join or aggregation and you are working with dataframes then setting these will not have any effect. You could, however, set the number of partitions yourself by calling `df.repartition(numOfPartitions)`
2. `spark.sql.shuffle.partitions` : configures the number of partitions that are used when shuffling data for joins or aggregations.configuration.

To change the settings in your code you can simply do:

```
sqlContext.setConf("spark.sql.shuffle.partitions", "300")
sqlContext.setConf("spark.default.parallelism", "300")
```

Alternatively, you can make the change when submitting the job to a cluster with `spark-submit`:

```
./bin/spark-submit --conf spark.sql.shuffle.partitions=300 --conf spark.default.parallelism=300
```


### **Types of Partitioning in Apache Spark**

1. **HashPartitioning** : Hash Partitioning attempts to spread the data evenly across various partitions based on the key. Object.hashCode method is used to determine the partition in Spark as partition = key.hashCode () % numPartitions.
2. **Range** : Range partitioning is an efficient partitioning technique. In range partitioning method, tuples having keys within the same range will appear on the same machine. Keys in a range partitioner are partitioned based on the set of sorted range of keys and ordering of keys.

### Mode of Partitioning in Apache Spark

Spark/PySpark supports partitioning in memory (RDD/DataFrame) and partitioning on the disk (File system).

1. **Partition in memory:** You can partition or repartition the DataFrame by calling `repartition()` or `coalesce()`transformations.

   ```bash
   newDF=df.repartition(10)
   ```

   ```bash
   rdd3 = rdd1.coalesce(4)
   ```
2. **Partition on disk:** While writing the PySpark DataFrame back to disk, you can choose how to partition the data based on columns by using `partitionBy()` of `pyspark.sql.DataFrameWriter`.

### Shuffling of data in spark

The Spark SQL shuffle is a mechanism for redistributing or re-partitioning data so that the data grouped differently across partitions, based on your data size you may need to reduce or increase the number of partitions of RDD/DataFrame using `spark.sql.shuffle.partitions` configuration or through code.

Spark shuffle is a very expensive operation as it moves the data between executors or even between worker nodes in a cluster so try to avoid it when possible. When you have a performance issue on Spark jobs, you should look at the Spark transformations that involve shuffling.

Spark shuffling triggers for transformation operations like:

1. **RDD** : `gropByKey()`, `reducebyKey()`, `join()`, `union()`, `groupBy()`
2. **DataFrames** : `join()`, `union()`, all aggregate functions

Spark Shuffle is an expensive operation since it involves the following

* Disk I/O
* Involves data serialization and deserialization
* Network I/O

### Choosing partitions

When using partitionBy(), you have to be very cautious with the number of partitions it creates, as having too many partitions creates too many sub-directories in a directory which brings unnecessarily and overhead to NameNode (if you are using Hadoop) since it must keep all metadata for the file system in memory.

Let’s assume you have a US census table that contains zip code, city, state, and other columns. Creating a partition on the state, splits the table into around 50 partitions, when searching for a zipcode within a state (state=’CA’ and zipCode =’92704′) results in faster as it needs to scan only in a **state=CA** partition directory.

Partition on zipcode may not be a good option as you might end up with too many partitions.

Another good example of partition is on the Date column. Ideally, you should partition on Year/Month but not on a date.

## 6. Too Many Partitions Good?

# Spark Submit

## Launching Applications with spark-submit

```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

* `--class`: The entry point for your application (e.g. `org.apache.spark.examples.SparkPi`)
* `--master`: The [master URL](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) for the cluster (e.g. `spark://23.195.26.187:7077`)
* `--deploy-mode`: Whether to deploy your driver on the worker nodes (`cluster`) or locally as an external client (`client`) (default: `client`) **†**
* `--conf`: Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes (as shown). Multiple configurations should be passed as separate arguments. (e.g. `--conf <key>=<value> --conf <key2>=<value2>`)
* `application-jar`: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an `hdfs://` path or a `file://` path that is present on all nodes.
* `application-arguments`: Arguments passed to the main method of your main class, if any
* `--supervise` to make sure that the driver is automatically restarted if it fails with a non-zero exit code.

```bash

# Run application locally on 8 cores
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local[8] \
  /path/to/examples.jar \
  100

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000

# Run on a YARN cluster in cluster deploy mode
export HADOOP_CONF_DIR=XXX
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 20G \
  --num-executors 50 \
  /path/to/examples.jar \
  1000

# Run a Python application on a Spark standalone cluster
./bin/spark-submit \
  --master spark://207.184.161.138:7077 \
  examples/src/main/python/pi.py \
  1000

# Run on a Mesos cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 20G \
  --total-executor-cores 100 \
  http://path/to/examples.jar \
  1000

# Run on a Kubernetes cluster in cluster deploy mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master k8s://xx.yy.zz.ww:443 \
  --deploy-mode cluster \
  --executor-memory 20G \
  --num-executors 50 \
  http://path/to/examples.jar \
  1000
```

## Master URLs


| Master URL | Meaning |
| - | - |
| `local` | Run Spark locally with one worker thread (i.e. no parallelism at all). |
| `local[K]` | Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine). |
| `local[K,F]` | Run Spark locally with K worker threads and F maxFailures (see[spark.task.maxFailures](https://spark.apache.org/docs/latest/configuration.html#scheduling) for an explanation of this variable). |
| `local[*]` | Run Spark locally with as many worker threads as logical cores on your machine. |
| `local[*,F]` | Run Spark locally with as many worker threads as logical cores on your machine and F maxFailures. |
| `local-cluster[N,C,M]` | Local-cluster mode is only for unit tests. It emulates a distributed cluster in a single JVM with N number of workers, C cores per worker and M MiB of memory per worker. |
| `spark://HOST:PORT` | Connect to the given[Spark standalone cluster](https://spark.apache.org/docs/latest/spark-standalone.html) master. The port must be whichever one your master is configured to use, which is 7077 by default. |
| `spark://HOST1:PORT1,HOST2:PORT2` | Connect to the given[Spark standalone cluster with standby masters with Zookeeper](https://spark.apache.org/docs/latest/spark-standalone.html#standby-masters-with-zookeeper). The list must have all the master hosts in the high availability cluster set up with Zookeeper. The port must be whichever each master is configured to use, which is 7077 by default. |
| `mesos://HOST:PORT` | Connect to the given[Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html) cluster. The port must be whichever one your is configured to use, which is 5050 by default. Or, for a Mesos cluster using ZooKeeper, use `mesos://zk://...`. To submit with `--deploy-mode cluster`, the HOST:PORT should be configured to connect to the [MesosClusterDispatcher](https://spark.apache.org/docs/latest/running-on-mesos.html#cluster-mode). |
| `yarn` | Connect to a[YARN ](https://spark.apache.org/docs/latest/running-on-yarn.html)cluster in `client` or `cluster` mode depending on the value of `--deploy-mode`. The cluster location will be found based on the `HADOOP_CONF_DIR` or `YARN_CONF_DIR` variable. |
| `k8s://HOST:PORT` | Connect to a[Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) cluster in `client` or `cluster` mode depending on the value of `--deploy-mode`. The `HOST` and `PORT` refer to the [Kubernetes API Server](https://kubernetes.io/docs/reference/generated/kube-apiserver/). It connects using TLS by default. In order to force it to use an unsecured connection, you can use `k8s://http://HOST:PORT`. |
