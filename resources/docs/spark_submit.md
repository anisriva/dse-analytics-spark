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
