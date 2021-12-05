# Spark Basics

* Its a fast and general engine for large-scale data processing
* Divides the data by splitting it up in multiple nodes
* Uses
  * Driver Program (Spark Context / Spark Session)
    * Cluster Manager (Spark, Yarn, Mesos, kubernetes)
      * Executor
        * Caches
        * Tasks
* Performs lazy evaluation of the workloads which makes it efficient and faster.
* Runs 100x faster than map reduce
* Uses DAG engine for optimized workflows
* Code in Python, Java or Scala
* Built around concept of RDD

## Why Python?

* Pros
  * No need to compile, manage dependencies.
  * Less coding overhead.
  * Time spent on learning the new language is completely ruled out.
  * Python code in Spark looks a lot like Scala code
* Cons
  * Scalay is native language for spark so its the first language of choice
  * New features tend to realease in spark earlier than pyspark

## Resilient Distributed Dataset (RDD)

* Its the core of everything that spark does, the other extensions / libraries of spark like spark sql and mllib also uses RDD under the hood.
* RDD is simply a dataset.
* Its distributed across the cluster.
* Its created by a driver program (Spark context)
* Spark Context is responsible for creating RDDs
* sc objects are created automatically in spark shells

### Common Transformations:

* map
* flatmap
* filter
* distinct
* sample
* union, intersection, subtract, cartesian

### Common Actions:

* collect
* count
* countByValue
* take
* top
* reduce

Nothing actually happens in the driver program until an action is called!

### Data Frame Api:

A DataSet of Row

**Dataframes:**

1. Contains Row objects
2. Can run SQL queries
3. Can have a schema (efficient storage)
4. Read and write to JSON, Hive, Parquet, csv, cassandra
5. Communicated with JDBC/ODBC etc

**Using DF Api:**

1. Can write simple sql queries and work on huge datasets
2. The same logic can be emulated in the spark df api way as well
3. Can be converted back to rdd to use the rdd functions
4. The trend in Spark is to use RDD's less and DF more.
5. Allows better interoperability for MLLib and SPark Streaming
6. Simplifies Development

### DataSet Api:

1. DF is really a DataSet of row objects
2. Can wrap known, typed data too but its transparent in python and it works better in Scala.
3. In scala use DataSet whenever possible

### UDF

User-Defined Functions (UDFs) are user-programmable routines that act on one row.

ex:

```python
from pypsark.sql.types import IntegerType

def square(x):
    return x*x

spark.udf.register("square", square, IntegerType())
df = spark.sql("SELECT square('numeric_feild') from table")
```

or

```python
from pyspark.sql import functions as f

name_dict = {id:value}

def lookup_name(id):
    return name_dict.value[id]

lookup_name_udf = f.udf(lookup_name)
```

## Broadcast

* Broadcast objects to the executors such that they're always there whenever needed
* sc.broadcast() to ship off whatever you want (sparkContext is the part of the spark session as well)
* <broadcast_name>.value() to get the object back
* Only good when working with small datasets
  ```python
  map_dict = {"key1" :"value1", "key2":"value2"}
  map_dict_broadcast = spark.sparkContext.broadcast(map_dict)
  ```

## Partitioning

* Partitioning a huge data is really important beacause it will blow up all the executors due to OOM issue.
* Spark does not automatically spread out the work of the job throughout the cluster but it should be dealt with it manually.
* Partitioning splits up a job into different executors.
* Use .pratitionBy() on an RDD before running a large operation that benefits from partitioning.
  * join(), cogroup(), groupWith(), join(), leftOuterJoin(), rightOuterJoin(), groupByKey(), reduceByKey(), combineByKey(), and lookup() these operations will preserve the partitioning in the result as well.
* Too few partitions wont take full advantage of the cluster.
* Too many partitions results in too much overhead from shuffling data.
* At least as many partitions as cores, or executors that fit within the available memory.
* partitionBy(100) is usually a reasonable place to start for large operations.
