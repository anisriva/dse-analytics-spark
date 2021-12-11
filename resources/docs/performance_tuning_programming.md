# Performance tuning using programming best practices

### 1. Use DataFrame/Dataset over RDD

> #### Why not RDD?

Spark RDD is a building block of Spark programming, even when we use DataFrame/Dataset, Spark internally uses RDD to execute operations/queries but the efficient and optimized way by analyzing your query and creating the execution plan thanks to Project Tungsten and Catalyst optimizer.

Using RDD directly leads to performance issues as Spark doesn't know how to apply the optimization techniques and RDD serialize and de-serialize the data when it distributes across a cluster (repartition & shuffling).

Serialization and de-serialization are very expensive operations for Spark applications or any distributed systems, most of our time is spent only on serialization of data rather than executing the operations hence try to avoid using RDD.

> #### Why DataFrames?

Since Spark DataFrame maintains the structure of the data and column types (like an RDMS table) it can handle the data better by storing and managing more efficiently.

The DataFrame API does two things that help to do this (through the Tungsten project):

1. First, using off-heap storage for data in binary format.
2. Second, generating encoder code on the fly to work with this binary format for your specific objects.

### 2. Using coleasce() over repartition()

Use `repartition()` only when increasing the number of partitions.

When you want to reduce the number of partitions prefer using `coalesce()` as it is an optimized or improved version of `repartition()` where the movement of the data across the partitions is lower using coalesce which ideally performs better when you dealing with bigger datasets.

### 3. Use mapPartitions() over map()

Spark `map()`and `mapPartitions()` transformation applies the function on each element/record/row of the DataFrame/Dataset and returns the new DataFrame/Dataset. `mapPartitions()` over `map()` prefovides performance improvement when you have havy initializations like initializing classes, database connections e.t.c

Spark `mapPartitions()` provides a facility to do heavy initializations (for example Database connection) once for each partition instead of doing it on every DataFrame row. This helps the performance of the Spark jobs when you dealing with heavy-weighted initialization on larger datasets.

**map()**

```python
import spark.implicits._
  val df3 = df2.map(row=>{
    val util = new Util() // Initialization happends for every record
    val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
    (fullName, row.getString(3),row.getInt(5))
  })
  val df3Map =  df3.toDF("fullName","id","salary")
```

**mapPartitions()**

```python
  val df4 = df2.mapPartitions(iterator => {
    val util = new Util()
    val res = iterator.map(row=>{
      val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
      (fullName, row.getString(3),row.getInt(5))
    })
    res
  })
  val df4part = df4.toDF("fullName","id","salary")
```

### 4. Use Serialized data format's

When you have such use case, prefer writing an intermediate file in Serialized and optimized formats like Avro, Kryo, Parquet e.t.c

**Parquet :** Apache Parquet is a columnar file format that provides optimizations to speed up queries and is a far more efficient file format than CSV or JSON, supported by many data processing systems.

It is compatible with most of the data processing frameworks in the Hadoop echo systems. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk.

```python
    # read parquet file
    val dF = spark.read.parquet(“/tmp/output/people.parquet”)
    # write parquet file
    df.write.parquet(“/tmp/output/people-new.parquet”)
```

**Avro :** Apache Avro is an open-source, row-based, data serialization and data exchange framework for Hadoop projects, originally developed by databricks as an open-source library that supports reading and writing data in Avro file format. it is mostly used in Apache Spark especially for Kafka-based data pipelines. When Avro data is stored in a file, its schema is stored with it, so that files may be processed later by any program.

It has build to serialize and exchange big data between different Hadoop based projects. It serializes data in a compact binary format and schema is in JSON format that defines the field names and data types.

```python
# read from avro file
val df = spark.read.format("avro").load("person.avro")
# write from avro file
df.write.format("avro").save("person-new.avro")
# avro sql
spark.sqlContext.sql("CREATE TEMPORARY VIEW PERSON USING avro 
    OPTIONS (path \"person.avro\")")
spark.sqlContext.sql("SELECT * FROM PERSON").show()
```

### 5. Avoid using UDF's

Try to avoid Spark/PySpark UDF’s at any cost and use when existing Spark built-in functions are not available for use. UDF’s are a black box to Spark hence it can’t apply optimization and you will lose all the optimization Spark does on Dataframe/Dataset. When possible you should use Spark SQL built-in functions as these functions provide optimization.

### 6. Persisting & Caching data in memory

Using `cache()` and `persist()` methods, Spark provides an optimization mechanism to store the intermediate computation of a Spark DataFrame so they can be reused in subsequent actions.

When you persist a dataset, each node stores it’s partitioned data in memory and reuses them in other actions on that dataset. And Spark’s persisted data on nodes are fault-tolerant meaning if any partition of a Dataset is lost, it will automatically be recomputed using the original transformations that created it.

```
Storage Level    Space used  CPU time  In memory  On-disk  Serialized   Recompute some partitions
----------------------------------------------------------------------------------------------------
MEMORY_ONLY          High        Low       Y          N        N         Y  
MEMORY_ONLY_SER      Low         High      Y          N        Y         Y
MEMORY_AND_DISK      High        Medium    Some       Some     Some      N
MEMORY_AND_DISK_SER  Low         High      Some       Some     Y         N
DISK_ONLY            Low         High      N          Y        Y         N
```

### 7. Reduce expensive Shuffle

Shuffling is a mechanism Spark uses to redistribute the data across different executors and even across machines. Spark shuffling triggers when we perform certain transformation operations like `gropByKey()`, `reducebyKey()`, `join()` on RDD and DataFrame.

Spark Shuffle is an expensive operation since it involves the following

* Disk I/O
* Involves data serialization and deserialization
* Network I/O

We cannot completely avoid shuffle operations in but when possible try to reduce the number of shuffle operations removed any unused operations.

Spark provides `spark.sql.shuffle.partitions` configurations to control the partitions of the shuffle, By tuning this property you can improve Spark performance.

### 8. Disable DEBUG and INFO logging

This is one of the simple ways to improve the performance of Spark Jobs and can be easily avoided by following good coding principles. During the development phase of Spark/PySpark application, we usually write debug/info messages to console using `println()` and logging to a file using some logging framework (log4j);

These both methods results I/O operations hence cause performance issues when you run Spark jobs with greater workloads. Before promoting your jobs to production make sure you review your code and take care of the following.
