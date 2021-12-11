# Performance tuning

### 1. Use Columnar format when Caching

When you are caching data from Dataframe/SQL, use the **in-memory columnar** format. When you perform Dataframe/SQL operations on columns, Spark retrieves only required columns which result in fewer data retrieval and less memory usage.

You can enable Spark to use in-memory columnar storage by setting `spark.sql.inMemoryColumnarStorage.compressed`configuration to`true`.

```
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", true)
```

### 2. Spark Cost-Based Optimizer

When you are working with multiple joins, use Cost-based Optimizer as it improves the query plan based on the table and columns statistics.

This is enabled by default, In case if this is disabled, you can enable it by setting spark.sql.cbo.enabled to true

```
spark.conf.set("spark.sql.cbo.enabled", true)
```

### 3. Prior running the join queries run "ANALYZE_TABLE"

Prior to your Join query, you need to run ANALYZE TABLE command by mentioning all columns you are joining. This command collects the statistics for tables and columns for a cost-based optimizer to find out the best query plan.

```python
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1,col2
```

### 4. Use Optimal value for Shuffle Partitions

When you perform an operation that triggers data shuffle (like Aggregates and Joins), Spark by default creates 200 partitions. This is because of `spark.sql.shuffle.partitions` configuration property set to `200`.

```
spark.conf.set("spark.sql.shuffle.partitions", 50)
```

Spark does't know the optimal partition size to use, post shuffle operation and sets it to 200. Most of the times this value will cause performance issues hence, change it based on the data size.

Data ~~ `spark.sql.shuffle.partitions`

Huge data will require higher number for this configuration.

Lower data will require lower number for this configuration.

### 5. Use Broadcast Join when your Join data can fit in memory

Among all different Join strategies available in Spark, broadcast hash join gives a greater performance. This strategy can be used only when one of the joins tables small enough to fit in memory within the broadcast threshold.

To use the broadcast join feature, we have to wrap the broadcasted DataFrame using the broadcast function:

```
from pyspark.sql.functions import broadcast

data_frame.join(
    broadcast(lookup_data_frame),
    lookup_data_frame.key_column==data_frame.key_column
)
```

You can increse the threshold size if your data is big and use the below configuration to do so.

```
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10485760) //10 MB by default
```

#### Automatically Using the Broadcast Join

In order to make spark the automatically broacast joins below steps are required to be performed:

1. Data source provides statistics :
   ```
   ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan
   ```
2. Configure `spark.sql.autoBroadcastJoinThreshold`

### 6. Spark 3.0 (Spark-sql) imporvements

#### a. Using coaleasce and repartition on SQL (hints)

While working with Spark SQL query, you can use the COALESCE, REPARTITION and REPARTITION_BY_RANGE within the query to increase and decrease the partitions based on your data size.

```
SELECT /*+ COALESCE(3) */ * FROM EMP_TABLE
SELECT /*+ REPARTITION(3) */ * FROM EMP_TABLE
SELECT /*+ REPARTITION(c) */ * FROM EMP_TABLE
SELECT /*+ REPARTITION(3, dept_col) */ * FROM EMP_TABLE
SELECT /*+ REPARTITION_BY_RANGE(dept_col) */ * FROM EMP_TABLE
SELECT /*+ REPARTITION_BY_RANGE(3, dept_col) */ * FROM EMP_TABLE
```

#### b. Enable Adaptive Query Execution

Re-optimizing the query plan during runtime with the statistics it collects after each stage completion.

```
spark.conf.set("spark.sql.adaptive.enabled",true)
```

#### c. Coalescing Post Shuffle Partitions

Spark dynamically determines the optimal number of partitions by looking at the metrics of the completed stage. In order to use this, you need to enable the below configuration.

```
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",true)
```

#### d. Optimizing Skew Join

Sometimes we may come across data in partitions that are not evenly distributed, this is called Data Skew. Operations such as join perform very slow on this partitions. By enabling the AQE, Spark checks the stage statistics and determines if there are any Skew joins and optimizes it by splitting the bigger partitions into smaller (matching partition size on other table/dataframe).

```
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",true)
```

### Summary


| Configuration | Value | Usage |
| - | - | - |
| `spark.sql.inMemoryColumnarStorage.compressed` | `true` | caches data in columnar format |
| `spark.sql.cbo.enabled` | `true` | improves query plans for multiple joins after running`ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1,col2` |
| `spark.sql.shuffle.partitions` | `int` | Increase in case of huge data decrease in case of lesser |
| `spark.sql.autoBroadcastJoinThreshold` | `> 10 MB` | Increase from 10 MBs if the datasets are bigger in size. |
| `spark.sql.adaptive.enabled` | `true` | Spark 3.0 runtime optimization of query |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Spark 3.0 runtime decrease in partitions after aggregations (data shuffles) |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Spark 3.0 optimizes bigger parititions into smaller ones |
