# Spark Basics

* Its a fast and general engine for large-scale data processing
* Divides the data by splitting it up in multiple nodes
* Uses
  * Driver Program (Spark Context / Spark Session)
    * Cluster Manager (Spark, Yarn)
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
