# Spark Basics : RDDs,Stages,Tasks and DAG


Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark. It is an immutable distributed collection of objects.

**RDDs**

> *RDD(***Resilient***,Distributed,Dataset) is immutable distributed collection of objects.***RDD** is a logical reference of a `dataset` which is partitioned across many server machines in the cluster. **RDD**s are Immutable and are self recovered in case of failure.An RDD could come from any datasource, e.g. text files, a database via JDBC, etc.

*Creating an RDD*

```
val rdd = sc.textFile("/some_file",3)  
val lines = sc.parallelize(List("this is","an example"))
```

*the argument ‘3’ in the method call sc.textFile() specifies the number of partitions*

**Partitions**

RDD are a collection of various data if it cannot fit into a single node it should be partitioned across various nodes. So it means, the more the number of partitions, the more the parallelism. These partitions of an RDD is distributed across all the nodes in the network.

**RDDs Operations**(Transformations and Actions)

There are two types of operations that you can perform on an RDD- *Transformations and Actions*.

**Transformation** applies some function on a RDD and creates a new RDD, it does not modify the RDD that you apply the function on.*(Remember that RDDs are immutable).* Also, the new RDD keeps a pointer to it’s parent RDD.

**Transformations** are lazy operations on a RDD that create one or many new RDDs, e.g. `map`,`filter`, `reduceByKey`, `join`, `cogroup`, `randomSplit`

At high level, there are two transformations that can be applied onto the RDDs, namely **narrow transformation and wide transformation**. Wide transformations basically result in stage boundaries.

* **Narrow transformation** — doesn’t require the data to be shuffled across the partitions. for example, Map, filter etc..
* **wide transformation** — requires the data to be shuffled for example, reduceByKey etc..

By applying transformations you incrementally build a [RDD lineage](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-rdd-lineage.html) with all the parent RDDs of the final RDD(s).Transformations are lazy, i.e. are not executed immediately. Only after calling an action are transformations executed.

```
val rdd = sc.textFile("spam.txt")
val filtered = rdd.filter(line => line.contains("money"))
filtered.count()
```

*sc.textFile() and rdd.filter()* do not get executed immediately, it will only get executed once you call an *Action* on the RDD — here filtered.count(). An **Action** is used to either save result to some location or to display it. You can also print the RDD lineage information by using the command `filtered.toDebugString`*(filtered is the RDD here)*.

> RDDs can also be thought of as a set of instructions that has to be executed, first instruction being the load =instruction=.


![](https://miro.medium.com/max/945/1*nenmaK1oa7EL-KtuVtTS7A.png)


![](https://miro.medium.com/max/945/1*sb2123nXPFsf0w23E3l-AA.png)

**New RDD is created after every transformation.(DAG graph)**

## DAG(Directed Acyclic Graph),Stages and Tasks

**DAGScheduler** is the scheduling layer of Apache Spark that implements **stage-oriented scheduling**. It transforms a **logical execution plan** (i.e. [RDD lineage](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-rdd-lineage.html) of dependencies built using RDD transformations) to a **physical execution plan** (using stages).


![](https://miro.medium.com/max/945/1*gTr_q0m4VAg6fWjWJCOjMA.png)

`DAGScheduler` Transforming RDD Lineage(DAG) Into Stage DAG(Physical Execution Plan)

As mentioned above, the DAG scheduler splits the graph into multiple stages, the stages are created based on the transformations. The narrow transformations will be grouped (pipe-lined) together into a single stage.

```
val input = sc.textFile("log.txt")
val splitedLines = input.map(line => line.split(" "))
                        .map(words => (words(0), 1))
                        .reduceByKey{(a,b) => a + b}
```

above example will create 2 stages.

![](https://miro.medium.com/max/945/1*1WfneX6c7Lc9fqAaR9MaGA.png)

Shuffled RDD is created by reduceByKey wide transforamtion.

Shuffle Operation or wide transformation define the boundary of 2 stages.Stages are separated by 2 shuffle operations.

The DAG scheduler will then submit the stages into the task scheduler. The number of tasks submitted depends on the number of partitions present in the textFile. Fox example consider we have 4 partitions in this example, then there will be 4 set of tasks created and submitted in parallel provided there are enough slaves/cores.


![](https://miro.medium.com/max/945/1*2bdRFvxGs7baeKHDk-Z0sA.png)

The stages that are not interdependent may be submitted to the cluster for execution in parallel: this maximizes the parallelization capability on the cluster. So if operations in our dataflow can happen simultaneously we will expect to see multiple stages launched.

```
val sfi  = sc.textFile("/data/blah/input").map{ x => val xi = x.toInt; (xi,xi*xi) }
val sp = sc.parallelize{ (0 until 1000).map{ x => (x,x * x+1) }}
val spj = sfi.join(sp)
val sm = spj.mapPartitions{ iter => iter.map{ case (k,(v1,v2)) => (k, v1+v2) }}
val sf = sm.filter{ case (k,v) => v % 10 == 0 }
sf.saveAsTextFile("/data/blah/out")
```


![](https://miro.medium.com/max/945/1*yf3cMmPT4SrXdCp-B_zN5Q.png)

Stage 0 and Stage 1 executes in parallel to each other as they are not inter-dependent.

Stage 2 (join operation) depends on stage 0 and stage 1 so it will be executed after executing both the stages.

The follow-on operations working on the joined data may be performed in the *same* stage because they must happen sequentially. There is no benefit to launching additional stages because they can not start work until the prior operation were completed.