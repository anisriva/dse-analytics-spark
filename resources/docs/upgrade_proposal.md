# Why should we upgrade to DSE 6.x (with spark in mind)

DSE 6.x comes with below upgrades:

1. Depriciation of Python2 and addition of Python3
2.

The upgrade of DSE Ecosystem brings in some updates in Spark as well.

Below are some changes are added in Spark:

1. MLlib RDD will be deprecated the new one will be DataFrame
2. Spark 3 has better performance and some benchmarks show results 17x faster than Spark2 with tricks like adaptive execution and dynamic partition pruning.
3. Python2 is deprecated and replaced by Python3.
4. Allows the access to GPUs allowing acceleration to deep learning.
5. Has better kubernetes integration.
6. Supports binary file integration.
7. Added SparkGraph supports Cipher Query Language which supports Graphs (Graphx)
