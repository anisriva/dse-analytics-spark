from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("delimited")
sc = SparkContext(conf=conf)

data = "Hello World in spark looks like this.".split()
rdd_data = sc.parallelize(data)

for c in rdd_data.collect():
    print(c)

sc.stop()