import collections
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("rddBasics")
sc = SparkContext(conf=conf)

file_path = "file:///var/lib/spark/jupyter/data/sample_kmeans_data.txt"

lines = sc.textFile(file_path)

second_col = lines.map(lambda x: x.split()[2].split(':')[0])

result = second_col.countByValue()

sorted_result = collections.OrderedDict(sorted(result.items()))

for key, value in sorted_result.items():
    print("{key}, {value}".format(key=key, value=value))

sc.stop()