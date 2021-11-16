from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('kv_friends')
sc = SparkContext(conf=conf)

file_path = "file:///var/lib/spark/jupyter/data/fakefriends.csv"

lines_rdd = sc.textFile(file_path)

def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

kv_rdd = lines_rdd.map(parse_line)

total_age_friends = kv_rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))

average_per_age = total_age_friends.mapValues(lambda x:x[0]*1.0/x[1])

results = average_per_age.collect()
for result in results:
    print(result)

sc.stop()