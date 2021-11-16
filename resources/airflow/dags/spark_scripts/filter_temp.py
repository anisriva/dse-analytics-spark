from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("filter_temp")

sc = SparkContext(conf=conf)

file_path = "file:///var/lib/spark/jupyter/data/1800.csv"

line_rdd = sc.textFile(file_path)

def parse_line(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temp = float(fields[3])*0.1*(9.0/5.0)+32.0
    return (station_id, entry_type, temp)

def filter_min(row):
    return "TMIN"==str(row[2])

row_rdd = line_rdd.map(parse_line)
all_min_temps = row_rdd.filter(filter_min)
kv_station_temp = all_min_temps.map(lambda x:(x[0],x[2]))
min_station_temp = kv_station_temp.reduceByKey(lambda x,y : min(x,y))
results = min_station_temp.collect()
for result in results:
    print(result[0],"\t{:.2f}F".format(result[1]))

sc.stop()