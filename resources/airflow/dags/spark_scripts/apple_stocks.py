from pyspark.sql import SparkSession, session
from cassandra.cluster import Cluster

create_table = '''CREATE TABLE IF NOT EXISTS "PortfolioDemo"."AppleStocks" 
(
    "Date" date, 
    "Open" text, 
    "High" text, 
    "Low" text, 
    "Close" text, 
    "Volume" text, 
    "Adj Close" text,
    PRIMARY KEY("Date","Open")
)WITH cdc=false;'''

cluster = Cluster(['analytics-seed', 'trans-seed'])

spark = SparkSession \
            .builder \
            .appName("AppleStocks") \
            .getOrCreate()

file_path = 'file:///var/lib/spark/jupyter/data/appl_stock.csv'

df = spark.read.format("csv").option("header","true").load(file_path)

session = cluster.connect()
session.execute(create_table)
cluster.shutdown()

df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="AppleStocks", keyspace="PortfolioDemo") \
    .save()

spark.stop()