from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hello_cassandra").getOrCreate()

file_path = "file:///var/lib/spark/jupyter/data/appl_stock.csv"

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.cassandra.connection.host", "analytics-seed") \
        .appName("Spark Cassandra Connector Example") \
        .getOrCreate()

apple_stocks_df = spark.read.format("csv").option("header","true").load(file_path)

stocks_df = spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace="PortfolioDemo", table="Stocks") \
            .load()

for row in stocks_df.collect():
    print("Stock {} has price {}".format(row[0], row[2]))

# apple_stocks_df.write \
#     .format("org.apache.spark.sql.cassandra") \
#     .mode("append") \
#     .options(table="AppleStocks", keyspace="PortfolioDemo") \
#     .save()

spark.stop()