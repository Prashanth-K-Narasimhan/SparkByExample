from pyspark.sql import SparkSession

Spark = SparkSession.builder.appName("PageRankDF").getOrCreate()

df = Spark.read.csv("..\Data\imdb_top_1000.csv", header=True, inferSchema=True)
df.printSchema()