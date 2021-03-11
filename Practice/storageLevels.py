from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark import shuffle

conf = SparkConf().setAppName("myapp").setMaster("local[*]")
sc = SparkContext(conf = conf)
# sc.setLogLevel("DEBUG")

lines = sc.textFile("E:\Spark\PySpark\SparkByExample\data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
print(lineLengths.collect())

lineLengths.persist(StorageLevel.MEMORY_ONLY)

print(totalLength)

print(lineLengths.collect())