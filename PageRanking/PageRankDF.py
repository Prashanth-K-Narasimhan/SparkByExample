from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, array
import re

from pyspark.sql.functions import udf

'''
@udf("long") or squared_udf = udf(squared, LongType()) can be use to declare a udf with return type long
'''
@udf()
def extractURLs(text):
    pattern = "(?<=\\[\\[)(.*?)(?=\\])"
    return re.findall(pattern=pattern, string=text)

'''
Adding external jar to Pyspark during runtime, opposed to --spark-jar during Spark-submit
'''
conf = SparkConf().set("spark.jars", "../spark-xml_2.12-0.11.0.jar")
Spark = SparkSession.builder.config(conf=conf).appName("PageRankDF").getOrCreate()

df = Spark.read.format('com.databricks.spark.xml').option("rowTag","page").load("../Data\Wikipedia-20210310051747.xml")

getPages = df.select("title","revision.text._VALUE")
'''
    _VALUE is used coz the xml unpacks the list as "pyspark.sql.utils.AnalysisException: No such struct field test in _VALUE, _bytes, _space;"
    FYI : "revision.text._bytes","revision.text._space" also has data in them.
'''
extractLinks = getPages.withColumn("links", extractURLs(F.col('_VALUE')))
    # .withColumn("url", F.explode(F.col('links').collect()))

extractLinks.show()

# extractLinks = getPages.select(extractURLs(F.col('_VALUE')).alias("value"), F.explode(array(F.col('_VALUE'))))
# output = extractLinks.select("links").collect()
# print(output)