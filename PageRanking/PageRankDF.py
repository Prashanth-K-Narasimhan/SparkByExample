from pyspark import SparkConf
from pyspark.sql import SparkSession
import re

'''
Adding external jar to Pyspark during runtime, opposed to --spark-jar during Spark-submit
'''

def extractURLs(text):
    pattern = "(?<=\\[\\[)(.*?)(?=\\])"
    return re.findall(pattern=pattern, string=text)

conf = SparkConf().set("spark.jars", "../spark-xml_2.12-0.11.0.jar")
Spark = SparkSession.builder.config(conf=conf).appName("PageRankDF").getOrCreate()

df = Spark.read.format('com.databricks.spark.xml').option("rowTag","page").load("../Data\Wikipedia-20210310051747.xml")

getText = df.select("title","revision.text._VALUE")
'''
    _VALUE is used coz the xml unpacks the list as "pyspark.sql.utils.AnalysisException: No such struct field test in _VALUE, _bytes, _space;"
    FYI : "revision.text._bytes","revision.text._space" also has data in them.
'''
getText.show()