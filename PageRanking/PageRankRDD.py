from pyspark import SparkContext, SparkConf
import re

config = SparkConf()
config.setMaster("local").setAppName("PageRank")

sc = SparkContext(conf = config)

def StrtoTuple(lister):
    x = re.split("\\s+", lister)
    return x[0], x[1]

def push_to_neighbors(urls, rank):
    num_urls = len(urls)
    w = rank / num_urls
    return [(n, w) for n in urls]

lines = sc.parallelize(["A B","A C","A D","B A","B C","C D","E B"])
'''
lambda functions are expressions that can return only one object at a time.
 this can be a tuple, list or string. But not 2 or more of a String etc.
'''
links = lines.map(lambda x: StrtoTuple(x)).groupByKey().cache()
ranks = links.mapValues(lambda x: 1.0)

print(lines.collect())
print(links.collect())
print(ranks.collect())

for i in range(5):
    '''
    Python 3 doesn't support passing tuple as arg to auto-unpack
    '''
    links_ranks = links.join(ranks).values().flatMap(lambda x : [(n, x[1]/len(x[0])) for n in x[0]])
    # contribs = links_ranks.flatMap( lambda x: push_to_neighbors(x[0], x[1]))

    ranks = links_ranks.reduceByKey(lambda x,y: x + y).mapValues(lambda x: 0.15 + 0.85 * x)

print(links_ranks.collect())
output = ranks.collect()
# print(ranks.collect())
[print((str(i[0]) + " has rank: " + str(i[1]) + ".")) for i in output]
