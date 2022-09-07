# map.py
from pyspark import SparkContext
sc = SparkContext("local", "Map app")
sc.setLogLevel("ERROR")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print('-' * 50)
print("Key value pair -> %s" % (mapping))
print('-' * 50)
