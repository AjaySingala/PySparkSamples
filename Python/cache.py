# cache.py
from pyspark import SparkContext 
sc = SparkContext("local", "Cache app") 
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
words.cache() 
caching = words.persist().is_cached 
print('-' * 50)
print("Words got chached > %s" % (caching))
print('-' * 50)

