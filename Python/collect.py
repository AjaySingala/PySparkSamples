# collect.py
from pyspark import SparkContext
sc = SparkContext("local", "Collect app")
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
coll = words.collect()
print('-' * 50)
print("Elements in RDD -> %s" % (coll))
print('-' * 50)
