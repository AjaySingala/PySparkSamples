# foreach.py
from pyspark import SparkContext
sc = SparkContext("local", "ForEach app")
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
def f(x): print(x)
print('-' * 50)
fore = words.foreach(f) 
print('-' * 50)