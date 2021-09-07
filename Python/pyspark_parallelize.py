# pyspark_parallelize.py
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()
sparkContext=spark.sparkContext

rdd=sparkContext.parallelize([1,2,3,4,5])
rddCollect = rdd.collect()
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))
print(rddCollect)
