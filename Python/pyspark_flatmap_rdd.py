# pyspark_flatmap_rdd.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

#Flatmap    
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)
