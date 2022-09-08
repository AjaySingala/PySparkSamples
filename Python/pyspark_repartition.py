# pyspark_repartition.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Repartition").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\nRead airports.csv, create DF and show schema...")
file = "file:///home/hdoop/SparkSamples/Python/airports.csv"
df = spark.read.csv(file, inferSchema=True, header=True)
df.printSchema()

print("\nPrint first, take(10) and count()...")
print("\nPrint first...")
print(df.first())
print("\nPrint take(10)...")
print(df.take(10))
print("\nPrint count()...")
print(df.count())
print("\nNo. of partitions of the DF...")
print(df.rdd.getNumPartitions())

print("\nFilter....")
# using the filter function.
df_ca = df.filter(df["country"] == "Canada")
print("\nPrint count()...")
print(df_ca.count())

print("\nPrint Canada records...")
for element in df_ca.collect():
   print(element)

print("\nNo. of partitions....")
print(df.rdd.getNumPartitions())

print("\nNo. of partitions changed to 5....")
c = df.rdd.repartition(5)
print("\nPrint take(20)...")
print(c.take(20))
print("\nNo. of partitions of the DF...")
print(c.getNumPartitions())

print("\nNo. of partitions changed to 3 on country....")
d = df.repartition(3, "country")
d.show()
print("\nNo. of partitions of the DF...")
print(d.rdd.getNumPartitions())

print("\nNo. of partitions changed to 3 on country and city....")
e = df.repartition(3, "country", "city")
e.show()
print("\nNo. of partitions of the DF...")
print(e.rdd.getNumPartitions())

# coalesce.
rdd = spark.sparkContext.parallelize((0,1,2,3,4,5,6,7))
print(rdd.collect())
print(rdd.getNumPartitions())		# 2.

rdd1 = rdd.repartition(5)
print(rdd1.getNumPartitions())		# 5.

rdd2 = rdd1.coalesce(4)
print(rdd2.getNumPartitions())		# 4.
