# dfempty.py
# Create empty DataFrame and RDD.

import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("ajaysingala.com").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print('-' * 50)

print("Creates Empty RDD...")
# Creates Empty RDD.
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

print("Creates Empty RDD using parallelize...")
# Creates Empty RDD using parallelize.
rdd2= spark.sparkContext.parallelize([])
print(rdd2)

print("Create Empty DataFrame with Schema (StructType)...")
print("Create the Schema...")
# Create Schema.
from pyspark.sql.types import StructType,StructField, StringType

schema = StructType([
  StructField("firstname", StringType(), True),
  StructField("middlename", StringType(), True),
  StructField("lastname", StringType(), True)
  ])

print("Create the empty DataFrame from empty RDD with the schema...")
# Create empty DataFrame from empty RDD.
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()

print("Convert empty RDD to DF...")
df1 = emptyRDD.toDF(schema)
df1.printSchema()

print("Create empty DataFrame directly...")
# Create empty DataFrame directly.
df2 = spark.createDataFrame([], schema)
df2.printSchema()

print("Create empty DatFrame with no schema (no columns)...")
# Create empty DatFrame with no schema (no columns).
df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

print('-' * 50)
