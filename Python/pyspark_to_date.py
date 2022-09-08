# pyspark_to_date.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
               .appName('SparkSQL Date Examples') \
               .getOrCreate()

df=spark.createDataFrame([["02-03-2013"],["05-06-2023"]],["input"])
df.select(col("input"),to_date(col("input"),"MM-dd-yyyy").alias("date")) \
  .show()

#SQL
spark.sql("select to_date('02-03-2013','MM-dd-yyyy') date").show()

df2=spark.createDataFrame([["02-03-2013 10:25:36"],["05-06-2023"]],["input"])
df2.select(col("input"),to_date(col("input"),"MM-dd-yyyy").alias("date")).show()

