# df_languages.py
import pyspark
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("Languages App").getOrCreate()
sc = spark.sparkContext

data = [("Java", "200000"), ("Python", "100000"), ("Scala", "30000")]
columns = ["language", "users_count"]

# Creating DF from RDD.
rdd = sc.parallelize(data)

dfFromRDD = rdd.toDF()
print("DF without column names...")
dfFromRDD.printSchema()
print(dfFromRDD.collect())

dfFromRDD2 = rdd.toDF(columns)
print("DF with column names...")
dfFromRDD2.printSchema()
print(dfFromRDD2.collect())

# Alternative: Create DF from SparkSession.
dfFromRdd3 = spark.createDataFrame(rdd).toDF(*columns)
print("DF with column names using SparkSession...")
dfFromRdd3.printSchema()
print(dfFromRdd3.collect())

# Create DF from List Collection.
dfFromRdd4 = spark.createDataFrame(data).toDF(*columns)
print("DF from List Collection with column names using SparkSession...")
dfFromRdd4.printSchema()
print(dfFromRdd4.collect())


# Create DF from Row Type.
rowData = map(lambda x: Row(*x), data)
dfFromRdd5 = spark.createDataFrame(rowData,columns)
print("DF from Row Type with column names using SparkSession...")
dfFromRdd5.printSchema()
print(dfFromRdd5.collect())
