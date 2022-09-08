# rdd_to_dataframe.py
# dfways.py
# Different ways to create DataFrames.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark=SparkSession.builder.appName("ajaysingala.com").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print('-' * 50)
print("Preparing the data...")
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark.sparkContext.parallelize(data)

print("Create DF using toDF...")
# Using toDF().
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

print("Create DF using toDF with schema...")
# With schema.
columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

print("Create DF using createDataFrame() from SparkSession...")
# Create DF using createDataFrame() from SparkSession.
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()

print("Create DF from List collection using spark.createDataFrame() with schema...")
# Create DF from List collection using spark.createDataFrame() with schema.
dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()

print("Using createDataFrame() with the Row type...")
# Using createDataFrame() with the Row type.
rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(rowData,columns)
dfFromData3.printSchema()

print("-" * 50)
print("More examples...")

print("Create the data...")
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)

print("Create DF from RDD using toDF()...")
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

print("Create DF from RDD using toDF() with schema...")
deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

print("Create DF from RDD using createDataFrame() woith schema...")
deptDF = spark.createDataFrame(rdd, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

print("Create DF from RDD using createDataFrame() woith StructType schema...")
from pyspark.sql.types import StructType,StructField, StringType
deptSchema = StructType([
    StructField("dept_name", StringType(), True),
    StructField("dept_id", StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

print('-' * 50)

# deptDF1.show(truncate=10)
# deptDF1.show(truncate=False, n=2)
# deptDF1.show(vertical=False)  # Default.
# deptDF1.show(vertical=True)
