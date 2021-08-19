import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()

# Read CSV file into dataframe
df = spark.read.csv("resources/zipcodes.csv")
df.printSchema()

# # Alternatives:
# df = spark.read.format("csv")
#                   .load("/tmp/resources/zipcodes.csv")
# ##       or
# df = spark.read.format("org.apache.spark.sql.csv")
#                   .load("/tmp/resources/zipcodes.csv")
# df.printSchema()

# Using Header Record For Column Names.
df2 = spark.read.option("header",True) \
     .csv("resources/zipcodes.csv")
df2.printSchema()
# OR
df3 = spark.read.options(header='True', delimiter=',') \
  .csv("resources/zipcodes.csv")
df3.printSchema()

# # Read Multiple CSV Files.
# df = spark.read.csv("path1,path2,path3")

# # Read all CSV Files in a Directory.
# df = spark.read.csv("Folder path")

# Reading CSV files with a user-specified custom schema.
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)
      
df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("resources/zipcodes.csv")
df_with_schema.printSchema()
# Once you have created DataFrame from the CSV file, you can apply all transformation and actions DataFrame support..

# Write PySpark DataFrame to CSV file.
df2.write.option("header",True) \
 .csv("resources/spark_output/zipcodes123")

# Write with Options.
df2.write.options(header='True', delimiter=',') \
 .csv("resources/spark_output/zipcodes")
