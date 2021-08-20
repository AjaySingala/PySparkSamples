# pyspark_readcsv.py 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")     # Default is INFO.

df = spark.read.csv("resources/zipcodes.csv")
df.printSchema()

df2 = spark.read.option("header",True) \
     .csv("resources/zipcodes.csv")
print("Read csv with header=true...")
df2.printSchema()
   
df3 = spark.read.options(header='True', delimiter=',') \
  .csv("resources/zipcodes.csv")
print("Read csv with header=true and delimiter=','...")
df3.printSchema()

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
print("Read csv with schema...")
df_with_schema.printSchema()
df_with_schema.show()

print("Writing csv to /tmp/spark_output/zipcodes123...")
df2.write.option("header",True) \
 .csv("/tmp/spark_output/zipcodes123")
