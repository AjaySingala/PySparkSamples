from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Read JSON file into dataframe
df = spark.read.json("resources/zipcodes.json")
df.printSchema()
df.show()

# Read JSON file into dataframe
df = spark.read.format('org.apache.spark.sql.json') \
        .load("resources/zipcodes.json")
# Read multiline json file
multiline_df = spark.read.option("multiline","true") \
      .json("resources/multiline-zipcode.json")
multiline_df.show()

# Read multiple files. Create zipcode2.json first.
df2 = spark.read.json(
    ['resources/zipcode1.json','resources/zipcode2.json'])
df2.show() 

# Read all JSON files from a folder
df3 = spark.read.json("resources/*.json")
df3.show()

# Define custom schema
schema = StructType([
      StructField("RecordNumber",IntegerType(),True),
      StructField("Zipcode",IntegerType(),True),
      StructField("ZipCodeType",StringType(),True),
      StructField("City",StringType(),True),
      StructField("State",StringType(),True),
      StructField("LocationType",StringType(),True),
      StructField("Lat",DoubleType(),True),
      StructField("Long",DoubleType(),True),
      StructField("Xaxis",IntegerType(),True),
      StructField("Yaxis",DoubleType(),True),
      StructField("Zaxis",DoubleType(),True),
      StructField("WorldRegion",StringType(),True),
      StructField("Country",StringType(),True),
      StructField("LocationText",StringType(),True),
      StructField("Location",StringType(),True),
      StructField("Decommisioned",BooleanType(),True),
      StructField("TaxReturnsFiled",StringType(),True),
      StructField("EstimatedPopulation",IntegerType(),True),
      StructField("TotalWages",IntegerType(),True),
      StructField("Notes",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema) \
        .json("resources/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show()

# Read JSON file using PySpark SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcode USING json OPTIONS" + 
      " (path 'resources/zipcodes.json')")
spark.sql("select * from zipcode").show()

# read a JSON file by creating a temporary view.
spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" + " (path 'src/main/resources/zipcodes.json')")
spark.sqlContext.sql("select * from zipcodes").show()

# Write PySpark DataFrame to JSON file
df2.write.json("/tmp/spark_output/zipcodes.json")

