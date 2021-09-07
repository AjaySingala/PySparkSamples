# pyspark_readjson.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("ajaysingala.com") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")     # Default is INFO.

# Read JSON file into dataframe    
df = spark.read.json("../resources/zipcodes.json")
df.printSchema()
df.show()

# Read multiline json file
multiline_df = spark.read.option("multiline","true") \
      .json("../resources/multiline-zipcode.json")
multiline_df.printSchema()
multiline_df.show()

# Read multiple files
df2 = spark.read.json(
    ['../resources/e1.json','../resources/e2.json'])
df2.show()    

# Read multiple files with different schemas.
df2 = spark.read.json(
    ['../resources/e1.json','../resources/e2.json', '../resources/e3.json'])
df2.show()    

#Read All JSON files from a directory
df3 = spark.read.json("../resources/*.json")
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
      StructField("Decommisioned",BooleanType(),True)
    #   ,
    #   StructField("TaxReturnsFiled",StringType(),True),
    #   StructField("EstimatedPopulation",IntegerType(),True),
    #   StructField("TotalWages",IntegerType(),True),
    #   StructField("Notes",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema) \
        .option("multiple", "true") \
        .json("../resources/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show()

# Create a table from JSON File
spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcode3 USING json OPTIONS" + 
      " (path '../resources/zipcodes.json')")
spark.sql("select * from zipcode3").show()

# PySpark write JSON File
df2.write.mode('Overwrite').json("/tmp/spark_output/emps.json")

