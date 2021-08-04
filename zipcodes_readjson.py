from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Read JSON file into dataframe
df = spark.read.json("file:///home/maria_dev/SparkSamples/resources/zipcodes.json")
df.printSchema()
df.show()

# # Read JSON file into dataframe
# df2 = spark.read.format('org.apache.spark.sql.json') \
#         .load("file:///home/maria_dev/SparkSamples/resources/zipcodes.json")
# df2.printSchema()
# df2.show()

# # Read multiline json file
# multiline_df = spark.read.option("multiline","true") \
#       .json("file:///home/maria_dev/SparkSamples/resources/multiline-zipcode.json")
# multiline_df.show()

# # Read all JSON files from a folder
# df3 = spark.read.json("file:///home/maria_dev/SparkSamples/resources/zipcodes?.json")
# df3.show()
# # Write PySpark DataFrame to JSON file
# df2.write.json("file:///home/maria_dev/tmp/spark_output/zipcodes.json")

# # Define custom schema
# schema = StructType([
#       StructField("RecordNumber",IntegerType(),True),
#       StructField("Zipcode",IntegerType(),True),
#       StructField("ZipCodeType",StringType(),True),
#       StructField("City",StringType(),True),
#       StructField("State",StringType(),True),
#       StructField("LocationType",StringType(),True),
#       StructField("Lat",DoubleType(),True),
#       StructField("Long",DoubleType(),True),
#       StructField("Xaxis",IntegerType(),True),
#       StructField("Yaxis",DoubleType(),True),
#       StructField("Zaxis",DoubleType(),True),
#       StructField("WorldRegion",StringType(),True),
#       StructField("Country",StringType(),True),
#       StructField("LocationText",StringType(),True),
#       StructField("Location",StringType(),True),
#       StructField("Decommisioned",BooleanType(),True)
#     #   ,
#     #   StructField("TaxReturnsFiled",StringType(),True),
#     #   StructField("EstimatedPopulation",IntegerType(),True),
#     #   StructField("TotalWages",IntegerType(),True),
#     #   StructField("Notes",StringType(),True)
#   ])

# df_with_schema = spark.read.schema(schema) \
#         .json("file:///home/maria_dev/SparkSamples/resources/zipcodes.json")
# df_with_schema.printSchema()
# df_with_schema.show()

# # Read JSON file using PySpark SQL
# spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcodes USING json OPTIONS" + 
#       " (path 'file:///home/maria_dev/SparkSamples/resources/zipcodes.json')")
# spark.sql("select * from zipcodes").show()

# # read a JSON file by creating a temporary view.
# spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcodes USING json OPTIONS" + " (path 'file:///home/maria_dev/SparkSamples/resources/zipcodes.json')")
# spark.sqlContext.sql("select * from zipcodes").show()


