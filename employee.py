import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# read json file into dataframe.
# Read from HDFS.
#df = spark.read.json("resources/employee.json")

# Read from local.
df = spark.read.json("file:///home/maria_dev/files/employee.json")

df.printSchema()
df.show(False)

# read multiline json file.
multiline_df = spark.read.option("multiline","true").json("resources/employee_m.json")
multiline_df.show(False)    

# read multiple files.
df2 = spark.read.json("resources/e1.json",  "resources/e2.json")
df2.show(False)

# # readAll files in a folder.
# df2 = spark.read.json(”somefolder")
# df2.show(False)
