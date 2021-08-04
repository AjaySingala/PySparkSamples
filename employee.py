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
df = spark.read.json("file:///home/maria_dev/SparkSamples/resources/employee.json")

df.printSchema()
df.show()

# # read multiline json file.
# multiline_df = spark.read.option("multiline","true").json("file:///home/maria_dev/SparkSamples/resources/multiline_employee.json")
# multiline_df.show()    

# # readAll files in a folder.
#df2 = spark.read.json("file:///home/maria_dev/SparkSamples/resources/e?.json")
#df2.show()
