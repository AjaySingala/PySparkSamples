# rdd_to_dataframe.py
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

deptDF = spark.createDataFrame(rdd, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

from pyspark.sql.types import StructType,StructField, StringType
deptSchema = StructType([       
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
# deptDF1.show(truncate=10)
# deptDF1.show(truncate=False, n=2)
# deptDF1.show(vertical=False)  # Default.
# deptDF1.show(vertical=True)
