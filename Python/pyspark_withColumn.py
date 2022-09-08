# pyspark_withColumn.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
print("The original schema...")
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("salary",col("salary").cast("Integer"))
print("df2 with salary as integer...")
df2.printSchema()
df2.show(truncate=False)

df3 = df.withColumn("salary",col("salary")*100)
print("salary * 100...")
df3.printSchema()
df3.show(truncate=False) 

df4 = df.withColumn("CopiedColumn",col("salary")* -1)
print("Copy the column 'salary'...")
df4.printSchema()
df4.show()

df5 = df.withColumn("Country", lit("USA"))
print("Literal value to column 'Country'...")
df5.printSchema()
df5.show()

df6 = df.withColumn("Country", lit("USA")) \
   .withColumn("anotherColumn",lit("anotherValue"))
print("Literal value to column 'Country' and create another column with a literal value...")
df6.printSchema()
df6.show()

df.withColumnRenamed("gender","sex") \
  .show(truncate=False) 
print("Rename column 'gender' to 'sex' in the original DF...")
df.printSchema()
df.show()

df4.drop("CopiedColumn") \
.show(truncate=False) 
print("Drop the column 'CopiedColumn from df4...")
df4.printSchema()
df4.show()
