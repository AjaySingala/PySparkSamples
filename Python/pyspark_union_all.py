# pyspark_union_all.py
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ajaysingala.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("-" * 50)
print("Defining the first DF and schema...")
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

print("\nDefining the second DF and schema...")
simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

df2.printSchema()
df2.show(truncate=False)

print("\nMerge the two DFs using union()...")
unionDF = df.union(df2)
unionDF.show(truncate=False)

print("\nMerge the two DFs using unionAll()...")
unionAllDF = df.unionAll(df2)
unionAllDF.show(truncate=False)
print("\nResult is same as union()...")

print("\nMerge the two DFs without duplicates using distinct()...")
disDF = df.union(df2).distinct()
disDF.show(truncate=False)

print("-" * 50)
