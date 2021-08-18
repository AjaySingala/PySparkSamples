# StructType_StructField.py
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when

spark = SparkSession.builder.master("local[1]") \
                    .appName('ajaysingala.com') \
                    .getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
print("Regular schema...")
df.printSchema()
df.show(truncate=False)

# Complex object.
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
print("Complex schema...")
df2.printSchema()
df2.show(truncate=False)

# # Custom / computed columns. Create columns from existing columns using "withColumn()".
# updatedDF = df2.withColumn("OtherInfo", 
#     struct(col("id").alias("identifier"),
#     col("gender").alias("gender"),
#     col("salary").alias("salary"),
#     when(col("salary").cast(IntegerType()) < 2000,"Low")
#       .when(col("salary").cast(IntegerType()) < 4000,"Medium")
#       .otherwise("High").alias("Salary_Grade")
#   )).drop("id","gender","salary")

# updatedDF.printSchema()
# updatedDF.show(truncate=False)


# # """ Array & Map"""
# # arrayStructureSchema = StructType([
# #     StructField('name', StructType([
# #        StructField('firstname', StringType(), True),
# #        StructField('middlename', StringType(), True),
# #        StructField('lastname', StringType(), True)
# #        ])),
# #        StructField('hobbies', ArrayType(StringType()), True),
# #        StructField('properties', MapType(StringType(),StringType()), True)
# #     ])

# # ArrayType example:
# dataArr = [
#  ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
#  ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
#  ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
# ]

# schemaArr = StructType([ 
#     StructField("name",StringType(),True), 
#     StructField("languagesAtSchool",ArrayType(StringType()),True), 
#     StructField("languagesAtWork",ArrayType(StringType()),True), 
#     StructField("currentState", StringType(), True), 
#     StructField("previousState", StringType(), True)
#   ])

# dfArr = spark.createDataFrame(data=dataArr,schema=schemaArr)
# print("ArrayType example...")
# dfArr.printSchema()
# dfArr.show()

# # MapType example:
# schemaMap = StructType([
#     StructField('name', StringType(), True),
#     StructField('properties', MapType(StringType(),StringType()),True)
# ])

# dataDictionary = [
#         ('James',{'hair':'black','eye':'brown'}),
#         ('Michael',{'hair':'brown','eye':None}),
#         ('Robert',{'hair':'red','eye':'black'}),
#         ('Washington',{'hair':'grey','eye':'grey'}),
#         ('Jefferson',{'hair':'brown','eye':''})
#         ]
# dfMap = spark.createDataFrame(data=dataDictionary, schema = schemaMap)
# print("MapType example...")
# dfMap.printSchema()
# dfMap.show(truncate=False)

# # Creating StructType object struct from JSON file.
# # Print the schema in JSON format.
# print("Schema in json format...")
# print(df2.schema.json())

# # This will return an relatively simpler schema format.
# print("Schema in simple string format...")
# print(df2.schema.simpleString())

# # Use the JSON to create a DF.
# print("Create DF using a JSON Schema....")
# import json
# schema_dict = {"fields":[{"metadata":{},"name":"name","nullable":True,"type":{"fields":[{"metadata":{},"name":"firstname","nullable":True,"type":"string"},{"metadata":{},"name":"middlename","nullable":True,"type":"string"},{"metadata":{},"name":"lastname","nullable":True,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"id","nullable":True,"type":"string"},{"metadata":{},"name":"gender","nullable":True,"type":"string"},{"metadata":{},"name":"salary","nullable":True,"type":"integer"}],"type":"struct"}

# #schemaFromJson = StructType.fromJson(json.loads("resources/spark_examples/schema.json"))
# schemaFromJson = StructType.fromJson(schema_dict)
# df3 = spark.createDataFrame(
#         spark.sparkContext.parallelize(structureData),schemaFromJson)
# df3.printSchema()
# df3.show()

# Create DF using Row.
print("Create DF using Row...")
print("Access values using index...")
row = Row("James", 40)
print(row[0], row[1])

print("Access values using field names (named arguments)...")
row2 = Row(name = "Mary", age = 2)
print(row2.name, row2.age)

print("Define a custom class from Row...")
Person = Row("name", "age")
p1 = Person("John", 25)
p2 = Person("Mary", 21)
print(p1.name, p1.age)
print(p2.name, p2.age)

# Use Row class on RDD.
dataRow = [Row(name="James,,Smith",lang=["Java","Scala","C++"], state="OH"),
  Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NY"),
  Row(name="Robert,,Williams",lang=["CSharp","VB"],state="UT")
]

rddRows = spark.sparkContext.parallelize(dataRow)
print("Printing rdd created using Rows...")
print(rddRows.collect())

rddRowsCollected = rddRows.collect()
print("Printing rdd created using Rows using a 'for' loop...")
for row in rddRowsCollected:
  print(row.name, row.lang, row.state)

# Create DF using custom Row class.
Person = Row("name", "lang", "state")
dataRow = [Person("James,,Smith",["Java","Scala","C++"], "OH"),
  Person("Michael,Rose,",["Spark","Java","C++"],"NY"),
  Person("Robert,,Williams",["CSharp","VB"],"UT")
]

dfRows = spark.createDataFrame(dataRow)
print("DF created using custom Row class...")
df.printSchema()
df.show()

columns = ["name", "languagesUsed", "currentState"]
dfRowsWithColumnList = spark.createDataFrame(dataRow).toDF(*columns)
print("Change column name(s) using .toDF()...")
dfRowsWithColumnList.printSchema()
dfRowsWithColumnList.show()

# Nested Rows.
dataNestedRows = [Row(name="John", properties=Row(hair="brown", eye="black")),
  Row(name="Mary", properties=Row(hair="blonde", eye="green")),
  Row(name="Joe", properties=Row(hair="black", eye="blue"))
]
dfNestedRows = spark.createDataFrame(dataNestedRows)
print("DF with nested rows...")
dfNestedRows.printSchema()
dfNestedRows.show()
