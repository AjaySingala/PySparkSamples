# df_row.py
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when

spark = SparkSession.builder.master("local[1]") \
                    .appName('ajaysingala.com') \
                    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create DF using Row.
print("Create DF using Row...")
print("Access values using index...")
row = Row("James", 40)
print(row[0], row[1])

print("\nAccess values using field names (named arguments)...")
row2 = Row(name = "Mary", age = 2)
print(row2.name, row2.age)

print("\nDefine a custom class from Row...")
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
print("\nPrinting rdd created using Rows...")
print(rddRows.collect())

rddRowsCollected = rddRows.collect()
print("\nPrinting rdd created using Rows using a 'for' loop...")
for row in rddRowsCollected:
  print(row.name, row.lang, row.state)

print("\nCreate RDD using custom Row class...")
# Create RDD using custom Row class.
Person = Row("name", "lang", "state")
dataRow = [Person("James,,Smith",["Java","Scala","C++"], "OH"),
  Person("Michael,Rose,",["Spark","Java","C++"],"NY"),
  Person("Robert,,Williams",["CSharp","VB"],"UT")
]
rdd=spark.sparkContext.parallelize(dataRow)
collData=rdd.collect()
print(collData)

print("\nPrint after collect()...")
for person in collData:
    print(person.name + "," +str(person.lang))

print("\nCreate DF using custom Row class...")
# Create DF using custom Row class.
dfRows = spark.createDataFrame(dataRow)
print("DF created using custom Row class...")
dfRows.printSchema()
dfRows.show()
collData=dfRows.collect()
print("\nPrint after collect()...")
for person in collData:
    print(person.name + "," +str(person.lang))

columns = ["name", "languagesUsed", "currentState"]
dfRowsWithColumnList = spark.createDataFrame(dataRow).toDF(*columns)
print("Change column name(s) using .toDF()...")
dfRowsWithColumnList.printSchema()
dfRowsWithColumnList.show()

print("\nPrint after collect()...")
collData=dfRowsWithColumnList.collect()
print(collData)
for row in collData:
    print(row.name + "," +str(row.languagesUsed))

# Nested Rows.
dataNestedRows = [Row(name="John", properties=Row(hair="brown", eye="black")),
  Row(name="Mary", properties=Row(hair="blonde", eye="green")),
  Row(name="Joe", properties=Row(hair="black", eye="blue"))
]
dfNestedRows = spark.createDataFrame(dataNestedRows)
print("DF with nested rows...")
dfNestedRows.printSchema()
dfNestedRows.show()
