pyspark_column_class.py
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when, lit

spark = SparkSession.builder.master("local[1]") \
                    .appName('ajaysingala.com') \
                    .getOrCreate()

# simplests way to create a column.
colObj = lit("ajaysingala.com")

data = [("John", 23), ("Mary", 21)]
df = spark.createDataFrame(data).toDF("name.firstname", "age")
df.printSchema()
df.show()

df.select(df.age).show()
df.select(df["age"]).show()
# Access column names with dot and backticks (`).
df.select(df["`name.firstname`"]).show()

# Using SQL col().
print("print using col()...")
df.select(col("gender")).show()
# Access column names with dot and backticks (`).
df.select(col("`name.firstname`")).show()

# Access struct type columns.
data2 = [Row(name="John", prop=Row(hair="black", eye="blue")),
    Row(name="Mary", prop=Row(hair="blonde", eye="green")),
    Row(name="John", prop=Row(hair="brown", eye="black"))
]
df2 = spark.createDataFrame(data2)
print("Schema...")
df2.printSchema()
df2.show()

print("Access struct cols...")
df2.select(df.prop.hair).show()
df2.select(df["prop.hair"]).show()
df2.select(col("prop.hair")).show()

print("Access all strut cols...")
df2.select(col("prop.*")).show()

# Column Operators.
data3 = [(100,2,1), (200,3,4), (300,4,4)]
df = spark.createDataFrame(data3).toDF("col1", "col2", "col3")

# Arithmetic operations.
print("column arithmetic operations...")
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

# Column functions.
data4 = [("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns4 = ["fname","lname","id","gender"]
df4=spark.createDataFrame(data4,columns4)

# alias().
from pyspark.sql.functions import expr
print("alias()...")
df4.select(df4.fname.alias("first_name"), \
          df4.lname.alias("last_name")
   ).show()

#Another example
df4.select(expr(" lname ||', '|| fname").alias("fullName") \
   ).show()

# sort(), .asc() and .desc().
print("asc() and desc()...")
df4.sort(df4.fname.asc()).show()
df4.sort(df4.fname.desc()).show()

# between().
print("between()...")
df4.filter(df4.id.between(100,300)).show()

# contains().
print("contains()...")
df4.filter(df4.fname.contains("Cruise")).show()

# like().
print("like()...")
df4.select(df4.fname,df.lname,df.id) \
  .filter(df4.fname.like("%om")) 

# when & otherwise
from pyspark.sql.functions import when
df4.select(df4.fname,df.lname,when(df4.gender=="M","Male") \
              .when(df4.gender=="F","Female") \
              .otherwise(df4.gender).alias("new_gender") \
    ).show()

# Select Nested Struct Columns from PySpark.
data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns

# Select struct column.
df2.select("name").show(truncate=False)

# Select the specific column from a nested struct, explicitly qualify the nested struct column name.
df2.select("name.firstname","name.lastname").show(truncate=False)

# Get all columns from struct column.
df2.select("name.*").show(truncate=False)


