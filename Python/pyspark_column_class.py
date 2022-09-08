# pyspark_column_class.py
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when, lit

spark = SparkSession.builder.master("local[1]") \
                    .appName('ajaysingala.com') \
                    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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
print("\nprint using col()...")
df.select(col("age")).show()
# Access column names with dot and backticks (`).
df.select(col("`name.firstname`")).show()

# Access struct type columns.
data2 = [Row(name="John", prop=Row(hair="black", eye="blue")),
    Row(name="Mary", prop=Row(hair="blonde", eye="green")),
    Row(name="John", prop=Row(hair="brown", eye="black"))
]
df2 = spark.createDataFrame(data2)
print("\nSchema...")
df2.printSchema()
df2.show()

print("\nAccess struct cols...")
df2.select(df2.prop.hair).show()
df2.select(df2["prop.hair"]).show()
df2.select(col("prop.hair")).show()

print("\nAccess all strut cols...")
df2.select(col("prop.*")).show()

# Column Operators.
data3 = [(100,2,1), (200,3,4), (300,4,4)]
df = spark.createDataFrame(data3).toDF("col1", "col2", "col3")

# Arithmetic operations.
print("\ncolumn arithmetic operations...")
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
print("\nalias()...")
df4.select(df4.fname.alias("first_name"), \
          df4.lname.alias("last_name")
   ).show()

#Another example
df4.select(expr(" lname ||', '|| fname").alias("fullName") \
   ).show()

# sort(), .asc() and .desc().
print("\nasc() and desc()...")
df4.sort(df4.fname.asc()).show()
df4.sort(df4.fname.desc()).show()

# between().
print("\nbetween()...")
df4.filter(df4.id.between(100,300)).show()

# contains().
print("\ncontains()...")
df4.filter(df4.fname.contains("Cruise")).show()

# like().
print("\nlike()...")
df4.select(df4.fname,df4.lname,df4.id) \
  .filter(df4.fname.like("%om")) 

# when & otherwise
from pyspark.sql.functions import when
print("\nwhen & otherwise...")
df4.select(df4.fname,df4.lname,when(df4.gender=="M","Male") \
    .when(df4.gender=="F","Female") \
    .when(col("gender").isNull() ,"undefined") \
    .otherwise(df4.gender).alias("new_gender") \
).show()

# startsWith() and endsWith()
print("\nstartsWith()...")
df4.filter(df4.fname.startswith("T")).show()
print("\nendsWith()...")
df4.filter(df4.fname.endswith("Cruise")).show()

# Select Nested Struct Columns from PySpark.
data5 = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]
    
schema5 = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df5 = spark.createDataFrame(data = data5, schema = schema5)
print("\nDF with nested struct types...")
df5.printSchema()
df5.show(truncate=False) # shows all columns

# Select struct column.
print("\nDisplay name...")
df5.select("name").show(truncate=False)

# Select the specific column from a nested struct, explicitly qualify the nested struct column name.
print("\nDisplay firstname and lastname...")
df5.select("name.firstname","name.lastname").show(truncate=False)

# Get all columns from struct column.
print("\nDisplay name.*...")
df5.select("name.*").show(truncate=False)

print("\nCreate DataFrame with struct, array & map...")
#Create DataFrame with struct, array & map
data=[(("James","Bond"),["Java","C#"],{"hair":"black","eye":"brown"}),
      (("Ann","Varsa"),[".NET","Python"],{"hair":"brown","eye":"black"}),
      (("Tom Cruise",""),["Python","Scala"],{"hair":"red","eye":"grey"}),
      (("Tom Brand",None),["Perl","Ruby"],{"hair":"black","eye":"blue"})]

schema = StructType([
        StructField("name", StructType([
            StructField("fname", StringType(), True),
            StructField("lname", StringType(), True)])),
        StructField("languages", ArrayType(StringType()),True),
        StructField("properties", MapType(StringType(),StringType()),True)
     ])
df=spark.createDataFrame(data,schema)
df.printSchema()

print("\ngetField() from MapType...")
df.select(df.properties.getField("hair")).show()

print("\ngetField() from Struct...")
df.select(df.name.getField("fname")).show()

print("\ngetItem() used with ArrayType...")
df.select(df.languages.getItem(1)).show()

print("\ngetItem() used with MapType...")
df.select(df.properties.getItem("hair")).show()
