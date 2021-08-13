# init_dataframes.py
# Starting point - SparkSession.

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Creating Dataframes.
# spark is an existing SparkSession
# Path must exist in HDFS.
df = spark.read.json("resources/spark_examples/people.json")
# Displays the content of the DataFrame to stdout
df.show()

# Untyped Dataset Operations (aka DataFrame Operations).
# DataFrames are just Dataset of Rows in Scala and Java API. 
# These operations are also referred as "untyped transformations" in contrast to "typed transformations" 
# that come with strongly typed Scala/Java Datasets.
#
# In Python, it's possible to access a DataFrame's columns either by attribute (df.age) or by indexing (df['age']). 
# While the former is convenient for interactive data exploration, users are highly encouraged to use the latter form, 
# which is future proof and won't break with column names that are also attributes on the DataFrame class.
#
# spark, df are from the previous example
# Print the schema in a tree format
df.printSchema()

# Select only the "name" column
df.select("name").show()

# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()

# Select people older than 21
df.filter(df['age'] > 21).show()

# Count people by age
df.groupBy("age").count().show()

# Running SQL Queries Programmatically.
# The sql function on a SparkSession enables applications to run SQL queries programmatically and 
# returns the result as a DataFrame.

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()

# Global Temporary View.
# Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. 
# If you want to have a temporary view that is shared among all sessions and keep alive until 
# the Spark application terminates, you can create a global temporary view. 
# Global temporary view is tied to a system preserved database global_temp, and we must use the 
# qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()

# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
