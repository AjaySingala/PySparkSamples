# init_rdd_schemas.py

# Creating Datasets.
# Not available in Python. Only in Java and Scala.

# Interoperating with RDDs.
# Spark SQL supports two different methods for converting existing RDDs into Datasets. 
# The first method uses reflection to infer the schema of an RDD that contains specific types of objects. 
# This reflection-based approach leads to more concise code and works well when you already know 
# the schema while writing your Spark application.
#
# The second method for creating Datasets is through a programmatic interface that allows you 
# to construct a schema and then apply it to an existing RDD. While this method is more verbose, 
# it allows you to construct Datasets when the columns and their types are not known until runtime.
#
# Inferring the Schema Using Reflection.
# Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the datatypes. 
# Rows are constructed by passing a list of key/value pairs as kwargs to the Row class. 
# The keys of this list define the column names of the table, and the types are inferred by 
# sampling the whole dataset, similar to the inference that is performed on JSON files.

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Schema basic example") \
    .getOrCreate()
#    .config("spark.some.config.option", "some-value") \

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
# Path must exist in HDFS.
lines = sc.textFile("resources/spark_examples/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)

# Programmatically Specifying the Schema.
# Import data types
from pyspark.sql.types import StringType, StructType, StructField

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
# Path must exist in HDFS.
lines = sc.textFile("resources/spark_examples/people.txt")
parts = lines.map(lambda l: l.split(","))
# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

results.show()
