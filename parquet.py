import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("parquetFile").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data =[("James ","","Smith","36636","M",3000),
        ("Michael ","Rose","","40288","M",4000),
        ("Robert ","","Williams","42114","M",4000),
        ("Maria ","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]

print("Creating Data Frame...")
df=spark.createDataFrame(data,columns)
df.show()

# Write to a Parquet file to HDFS.
#df.write.parquet("/tmp/output/people.parquet")

# Write to local folder.
#print("Writing...")
#df.write.parquet("file:///home/hdoop/tmp/output/people.parquet")

# Read from a Parquet file.
#print("Reading...")
#parDF=spark.read.parquet("/tmp/output/people.parquet")
#parDF.show()

# Append or overwrite a Parquet file.
#print("Appending...")
#df.write.mode('append').parquet("/tmp/output/people.parquet")
#print("Overwriting...")
#df.write.mode('overwrite').parquet("/tmp/output/people.parquet")

# Create temporary views on parquet files for executing sql queries.
#print("Creating temporary view...")
#parDF.createOrReplaceTempView("ParquetTable")
#parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
#parkSQL.show()

# Creating a table on Parquet file.
#print("Create and show table on Parquet file...")
#spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"/tmp/output/people.parquet\")")
#spark.sql("SELECT * FROM PERSON").show()

# Create Parquet partition file.
#print("Creating Parquet partition file...")
#df.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")

# Retrieving from a partitioned Parquet file
#print("Retrieving from a partitioned Parquet file...")
#parDF2=spark.read.parquet("/tmp/output/people2.parquet/gender=M")
#parDF2.show(truncate=False)

# Creating a table on Partitioned Parquet file.
#print("Creating a table on Partitioned Parquet file...")
#spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"/tmp/output/people2.parquet/gender=F\")")
#spark.sql("SELECT * FROM PERSON2" ).show()

