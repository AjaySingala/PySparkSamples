# broadcast_dataframe.py
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ajay.singala.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

print("-" * 50)
columns = ["firstname","lastname","country","state"]
print("Creating the Dataframe...")
df = spark.createDataFrame(data = data, schema = columns)
print("Printing the schema...")
df.printSchema()
print("Showing the data...")
df.show(truncate=False)

print("States...")
print(states.keys())
print("Broadcast States...")
print(broadcastStates.value)
print("Broadcast States value.keys()...")
print(broadcastStates.value.keys())

print("List of keys...")
keys = list(broadcastStates.value.keys())
print(keys)

def state_convert(code):
    return broadcastStates.value[code]

print("Print DF after mapping...")
result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# Broadcast variable on filter
print("Filtering for keys...")
print(keys)
#filteDf= df.where((df['state'].isin("FL", "CA")))
#filteDf= df.where((df['state'].isin(states.keys())))
#filteDf= df.where((df['state'].isin(broadcastStates.value))))
filteDf= df.where((df['state'].isin(keys)))
filteDf.show(truncate=False)
print("-" * 50)
