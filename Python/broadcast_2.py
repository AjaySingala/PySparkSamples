# broadcast_2.py

from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast-2 app") 
sc.setLogLevel("ERROR")

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = sc.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = sc.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

print('-' * 50)
print("result...")
result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)
print('-' * 50)
