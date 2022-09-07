#broadcast.py
from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 
sc.setLogLevel("ERROR")
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print('-' * 50)
print( "Stored data -> %s" % (data)) 
elem = words_new.value[2] 
print("Printing a particular element in RDD -> %s" % (elem))
print('-' * 50)

