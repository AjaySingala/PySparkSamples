# readfile.py
from pyspark import SparkContext
sc = SparkContext("local", "read file app")
sc.setLogLevel("ERROR")
inputfile = sc.textFile("file:///home/hdoop/SparkSamples/Python/twinkle.txt")
counts = inputfile.flatMap(lambda line: line.split(" ")).map(lambda word:  (word, 1)).reduceByKey(lambda a,b: a+b)
print('-' * 50)
print("counts.toDebugString()...")
print(counts.toDebugString())
print('-' * 50)
print("There are %i elements" % (counts.count()))
print('-' * 50)
print("The contents...")
elems = counts.collect()
for row in elems:
    print(row)
print('-' * 50)
print("Writing to /home/hdoop/SparkSamples/Python/twinkle-output")
counts.saveAsTextFile("file:///home/hdoop/SparkSamples/Python/twinkle-output")
print('-' * 50)

def printData(c):
    print(c)
