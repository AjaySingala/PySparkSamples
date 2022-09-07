# reduce.py
from pyspark import SparkContext
from operator import add
sc = SparkContext("local", "Reduce app")
sc.setLogLevel("ERROR")
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print('-' * 50)
print("Adding all the elements -> %i" % (adding))
print('-' * 50)

