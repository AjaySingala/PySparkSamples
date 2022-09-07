#accumulator.py
from pyspark import SparkContext 
sc = SparkContext("local", "Accumulator app") 
sc.setLogLevel("ERROR")

num = sc.accumulator(10) 
def f(x): 
   global num 
   num+=x 
rdd = sc.parallelize([20,30,40,50]) 
rdd.foreach(f) 
final = num.value 
print('-' * 50)
print("Accumulated value is -> %i" % (final))
print('-' * 50)

# Example 2: Accumulator with function
accuSum=sc.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
rdd.foreach(countFun)
print('-' * 50)
print("Accumulator with function...")
print(accuSum.value)
print('-' * 50)
