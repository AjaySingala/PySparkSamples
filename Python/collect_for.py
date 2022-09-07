# collect_for.py

from pyspark import SparkContext
sc = SparkContext("local", "collect-for app")
sc.setLogLevel("ERROR")

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]

rdd=sc.parallelize(dept)
dataColl=rdd.collect()
print('-' * 50)
for row in dataColl:
    print(row[0] + "," +str(row[1]))
print('-' * 50)
