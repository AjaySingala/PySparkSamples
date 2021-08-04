from pyspark import SparkContext
#logFile = "file:///home/hdoop/hadoop-3.2.2/README.txt"
#logFile = "file:///home/maria_dev/hadoop-3.2.2/README.txt"
logFile = "file:///home/maria_dev/SparkSamples/input.txt"
#sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


