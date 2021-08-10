# In this example, we have two accumulators sum and num which accumulates the value of items and the number of items respectively.
# init and mean should be executed on the driver while process on the worker node.
# Though the mean could be derived using other native spark methods, this is an example of how to manage multiple
# accumulators using a single class without declaring them as global (which doesn’t work in case of modularized code).

from pyspark.sql import SparkSession


def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()


def test_rdd(spark):
    return spark.sparkContext.parallelize([i for i in range(10)])



class MeanAccumulator:

    def __init__(self, spark):
        """
        To be executed in driver program
        :param spark: spark session
        """
        self.sum = spark.sparkContext.accumulator(0)
        self.num = spark.sparkContext.accumulator(0)

    def process(self, item):
        """
        To be executed in worker nodes
        :param item: item to add
        :return: None
        """
        self.num += 1
        self.sum += item

    def mean(self):
        """
        To be executed in driver program.
        :return: mean of all items
        """
        return self.sum.value/self.num.value


def process_data_mean():
    spark = get_spark()
    mean_acc = MeanAccumulator(spark)
    test_rdd(spark).foreach(mean_acc.process)
    print(mean_acc.mean())


process_data_mean()