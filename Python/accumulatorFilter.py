# The FilterAccumulator class holds the list of items. 
# Accumulator acc is updated if the item passed in the process method is not in the list of items. 
# init and count should be executed on the driver while process on the worker node. 
# Execution of the above gives a value of 4 which is the correct result.

from pyspark.sql import SparkSession


def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()

def test_rdd(spark):
    return spark.sparkContext.parallelize([i for i in range(10)])

class FilterAccumulator:

    def __init__(self, spark, items):
        """
        To be executed in driver program
        :param spark: spark session
        :param items: list of items to filter 
        """
        self.acc = spark.sparkContext.accumulator(0)
        self.items = items

    def process(self, item):
        """
        To be executed in worker nodes
        :param item: item to filter
        :return: None
        """
        if item not in self.items:
            self.acc += 1

    def count(self):
        """
        To be executed in driver program. 
        :return: The number of item not in items
        """
        return self.acc.value


def process_data_filter():
    spark = get_spark()
    filter_acc = FilterAccumulator(spark, [i for i in range(4, 10)])
    test_rdd(spark).foreach(filter_acc.process)
    print(filter_acc.count())

process_data_filter()
