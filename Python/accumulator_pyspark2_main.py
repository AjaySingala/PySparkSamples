# accumulator_pyspark2_main.py
from pyspark.sql import SparkSession
from accumulator_pyspark2_process import process_data, process_data_accumulator

def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()


def test_rdd(spark):
    return spark.sparkContext.parallelize([1, 2, 3])

# This will not work.
def global_accumulator_module():
    spark = get_spark()
    cnt = spark.sparkContext.accumulator(0)
    print(process_data(test_rdd(spark)))

# # This after explaining failure of global_accumulator_module().
# def broadcast_accumulator():
#     spark = get_spark()
#     acc = spark.sparkContext.accumulator(0)
#     print(process_data_accumulator(test_rdd(spark), acc))

global_accumulator_module()

    