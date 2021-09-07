# accumulator_pyspark2_process.py
def count_items(x):
    global cnt
    cnt += x


def process_data(rdd):
    rdd.foreach(count_items)
    return cnt.value

# These are part 2, i.e.; after explaining global_accumulator_module() call in accumulator_pyspark2_main.py.
def count_items_with_accumulator(x, acc):
    acc += x

def process_data_accumulator(rdd, acc):
    rdd.foreach(lambda x: count_items_with_accumulator(x, acc))
    return acc.value
