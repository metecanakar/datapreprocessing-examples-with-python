import datetime
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def repartition_example(df: DataFrame, num_of_partitions):
    start_time = time.time()

    df_repartitioned = df.repartition(num_of_partitions)

    num_of_partitions = df_repartitioned.rdd.getNumPartitions()

    print(f"Repartition: Num of partitions: {num_of_partitions}")
    folder_path = f"../data/output/repartition_vs_coalesce/repartition/{num_of_partitions}_{datetime.datetime.utcnow()}"

    df_repartitioned.write.format("csv").option("header", "true").save(folder_path)

    print(f"time took for repartition and num of partitions: {num_of_partitions} is => {time.time() - start_time}")


def coalesce_example(df: DataFrame, num_of_partitions):
    start_time = time.time()

    df_coalesced = df.coalesce(num_of_partitions)

    num_of_partitions = df_coalesced.rdd.getNumPartitions()
    print(f"Coalesce: Num of partitions: {num_of_partitions}")

    folder_path = f"../data/output/repartition_vs_coalesce/coalesce/{num_of_partitions}_{datetime.datetime.utcnow()}"

    df_coalesced.write.format("csv").option("header", "true").save(folder_path)

    print(f"time took for coalesce and num of partitions: {num_of_partitions} is => {time.time() - start_time}")


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    rows = [(f"person_{i}", i) for i in range(1000000)]

    df = spark.createDataFrame(rows, ["name", "person_id"])
    initial_num_of_partitions = df.rdd.getNumPartitions()
    print(f"Initial num of partitions {initial_num_of_partitions}")

    repartition_example(df, num_of_partitions=1)
    repartition_example(df, num_of_partitions=100)
    coalesce_example(df, num_of_partitions=1)
    coalesce_example(df, num_of_partitions=100)

    """

    Initial num of partitions 4
    
    Repartition: Num of partitions: 1
    time took for repartition and num of partitions: 1 is => 13.713807582855225
    
    Repartition: Num of partitions: 100
    time took for repartition and num of partitions: 100 is => 17.30254888534546
    
    Coalesce: Num of partitions: 1
    time took for coalesce and num of partitions: 1 is => 2.7002055644989014
    
    Coalesce: Num of partitions: 4
    time took for coalesce and num of partitions: 4 is => 2.36069655418396

    
    """
