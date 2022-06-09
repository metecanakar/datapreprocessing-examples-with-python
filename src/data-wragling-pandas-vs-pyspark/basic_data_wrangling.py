import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import numpy as np


def _create_pandas_dfs():
    """
    Create 2 pandas dataframes.
    Returns:
        pd_df_sales containing sales data. pd_df_stores containing store information.

    """
    data_sales = {"date": ["2022-02-01", "2022-02-02", "2022-02-03", "2021-02-01", "2021-02-02", "2021-02-03"],
                  "net_sales": [10, 20, 50, 30, 40, 80],
                  "store_id": [1, 2, 2, 3, 4, 4],
                  "year": ["2022", "2022", "2022", "2021", "2021", "2021"]}
    pd_df_sales = pd.DataFrame(data=data_sales)

    data_stores = {"store_id": [1, 2, 2, 3, 4, 4],
                   "store_name": ["store_1", "store_2", "store_2", "store_3", "store_4", "store_4"]}
    pd_df_stores = pd.DataFrame(data=data_stores)

    return pd_df_sales, pd_df_stores


def _create_spark_df(spark, pd_df_sales, pd_df_stores):
    """
    Create 2 spark dataframes based on the dataframes created by the _create_pandas_dfs.
    Args:
        spark: SparkSession
        pd_df_sales: Pandas sales dataframe
        pd_df_stores: Pandas store dataframe

    Returns:
        spark_df_sales, spark_df_stores
    """
    spark_df_sales = spark.createDataFrame(data=pd_df_sales)
    spark_df_stores = spark.createDataFrame(data=pd_df_stores)

    return spark_df_sales, spark_df_stores


def _immutability_check(pd_df_sales, spark_df_sales):
    """
    Spark dataframes are immutable.
    It means that when you try to make a change you get a new dataframe with a new reference.
    And the old dataframe remains unmodified.

    All pandas data structures are value-mutable (the values they contain can be altered)
    Returns:

    """
    # Filtering example
    print("Doing filtering")
    # PySpark
    # New Pyspark dataframe created
    id_rdd_before = spark_df_sales.rdd.id()
    obj_id_before = id(spark_df_sales)
    spark_df_sales = spark_df_sales.filter(spark_df_sales.store_id == 1)
    id_rdd_after = spark_df_sales.rdd.id()
    obj_id_after = id(spark_df_sales)
    print(f"pyspark rdd ids equal {id_rdd_before == id_rdd_after}")
    print(f"pyspark obj ids equal {obj_id_before == obj_id_after}")

    # Pandas
    # new pandas dataframe created
    obj_id_before = id(pd_df_sales)
    pd_df_sales = pd_df_sales[pd_df_sales.store_id == 1]
    obj_id_after = id(pd_df_sales)
    print(f"pandas obj ids equal {obj_id_before == obj_id_after}")

    # Add new column
    # New Pyspark dataframe created
    print("Adding new column")
    # PySpark
    obj_id_before = id(spark_df_sales)
    spark_df_sales = spark_df_sales.withColumn("new_column", 1 / spark_df_sales.store_id)
    obj_id_after = id(spark_df_sales)
    print(f"pyspark obj ids equal {obj_id_before == obj_id_after}")

    # Pandas
    # when adding a new column: the existing dataframe is updated (same reference)
    obj_id_before = id(pd_df_sales)
    pd_df_sales["new_column"] = 1 / pd_df_sales.store_id
    obj_id_after = id(pd_df_sales)
    print(f"pandas obj ids equal {obj_id_before == obj_id_after}")

    print("immutability test completed")


def _view_dataframe(pd_df_sales, spark_df_sales):
    """
    View pd dataframe and spark dataframe.
    Args:
        pd_df_sales: Pandas dataframe
        spark_df_sales: Spark dataframe
    """
    # pandas
    print(pd_df_sales)
    # PySpark
    spark_df_sales.show()


def _rename_columns(pd_df_sales, spark_df_sales):
    """
    Rename columns
    Args:
        pd_df_sales: Pandas dataframe
        spark_df_sales: Spark dataframe
    """
    # pandas
    pd_df_sales.columns
    pd_df_sales = pd_df_sales.rename(columns={"date": "date_new"})
    print(pd_df_sales)

    # PySpark
    spark_df_sales = spark_df_sales.withColumnRenamed("date", "date_new")
    spark_df_sales.show()


def _drop_column(pd_df_sales, spark_df_sales):
    """
    Drop columns
    Args:
        pd_df_sales: Pandas dataframe
        spark_df_sales: Spark dataframe
    """
    # pandas
    # axis=1 is column
    pd_df_sales = pd_df_sales.drop("date", axis=1)
    print(pd_df_sales)

    # PySpark
    spark_df_sales = spark_df_sales.drop("date")
    spark_df_sales.show()


def _filtering(pd_df_sales, spark_df_sales):
    """
    Filtering via pandas and pyspark dataframes
    Args:
        pd_df_sales: Pandas dataframe
        spark_df_sales: Spark dataframe
    """

    # pandas
    # get values with net_sales < 30
    #     date  net_sales  store_id
    # 0  2022-02-01         10         1
    # 0  2022-02-01         10         1
    pd_df_sales_net_sales_smaller_than_30 = pd_df_sales[pd_df_sales.net_sales < 30]
    # or using loc
    pd_df_sales_net_sales_smaller_than_30_alternative = pd_df_sales.loc[pd_df_sales["net_sales"] < 30]
    print(pd_df_sales_net_sales_smaller_than_30)
    print(pd_df_sales_net_sales_smaller_than_30_alternative)

    # get values with net_sales < 30 and date smaller than 2022_02_02
    # pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02
    #     date  net_sales  store_id
    # 0  2022-02-01         10         1
    pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02 = pd_df_sales[
        (pd_df_sales.net_sales < 30) & (pd_df_sales.date < "2022-02-02")]
    # or using loc
    pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02_alternative = pd_df_sales.loc[
        (pd_df_sales["net_sales"] < 30) & (pd_df_sales["date"] < "2022-02-02")]
    print(pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02)
    print(pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02_alternative)

    # PySpark
    # +----------+---------+--------+
    # | date | net_sales | store_id |
    # +----------+---------+--------+
    # | 2022 - 02 - 01 | 10 | 1 |
    # | 2022 - 02 - 02 | 20 | 2 |
    # +----------+---------+--------+
    spark_df_sales_net_sales_smaller_than_30 = spark_df_sales[spark_df_sales.net_sales < 30]
    spark_df_sales_net_sales_smaller_than_30.show()
    # or use filter
    # +----------+---------+--------+
    # | date | net_sales | store_id |
    # +----------+---------+--------+
    # | 2022 - 02 - 01 | 10 | 1 |
    spark_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02 = spark_df_sales.filter(
        (spark_df_sales.net_sales < 30) & (spark_df_sales.date < "2022-02-02"))
    spark_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02.show()


def _add_column(pd_df_sales, spark_df_sales):
    """
    Add column to existing dataframes
    Args:
        pd_df_sales: Pandas df
        spark_df_sales: Pyspark df
    """
    # pandas
    pd_df_sales["new_column"] = 1 / pd_df_sales.net_sales

    # pyspark
    spark_df_sales = spark_df_sales.withColumn("new_column", 1 / spark_df_sales.net_sales)


def _fill_nulls(pd_df_sales, spark_df_sales):
    """
    Fill null values.
    Args:
        pd_df_sales: Pandas df
        spark_df_sales: Pyspark df

    """
    # pandas
    pd_df_sales.fillna(0)
    # pyspark
    spark_df_sales.fillna(0)


def _aggregation(spark, pd_df_sales, spark_df_sales):
    """
    Do aggregations using pyspark and pandas
    Args:
        spark: SparkSession
        pd_df_sales: Pandas df
        spark_df_sales: Spark df
    """
    # pyspark
    # using GroupedData.agg
    # dictionary expression multiple metrics on the same column doesn't work for some reason. Therefore, use functions
    # exprs = {"net_sales": "mean", "net_sales": "max"}
    spark_df_sales_with_mean_and_max_sales = spark_df_sales.groupBy("year"). \
        agg(F.mean("net_sales"), F.max("net_sales")). \
        withColumnRenamed("avg(net_sales)", "avg_net_sales"). \
        withColumnRenamed("max(net_sales)", "max_net_sales")
    spark_df_sales_with_mean_and_max_sales.show()

    # using pure sql
    spark_df_sales.createOrReplaceTempView("sales")
    query = "SELECT year, AVG(net_sales) as avg_net_sales, MAX(net_sales) as max_net_sales" \
            " FROM sales" \
            " GROUP BY year"
    spark_df_sales_with_mean_and_max_sales_sql_generated = spark.sql(query)
    spark_df_sales_with_mean_and_max_sales_sql_generated.show()

    # pandas
    exprs_pd = {"net_sales": [np.average, np.max]}
    pd_df_sales_with_mean_and_max = pd_df_sales.groupby(by=["year"]).agg(exprs_pd)
    pd_df_sales_with_mean_and_max = pd_df_sales_with_mean_and_max.reset_index(). \
        rename(columns={"average": "avg_net_sales",
                        "amax": "max_net_sales"}). \
        sort_values(by="year", ascending=False)


def _custom_aggregation(spark, pd_df_sales, spark_df_sales):
    """
    Do custom aggregations using pyspark instead of using the existing functions such as max, min, mean etc.
    Args:
        spark: SparkSession
        pd_df_sales: Pandas df
        spark_df_sales: Spark df
    """
    # add the net_sales_div2 column
    spark_df_sales_with_net_sales_div_2 = spark_df_sales.withColumn("net_sales_div2", F.col("net_sales") / 2)
    """
    spark_df_sales_with_net_sales_div_2.show() outputs:
    +----------+---------+--------+----+----------------+
    | date | net_sales | store_id | year | net_sales_times2 |
    +----------+---------+--------+----+----------------+
    | 2022 - 02 - 01 | 10 | 1 | 2022 | 20 |
    | 2022 - 02 - 02 | 20 | 2 | 2022 | 40 |
    | 2022 - 02 - 03 | 50 | 2 | 2022 | 100 |
    | 2021 - 02 - 01 | 30 | 3 | 2021 | 60 |
    | 2021 - 02 - 02 | 40 | 4 | 2021 | 80 |
    | 2021 - 02 - 03 | 80 | 4 | 2021 | 160 |
    +----------+---------+--------+----+----------------+
    """

    """
    # pyspark
    # using GroupedData.agg
    # dictionary expression multiple metrics on the same column doesn't work for some reason. Therefore, use functions
    # exprs = {"net_sales": "mean", "net_sales": "max"}
    spark_df_sales_with_mean_and_max_sales = spark_df_sales_with_net_sales_times_2.groupBy("year"). \
        agg(F.abs(F.col("net_sales")) + F.abs(F.col("net_sales")))


    spark_df_sales_with_mean_and_max_sales.show()
    """

    # using pure sql
    spark_df_sales_with_net_sales_div_2.createOrReplaceTempView("sales")
    query = "SELECT store_id, year, SUM(net_sales) as sum1, SUM(net_sales_div2) as sum2" \
            " FROM sales" \
            " GROUP BY store_id, year"
    df_sum_each_column_separately = spark.sql(query)
    df_sum_each_column_separately.show()
    """
    df_sum_each_column_separately.show()
    Outputs: 

    """

    spark_df_sales_with_net_sales_div_2.createOrReplaceTempView("sales")
    query = "SELECT store_id, year, SUM(net_sales + net_sales_div2)" \
            " FROM sales" \
            " GROUP BY store_id, year"
    df_sum_net_sales_w_net_sales_div_2 = spark.sql(query)
    df_sum_net_sales_w_net_sales_div_2.show()
    """
    Outputs:

    """


def _custom_aggregation_mape_option_1(spark, spark_df_sales):
    """
    1. Calculate absolute percentage error (APE)
    2. Count the 'n' in each group (store, year combinations)
    3. Calculate the ape with sum (within each group)
    4. Join count n with ape_w_sum to calculate MAPE
    5. Calculate MAPE

    Args:
        spark:
        spark_df_sales:

    Returns:

    """
    # add the net_sales_div2 column
    spark_df_sales_with_net_sales_div_2 = spark_df_sales.withColumn("net_sales_div2", F.col("net_sales") / 2)
    """
    spark_df_sales_with_net_sales_div_2.show() outputs:
    +----------+---------+--------+----+--------------+
    |      date|net_sales|store_id|year|net_sales_div2|
    +----------+---------+--------+----+--------------+
    |2022-02-01|       10|       1|2022|           5.0|
    |2022-02-02|       20|       2|2022|          10.0|
    |2022-02-03|       50|       2|2022|          25.0|
    |2021-02-01|       30|       3|2021|          15.0|
    |2021-02-02|       40|       4|2021|          20.0|
    |2021-02-03|       80|       4|2021|          40.0|
    +----------+---------+--------+----+--------------+
    """

    # MAPE CALCULATION:
    # calculate absolute percentage error (ape) without summing the rows in groups
    df_ape_without_sum = spark_df_sales_with_net_sales_div_2.withColumn("ape_wo_sum",
                                                                        (F.abs(F.col("net_sales") - F.col(
                                                                            "net_sales_div2"))) / F.col("net_sales"))
    df_ape_without_sum.show()

    # count the 'n' in each group
    df_ape_without_sum.createOrReplaceTempView("df_ape_without_sum")
    query_count_n = "SELECT store_id, year, COUNT(*) as n" \
                    " FROM df_ape_without_sum" \
                    " GROUP BY store_id, year"
    df_count_n = spark.sql(query_count_n)
    df_count_n.show()

    # sum ape_wo_sum so that we calculate the ape (absolute percentage error)
    query_ape_w_sum = "SELECT store_id, year, SUM(ape_wo_sum) as ape_w_sum" \
                      " FROM df_ape_without_sum" \
                      " GROUP BY store_id, year"
    df_sum_of_each_row_separately_and_then_group_by = spark.sql(query_ape_w_sum)
    df_sum_of_each_row_separately_and_then_group_by.show()

    # join count_n dataframe with ape_with_sum_df so that we can calculate mape
    df_ape_w_sum_and_count_n = df_sum_of_each_row_separately_and_then_group_by.join(df_count_n,
                                                                                    on=["store_id", "year"],
                                                                                    how="inner")

    # calculate mape by dividing ape_w_sum to n
    df_mape = df_ape_w_sum_and_count_n.withColumn("mape", F.col("ape_w_sum") / F.col("n")).select("store_id", "year",
                                                                                                  "mape")
    df_mape.show()


def _custom_aggregation_mape_option_2(spark, spark_df_sales):
    """
    Try with window function instead of join but won't work
    Args:
        spark:
        spark_df_sales:

    Returns:

    """
    # add the net_sales_div2 column
    spark_df_sales_with_net_sales_div_2 = spark_df_sales.withColumn("net_sales_div2", F.col("net_sales") / 2)
    """
    spark_df_sales_with_net_sales_div_2.show() outputs:

    """

    # MAPE CALCULATION:
    # calculate absolute percentage error (ape) without summing the rows in groups
    df_ape_without_sum = spark_df_sales_with_net_sales_div_2.withColumn("ape_wo_sum",
                                                                        (F.abs(F.col("net_sales") - F.col(
                                                                            "net_sales_div2"))) / F.col("net_sales"))
    df_ape_without_sum.show()

    window_spec = Window.partitionBy("store_id", "year")
    df_ape_without_sum_n = df_ape_without_sum.withColumn("n", F.count("*").over(window_spec))

    # sum ape_wo_sum so that we calculate the ape (absolute percentage error)
    # won't work due to => pyspark.sql.utils.AnalysisException: expression 'df_ape_without_sum_n.n' is neither present in the group by, nor is it an aggregate...
    # a workaround could be assigning 1 to each row and here do SUM(ape_wo_sum)/SUM(n)
    df_ape_without_sum_n.createOrReplaceTempView("df_ape_without_sum_n")
    query_ape_w_sum = "SELECT store_id, year, SUM(ape_wo_sum)/n as mape" \
                      " FROM df_ape_without_sum_n" \
                      " GROUP BY store_id, year"
    df_sum_of_each_row_separately_and_then_group_by = spark.sql(query_ape_w_sum)
    df_sum_of_each_row_separately_and_then_group_by.show()


def _percentile_calculation(spark):
    """
        Example usage of percentile_approx with explode and alias.
        Args:
            spark: SparkSession

        """

    # create an example dataframe
    notes = {"year": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021,
                      2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022],
             "notes": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                       0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]}
    pd_df_notes = pd.DataFrame(data=notes)
    spark_df_notes = spark.createDataFrame(data=pd_df_notes)

    # calculate the median value for each year
    spark_df_notes_years_with_median = spark_df_notes.groupby("year").agg(
        F.percentile_approx("notes", 0.5).alias("median"))
    spark_df_notes_years_with_median.show()
    # +----+------+
    # | year | median |
    # +----+------+
    # | 2021 | 5 |
    # | 2022 | 50 |
    # +----+------+

    # only for the year 2022 calculate the quartiles
    spark_df_notes_quartiles = spark_df_notes.filter(spark_df_notes.year == 2022). \
        select(F.percentile_approx("notes", [0.25, 0.5, 0.75]).alias("quartiles"))
    spark_df_notes_quartiles.show()
    # +------------+
    # | quartiles |
    # +------------+
    # | [20, 50, 80] |
    # +------------+

    # only for the year 2022 calculate the quartiles exploded (instead of array type explode it to rows)
    spark_df_notes_quartiles_exploded = spark_df_notes_quartiles.select(
        F.explode("quartiles").alias("exploded_quartiles"))
    spark_df_notes_quartiles_exploded.show()
    # +------------------+
    # | exploded_quartiles |
    # +------------------+
    # | 20 |
    # | 50 |
    # | 80 |
    # +------------------+

    # calculate the quartiles for each year (as array type)
    spark_df_notes.groupby("year").agg(F.percentile_approx("notes", [0.25, 0.5, 0.75]).alias("quartiles")).show()
    # +----+------------+
    # | year | quartiles |
    # +----+------------+
    # | 2021 | [2, 5, 8] |
    # | 2022 | [20, 50, 80] |
    # +----+------------+

    # calculate the quartiles for each year and explode
    spark_df_notes.groupby("year").agg(
        F.explode(F.percentile_approx("notes", [0.25, 0.5, 0.75])).alias("quartiles")).show()
    # +----+---------+
    # | year | quartiles |
    # +----+---------+
    # | 2021 | 2 |
    # | 2021 | 5 |
    # | 2021 | 8 |
    # | 2022 | 20 |
    # | 2022 | 50 |
    # | 2022 | 80 |
    # +----+---------+

    # For each year calculate the difference between the 3rd quartile (75th percentile) and the 1st quartile (25th percentile)
    spark_df_notes.groupby("year").agg(
        (F.percentile_approx("notes", 0.75) - F.percentile_approx("notes", 0.25)).alias("dif_3rd_1st_quartiles")).show()
    # +----+---------------------+
    # | year | dif_3rd_1st_quartiles |
    # +----+---------------------+
    # | 2021 | 6 |
    # | 2022 | 60 |
    # +----+---------------------+

    # For each year calculate the half of the difference between the 3rd quartile (75th percentile) and the 1st quartile (25th percentile)
    spark_df_notes.groupby("year").agg(
        ((F.percentile_approx("notes", 0.75) - F.percentile_approx("notes", 0.25)) / 2).alias(
            "dif_3rd_1st_quartiles_divided_by_2")).show()

    # +----+----------------------------------+
    # | year | dif_3rd_1st_quartiles_divided_by_2 |
    # +----+----------------------------------+
    # | 2021 | 3.0 |
    # | 2022 | 30.0 |
    # +----+----------------------------------+


def _join_basic():
    pass


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Data Wrangling Examples") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    pd_df_sales, pd_df_stores = _create_pandas_dfs()

    spark_df_sales, spark_df_stores = _create_spark_df(spark, pd_df_sales, pd_df_stores)

    _immutability_check(pd_df_sales, spark_df_sales)

    _view_dataframe(pd_df_sales, spark_df_sales)

    _rename_columns(pd_df_sales, spark_df_sales)

    _drop_column(pd_df_sales, spark_df_sales)

    _filtering(pd_df_sales, spark_df_sales)

    _add_column(pd_df_sales, spark_df_sales)

    _fill_nulls(pd_df_sales, spark_df_sales)

    _aggregation(spark, pd_df_sales, spark_df_sales)

    _percentile_calculation(spark)

    _join_basic()

    _custom_aggregation(spark, pd_df_sales, spark_df_sales)

    _custom_aggregation_mape_option_1(spark, spark_df_sales)

    #_custom_aggregation_mape_option_2(spark, spark_df_sales)
