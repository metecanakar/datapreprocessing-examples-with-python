import pandas as pd
from pyspark.sql import SparkSession


def _create_pandas_dfs():
    """
    Create 2 pandas dataframes.
    Returns:
        pd_df_sales containing sales data. pd_df_stores containing store information.

    """
    data_sales = {"date": ["2022-02-01", "2022-02-02", "2022-02-03", "2021-02-01", "2021-02-02", "2021-02-03"],
                  "net_sales": [10, 20, 50, 30, 40, 80],
                  "store_id": [1, 2, 2, 3, 4, 4]}
    pd_df_sales = pd.DataFrame(data=data_sales)

    data_stores = {"date": ["2022-02-01", "2022-02-02", "2022-02-03", "2021-02-01", "2021-02-02", "2021-02-03"],
                   "store_id": [1, 2, 2, 3, 4, 4],
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


def _aggregation(pd_df_sales, spark_df_sales):
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
