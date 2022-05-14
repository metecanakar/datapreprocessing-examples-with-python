import pandas as pd
from pyspark.sql import SparkSession


def _create_pandas_dfs():
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
    spark_df_sales = spark.createDataFrame(data=pd_df_sales)
    spark_df_stores = spark.createDataFrame(data=pd_df_stores)

    return spark_df_sales, spark_df_stores


def _view_dataframe(pd_df_sales, spark_df_sales):
    # pandas
    print(pd_df_sales)
    # PySpark
    spark_df_sales.show()


def _rename_columns(pd_df_sales, spark_df_sales):
    # pandas
    pd_df_sales.columns
    pd_df_sales = pd_df_sales.rename(columns={"date": "date_new"})
    print(pd_df_sales)

    # PySpark
    spark_df_sales = spark_df_sales.withColumnRenamed("date", "date_new")
    spark_df_sales.show()


def _drop_column(pd_df_sales, spark_df_sales):
    # pandas
    # axis=1 is column
    pd_df_sales = pd_df_sales.drop("date", axis=1)
    print(pd_df_sales)

    # PySpark
    spark_df_sales = spark_df_sales.drop("date")
    spark_df_sales.show()


def _filtering(pd_df_sales, spark_df_sales):
    # pandas
    # get values with net_sales < 30
    #     date  net_sales  store_id
    # 0  2022-02-01         10         1
    # 0  2022-02-01         10         1
    pd_df_sales_net_sales_smaller_than_30 = pd_df_sales[pd_df_sales.net_sales < 30]
    print(pd_df_sales_net_sales_smaller_than_30)

    # get values with net_sales < 30 and date smaller than 2022_02_02
    # pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02
    #     date  net_sales  store_id
    # 0  2022-02-01         10         1
    pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02 = pd_df_sales[
        (pd_df_sales.net_sales < 30) & (pd_df_sales.date < "2022-02-02")]
    print(pd_df_sales_net_sales_smaller_than_30_and_date_smaller_than_2022_02_02)

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


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Data Wrangling Examples") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    pd_df_sales, pd_df_stores = _create_pandas_dfs()

    spark_df_sales, spark_df_stores = _create_spark_df(spark, pd_df_sales, pd_df_stores)

    _view_dataframe(pd_df_sales, spark_df_sales)

    _rename_columns(pd_df_sales, spark_df_sales)

    _drop_column(pd_df_sales, spark_df_sales)

    _filtering(pd_df_sales, spark_df_sales)
