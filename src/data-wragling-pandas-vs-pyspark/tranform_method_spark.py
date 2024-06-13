from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_new_age_category(df):
    df = df.withColumn("age_category", 
                       F.when(df.age < 18, "child")
                       .when((df.age >= 18) & (df.age <65), "adult")
                       .otherwise("retired"))
    
    return df

if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()
    rows = [("Mete", 32), ("John", 16), ("Jane", 88)]
    columns = ["name", "age"]

    df = spark.createDataFrame(rows, columns)
    df.show()

    # Applying transformation directly
    df = create_new_age_category(df)
    df.show()

     # Applying transformation using the transform method
    df = df.transform(create_new_age_category)
    df.show()