from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a sample DataFrame
data = [
    (1001, "2024-03-10", 1, "Alice", "Istanbul", [101, 102, 103], [100, 200, 300], [2, 2, 1]),
    (1002, "2024-03-11", 2, "Jane", "NY", [103, 104], [300, 400], [4, 3])
    ]

# initial dataframe (not normalized)
df = spark.createDataFrame(data, ["OrderID", "OrderDate", "CustomerID", "CustomerName", "CustomerAddress", "ProductID", "Price", "OrderQuantity"])
df.show()

# ----------------- 1NF start -----------------
# 1NF requires that the table is a two-dimensional table with no repeating groups or arrays in single column
# (no multivalued attributes) which means that each cell should contain a single value (atomic value) 

# 1NF requires that each column be atomic, meaning no multi-valued attributes or nested arrays. The initial table violates 1NF because ProductID, Price, and OrderQuantity are arrays.
# Transformation to 1NF:
# Unnest the ProductID, Price, and OrderQuantity arrays.
# Duplicate rows for each order where there are multiple products.
df_1nf_intermediate = df.withColumn("zipped_cols", F.explode(F.arrays_zip("ProductID", "Price", "OrderQuantity")))
"""
+-------+----------+----------+------------+---------------+---------------+---------------+-------------+-------------+
|OrderID| OrderDate|CustomerID|CustomerName|CustomerAddress|      ProductID|          Price|OrderQuantity|  zipped_cols|
+-------+----------+----------+------------+---------------+---------------+---------------+-------------+-------------+
|   1001|2024-03-10|         1|       Alice|       Istanbul|[101, 102, 103]|[100, 200, 300]|    [2, 2, 1]|{101, 100, 2}|
|   1001|2024-03-10|         1|       Alice|       Istanbul|[101, 102, 103]|[100, 200, 300]|    [2, 2, 1]|{102, 200, 2}|
|   1001|2024-03-10|         1|       Alice|       Istanbul|[101, 102, 103]|[100, 200, 300]|    [2, 2, 1]|{103, 300, 1}|
|   1002|2024-03-11|         2|        Jane|             NY|     [103, 104]|     [300, 400]|       [4, 3]|{103, 300, 4}|
|   1002|2024-03-11|         2|        Jane|             NY|     [103, 104]|     [300, 400]|       [4, 3]|{104, 400, 3}|
+-------+----------+----------+------------+---------------+---------------+---------------+-------------+-------------+
"""
df_1nf = df_1nf_intermediate.select("OrderID", "OrderDate", "CustomerID", "CustomerName", "CustomerAddress", 
                           F.col("zipped_cols.ProductID"), F.col("zipped_cols.Price"), F.col("zipped_cols.OrderQuantity").alias("OrderQuantity"))

# Show the result (1NF comformant DataFrame (no repeating groups or arrays in single column (no multivalued attributes)))
# However, it still contains insertion, deletion, and update anomalies:
# Insertion anomaly: Cannot add a new product without creating a new row for the order
# Deletion anomaly: If a product is deleted, the entire order is deleted
# Update anomaly: If the price of a product changes, all rows with that product need to be updated

df_1nf.show()
"""
+-------+----------+----------+------------+---------------+---------+-----+-------------+
|OrderID| OrderDate|CustomerID|CustomerName|CustomerAddress|ProductID|Price|OrderQuantity|
+-------+----------+----------+------------+---------------+---------+-----+-------------+
|   1001|2024-03-10|         1|       Alice|       Istanbul|      101|  100|            2|
|   1001|2024-03-10|         1|       Alice|       Istanbul|      102|  200|            2|
|   1001|2024-03-10|         1|       Alice|       Istanbul|      103|  300|            1|
|   1002|2024-03-11|         2|        Jane|             NY|      103|  300|            4|
|   1002|2024-03-11|         2|        Jane|             NY|      104|  400|            3|
+-------+----------+----------+------------+---------------+---------+-----+-------------+
"""



# ----------------- 2NF start -----------------
# 2NF requires removing partial functional dependencies, where an attribute is functionally dependent on part of a composite primary key. Transitive dependencies are allowed.
# Therefore, we need to convert all the partial functional dependencies to fully functional dependencies.
# In the output of 1NF , the composite primary key is (OrderID, ProductID).
# OrderDate, CustomerID, CustomerName, and CustomerAddress are only functionally dependent on OrderID, not on ProductID. Therefore, they are partially functionally dependent on the composite primary key.
# Price only functionally depends on ProductID, not on OrderID. Therefore, it is partially functionally dependent on the composite primary key.
# However, to define the OrderQuantity, we have to know both the OrderID and the ProductID. Therefore, the OrderQuantity is fully functionally dependent on the composite primary key (OrderID, ProductID).
# Now, we need to create separate tables for Order and Product, to remove partial functional dependency and create fully functional dependency in each table.

# Orderline table (3NF)
orderline = df_1nf.select("OrderID", "ProductID", "OrderQuantity")
orderline.show()
"""
+-------+---------+-------------+
|OrderID|ProductID|OrderQuantity|
+-------+---------+-------------+
|   1001|      101|            2|
|   1001|      102|            2|
|   1001|      103|            1|
|   1002|      103|            4|
|   1002|      104|            3|
+-------+---------+-------------+
"""

# Product table (3NF)
# Price is now fully functionally dependent on the primary key ProductID
product = df_1nf.select("ProductID", "Price")
product.show()
"""
+---------+-----+
|ProductID|Price|
+---------+-----+
|      101|  100|
|      102|  200|
|      103|  300|
|      103|  300|
|      104|  400|
+---------+-----+
"""

# CustomerOrder table (2NF)
# Now, OrderDate, CustomerID, CustomerName, and CustomerAddress are fully functionally dependent on the primary key OrderID
# In other words, OrderID determines OrderDate, CustomerID, CustomerName, and CustomerAddress.
# But, there is transitive dependency between CustomerID and CustomerName, and CustomerAddress.
# Which means that CustomerID determines CustomerName and CustomerAddress.
# TL;DR OrderID determines CustomerID, and CustomerID determines CustomerName and CustomerAddress. This means that there is a transitive dependency between CustomerID and CustomerName, and CustomerAddress.
# Solution: Non-key determinant (CustomerID) with transitive dependency (CustomerName, and CustomerAddress) should be moved to a separate table:
customerorder = df_1nf.select("OrderID", "OrderDate", "CustomerID", "CustomerName", "CustomerAddress").distinct()
customerorder.show()
"""
+-------+----------+----------+------------+---------------+
|OrderID| OrderDate|CustomerID|CustomerName|CustomerAddress|
+-------+----------+----------+------------+---------------+
|   1001|2024-03-10|         1|       Alice|       Istanbul|
|   1002|2024-03-11|         2|        Jane|             NY|
+-------+----------+----------+------------+---------------+
"""

# ---------------- 3NF start ---------------------
# 3NF requires that there are no transitive dependencies.
# The CustomerOrder table has a transitive dependency between CustomerID and CustomerName, and CustomerAddress.
# To remove the transitive dependency, we need to create a separate table for Customer.
# Non-key determinant (CustomerID) becomes the primary key of the new table, and the transitive dependent attributes become the columns of the new table.
# And the non-key determinant (CustomerID) stays in the Order table as a foreign key.
# Customer table (3NF)
customer = customerorder.select("CustomerID", "CustomerName", "CustomerAddress").distinct()
customer.show()
"""
+----------+------------+---------------+
|CustomerID|CustomerName|CustomerAddress|
+----------+------------+---------------+
|         1|       Alice|       Istanbul|
|         2|        Jane|             NY|
+----------+------------+---------------+
"""

# Order (3NF)
order = customerorder.select("OrderID", "OrderDate", "CustomerID").distinct()
order.show()
"""
+-------+----------+----------+
|OrderID| OrderDate|CustomerID|
+-------+----------+----------+
|   1001|2024-03-10|         1|
|   1002|2024-03-11|         2|
+-------+----------+----------+
"""

# ------ 3NF is achieved  (Transitive dependencies converted to full dependency ------ 
