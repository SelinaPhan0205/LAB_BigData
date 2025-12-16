from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai01 - Doc du lieu CSV") \
    .getOrCreate()

# Đọc dữ liệu từ các file CSV với infer schema
print("=" * 80)
print("BAI 01: DOC DU LIEU TU CAC FILE CSV VA TU SUY RA KIEU DU LIEU CHO MOI COT")
print("=" * 80)

# Đọc Customer_List
customers_df = spark.read.csv(
    "Customer_List.csv",
    header=True,
    inferSchema=True,
    sep=";",
    multiLine=True,
    escape='\"'
)

# Đọc Orders
orders_df = spark.read.csv(
    "Orders.csv",
    header=True,
    inferSchema=True,
    sep=";",
    multiLine=True,
    escape='\"'
)

# Đọc Order_Items
order_items_df = spark.read.csv(
    "Order_Items.csv",
    header=True,
    inferSchema=True,
    sep=";",
    multiLine=True,
    escape='\"'
)

# Đọc Order_Reviews
order_reviews_df = spark.read.csv(
    "Order_Reviews.csv",
    header=True,
    inferSchema=True,
    sep=";",
    multiLine=True,
    escape='\"'
)

# Đọc Products
products_df = spark.read.csv(
    "Products.csv",
    header=True,
    inferSchema=True,
    sep=";",
    multiLine=True,
    escape='\"'
)

# In schema của từng DataFrame
print("\n1. CUSTOMER_LIST SCHEMA:")
print("-" * 80)
customers_df.printSchema()

print("\n2. ORDERS SCHEMA:")
print("-" * 80)
orders_df.printSchema()

print("\n3. ORDER_ITEMS SCHEMA:")
print("-" * 80)
order_items_df.printSchema()

print("\n4. ORDER_REVIEWS SCHEMA:")
print("-" * 80)
order_reviews_df.printSchema()

print("\n5. PRODUCTS SCHEMA:")
print("-" * 80)
products_df.printSchema()

# Hiển thị một vài dòng dữ liệu mẫu
print("\n" + "=" * 80)
print("SAMPLE DATA")
print("=" * 80)

print("\nCustomer_List (5 rows):")
print("-" * 80)
customers_df.show(5, truncate=False)

print("\nOrders (5 rows):")
print("-" * 80)
orders_df.show(5, truncate=False)

print("\nOrder_Items (5 rows):")
print("-" * 80)
order_items_df.show(5, truncate=False)

print("\nOrder_Reviews (5 rows):")
print("-" * 80)
order_reviews_df.show(5, truncate=False)

print("\nProducts (5 rows):")
print("-" * 80)
products_df.show(5, truncate=False)

print("\n" + "=" * 80)
print("HOAN THANH BAI 01")
print("=" * 80)

spark.stop()
