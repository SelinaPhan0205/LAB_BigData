from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai03 - Phan tich theo quoc gia") \
    .getOrCreate()

# Đọc dữ liệu
customers_df = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')

print("=" * 80)
print("BAI 03: PHAN TICH SO LUONG DON HANG THEO QUOC GIA")
print("SAP XEP THEO THU TU GIAM DAN")
print("=" * 80)

# Join Orders với Customers để lấy thông tin quốc gia
orders_with_country = orders_df.join(
    customers_df.select("Customer_Trx_ID", "Customer_Country"),
    on="Customer_Trx_ID",
    how="inner"
)

# Đếm số lượng đơn hàng theo quốc gia và sắp xếp giảm dần
country_stats = orders_with_country.groupBy("Customer_Country") \
    .agg(count("Order_ID").alias("So_luong_don_hang")) \
    .orderBy(desc("So_luong_don_hang"))

# Hiển thị kết quả
print("\nKET QUA PHAN TICH:")
print("-" * 80)
print(f"{'Quoc gia':<30} {'So luong don hang':>20}")
print("-" * 80)

# Collect và in kết quả
results = country_stats.collect()
for row in results:
    print(f"{row['Customer_Country']:<30} {row['So_luong_don_hang']:>20,}")

print("-" * 80)
print(f"Tong so quoc gia: {len(results)}")
print("=" * 80)

print("\nHOAN THANH BAI 03")
print("=" * 80)

spark.stop()
