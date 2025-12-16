from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, desc, asc

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai04 - Phan tich theo nam thang") \
    .getOrCreate()

# Đọc dữ liệu
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')

print("=" * 80)
print("BAI 04: PHAN TICH SO LUONG DON HANG NHOM THEO NAM, THANG DAT HANG")
print("HIEN THI THEO NAM TANG DAN, THANG GIAM DAN")
print("=" * 80)

# Trích xuất năm và tháng từ Order_Purchase_Timestamp
orders_with_time = orders_df.withColumn("Nam", year("Order_Purchase_Timestamp")) \
                             .withColumn("Thang", month("Order_Purchase_Timestamp"))

# Nhóm theo năm và tháng, đếm số đơn hàng
time_stats = orders_with_time.groupBy("Nam", "Thang") \
    .agg(count("Order_ID").alias("So_luong_don_hang")) \
    .orderBy(asc("Nam"), desc("Thang"))

# Hiển thị kết quả
print("\nKET QUA PHAN TICH:")
print("-" * 80)
print(f"{'Nam':>6} {'Thang':>8} {'So luong don hang':>25}")
print("-" * 80)

# Collect và in kết quả
results = time_stats.collect()
for row in results:
    print(f"{row['Nam']:>6} {row['Thang']:>8} {row['So_luong_don_hang']:>25,}")

print("-" * 80)
print(f"Tong so ban ghi: {len(results)}")
print("=" * 80)

print("\nHOAN THANH BAI 04")
print("=" * 80)

spark.stop()
