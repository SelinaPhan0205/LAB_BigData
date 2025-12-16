from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, year, desc, col, count

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai06 - Tinh doanh thu 2024 theo danh muc") \
    .getOrCreate()

# Đọc dữ liệu
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='"')
order_items_df = spark.read.csv("Order_Items.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='"')
products_df = spark.read.csv("Products.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='"')

print("=" * 80)
print("BAI 06: TINH DOANH THU (GIA SAN PHAM + PHI VAN CHUYEN)")
print("TRONG NAM 2024 VA NHOM THEO DANH MUC SAN PHAM")
print("=" * 80)

# Lọc đơn hàng năm 2024
orders_2024 = orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024)

print(f"\nSo luong don hang trong nam 2024: {orders_2024.count():,}")

# Join Order_Items với Orders để lấy thông tin thời gian
items_with_orders = order_items_df.join(
    orders_2024.select("Order_ID", "Order_Purchase_Timestamp"),
    on="Order_ID",
    how="inner"
)

# Join với Products để lấy danh mục sản phẩm
items_with_category = items_with_orders.join(
    products_df.select("Product_ID", "Product_Category_Name"),
    on="Product_ID",
    how="inner"
)

# Tính doanh thu = Price + Freight_Value cho mỗi sản phẩm
items_with_revenue = items_with_category.withColumn(
    "Doanh_thu",
    col("Price") + col("Freight_Value")
)

# Nhóm theo danh mục sản phẩm và tính tổng doanh thu
revenue_by_category = items_with_revenue.groupBy("Product_Category_Name") \
    .agg(
        spark_sum("Doanh_thu").alias("Tong_doanh_thu"),
        spark_sum("Price").alias("Tong_gia_san_pham"),
        spark_sum("Freight_Value").alias("Tong_phi_van_chuyen"),
        count("Order_Item_ID").alias("So_luong_san_pham")
    ) \
    .orderBy(desc("Tong_doanh_thu"))

# Tính tổng doanh thu toàn bộ
total_revenue = items_with_revenue.agg(
    spark_sum("Doanh_thu").alias("total")
).collect()[0]["total"]

print(f"Tong doanh thu nam 2024: {total_revenue:,.2f}")

print("\n" + "=" * 80)
print("DOANH THU THEO DANH MUC SAN PHAM - NAM 2024")
print("=" * 80)
print(f"{'Danh muc san pham':<30} {'Doanh thu':>15} {'Gia SP':>15} {'Phi VC':>15} {'So luong':>10}")
print("-" * 80)

# Hiển thị kết quả
results = revenue_by_category.collect()
for row in results:
    category = row['Product_Category_Name'] if row['Product_Category_Name'] else "Unknown"
    revenue = row['Tong_doanh_thu']
    price = row['Tong_gia_san_pham']
    freight = row['Tong_phi_van_chuyen']
    quantity = row['So_luong_san_pham']
    percentage = (revenue / total_revenue) * 100
    
    print(f"{category:<30} {revenue:>15,.2f} {price:>15,.2f} {freight:>15,.2f} {quantity:>10,}")

print("-" * 80)
print(f"{'TONG CONG':<30} {total_revenue:>15,.2f}")
print("=" * 80)

# Top 10 danh mục có doanh thu cao nhất
print("\n" + "=" * 80)
print("TOP 10 DANH MUC CO DOANH THU CAO NHAT")
print("=" * 80)
print(f"{'STT':>5} {'Danh muc':<30} {'Doanh thu':>20} {'Ty le (%)':>15}")
print("-" * 80)

top_10 = results[:10]
for idx, row in enumerate(top_10, 1):
    category = row['Product_Category_Name'] if row['Product_Category_Name'] else "Unknown"
    revenue = row['Tong_doanh_thu']
    percentage = (revenue / total_revenue) * 100
    print(f"{idx:>5} {category:<30} {revenue:>20,.2f} {percentage:>14.2f}%")

print("-" * 80)
top_10_total = sum(row['Tong_doanh_thu'] for row in top_10)
top_10_percentage = (top_10_total / total_revenue) * 100
print(f"{'':>5} {'Tong Top 10':<30} {top_10_total:>20,.2f} {top_10_percentage:>14.2f}%")
print("=" * 80)

print(f"\nTong so danh muc san pham: {len(results)}")
print("\nHOAN THANH BAI 06")
print("=" * 80)

spark.stop()
