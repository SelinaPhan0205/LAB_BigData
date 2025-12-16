from pyspark.sql import SparkSession

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai02 - Thong ke tong so") \
    .getOrCreate()

# Đọc dữ liệu
customers_df = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')
order_items_df = spark.read.csv("Order_Items.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')

print("=" * 80)
print("BAI 02: THONG KE TONG SO DON HANG, SO LUONG KHACH HANG VA NGUOI BAN")
print("=" * 80)

# 1. Tổng số đơn hàng
total_orders = orders_df.count()
print(f"\n1. Tong so don hang: {total_orders:,}")

# 2. Tổng số lượng khách hàng
total_customers = customers_df.count()
print(f"\n2. Tong so luong khach hang: {total_customers:,}")

# 3. Tổng số người bán (sellers)
total_sellers = order_items_df.select("Seller_ID").distinct().count()
print(f"\n3. Tong so nguoi ban (sellers): {total_sellers:,}")

print("\n" + "=" * 80)
print("TOM TAT KET QUA")
print("=" * 80)
print(f"Don hang:     {total_orders:>10,}")
print(f"Khach hang:   {total_customers:>10,}")
print(f"Nguoi ban:    {total_sellers:>10,}")
print("=" * 80)

print("\nHOAN THANH BAI 02")
print("=" * 80)

spark.stop()
