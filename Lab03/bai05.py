from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, when, expr

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Bai05 - Thong ke danh gia") \
    .getOrCreate()

# Đọc dữ liệu
reviews_df = spark.read.csv("Order_Reviews.csv", header=True, inferSchema=True, sep=";", multiLine=True, escape='\"')

print("=" * 80)
print("BAI 05: THONG KE DIEM DANH GIA TRUNG BINH VA SO LUONG DANH GIA")
print("THEO TUNG MUC (1 DEN 5)")
print("LUU Y: XU LY GIA TRI NGOAI LE VA NULL TRONG COT REVIEW_SCORE")
print("=" * 80)

# Chuyển đổi Review_Score sang kiểu integer sử dụng try_cast để xử lý lỗi
reviews_df = reviews_df.withColumn("Review_Score", expr("try_cast(Review_Score as int)"))

# Lọc bỏ các giá trị NULL và ngoại lệ (chỉ giữ điểm từ 1 đến 5)
valid_reviews = reviews_df.filter(
    (col("Review_Score").isNotNull()) & 
    (col("Review_Score") >= 1) & 
    (col("Review_Score") <= 5)
)

print("\nTHONG TIN VE DU LIEU:")
print("-" * 80)
total_reviews = reviews_df.count()
valid_count = valid_reviews.count()
invalid_count = total_reviews - valid_count

print(f"Tong so danh gia: {total_reviews:,}")
print(f"Danh gia hop le (1-5): {valid_count:,}")
print(f"Danh gia khong hop le (NULL hoac ngoai le): {invalid_count:,}")

# Tính điểm đánh giá trung bình
avg_score = valid_reviews.agg(avg("Review_Score").alias("Diem_trung_binh")).collect()[0]["Diem_trung_binh"]

print(f"\nDiem danh gia trung binh: {avg_score:.2f}")

# Đếm số lượng đánh giá theo từng mức (1 đến 5)
print("\n" + "=" * 80)
print("SO LUONG DANH GIA THEO TUNG MUC DIEM")
print("=" * 80)
print(f"{'Muc diem':>15} {'So luong danh gia':>25} {'Ty le (%)':>15}")
print("-" * 80)

rating_stats = valid_reviews.groupBy("Review_Score") \
    .agg(count("Review_ID").alias("So_luong")) \
    .orderBy("Review_Score")

results = rating_stats.collect()
for row in results:
    score = int(row["Review_Score"])
    count_val = row["So_luong"]
    percentage = (count_val / valid_count) * 100
    print(f"{score:>15} {count_val:>25,} {percentage:>14.2f}%")

print("-" * 80)
print(f"{'Tong cong':>15} {valid_count:>25,} {100.00:>14.2f}%")
print("=" * 80)

# Thống kê bổ sung
print("\nTHONG KE BO SUNG:")
print("-" * 80)
positive_reviews = valid_reviews.filter(col("Review_Score") >= 4).count()
negative_reviews = valid_reviews.filter(col("Review_Score") <= 2).count()
neutral_reviews = valid_reviews.filter(col("Review_Score") == 3).count()

print(f"Danh gia tich cuc (4-5 sao): {positive_reviews:,} ({(positive_reviews/valid_count)*100:.2f}%)")
print(f"Danh gia trung binh (3 sao): {neutral_reviews:,} ({(neutral_reviews/valid_count)*100:.2f}%)")
print(f"Danh gia tieu cuc (1-2 sao): {negative_reviews:,} ({(negative_reviews/valid_count)*100:.2f}%)")
print("=" * 80)

print("\nHOAN THANH BAI 05")
print("=" * 80)

spark.stop()
