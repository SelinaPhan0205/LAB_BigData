from pyspark import SparkContext, SparkConf
from datetime import datetime

# Khởi tạo SparkContext
conf = SparkConf().setAppName("YearAverageRating")
sc = SparkContext(conf=conf)

# Hàm chuyển timestamp thành năm
def timestamp_to_year(timestamp):
    return datetime.fromtimestamp(int(timestamp)).year

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (Year, Rating)
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (timestamp_to_year(fields[3]), float(fields[2])))

# Tính tổng rating và số lượng ratings cho mỗi năm
# (Year, Rating) -> (Year, (Rating, 1))
year_aggregated = ratings_parsed.map(lambda x: (x[0], (x[1], 1))) \
                                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: (Year, (TotalRatings, AverageRating))
year_avg = year_aggregated.map(lambda x: (x[0], (int(x[1][1]), x[1][0] / x[1][1])))

# Sắp xếp theo năm
result_sorted = year_avg.sortBy(lambda x: x[0])

# In kết quả
for year in result_sorted.collect():
    year_num, (total_ratings, avg_rating) = year
    print(f"{year_num} - TotalRatings: {total_ratings}, AverageRating: {avg_rating:.2f}")

# Dừng SparkContext
sc.stop()
