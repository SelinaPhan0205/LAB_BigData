from pyspark import SparkContext, SparkConf

# Khởi tạo SparkContext
conf = SparkConf().setAppName("OccupationAverageRating")
sc = SparkContext(conf=conf)

# Đọc dữ liệu occupation
occupation_rdd = sc.textFile("occupation.txt")
# Parse occupation: (OccupationID, OccupationName)
occupation_parsed = occupation_rdd.map(lambda line: line.split(',')) \
                                   .map(lambda fields: (fields[0], fields[1]))

# Đọc dữ liệu users
users_rdd = sc.textFile("users.txt")
# Parse users: (UserID, OccupationID)
users_parsed = users_rdd.map(lambda line: line.split(',')) \
                         .map(lambda fields: (fields[0], fields[3]))

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (UserID, Rating)
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (fields[0], float(fields[2])))

# Join ratings với users để có (UserID, (Rating, OccupationID))
ratings_with_occupation_id = ratings_parsed.join(users_parsed)

# Chuyển đổi thành (OccupationID, Rating)
occupation_rating = ratings_with_occupation_id.map(lambda x: (x[1][1], x[1][0]))

# Tính tổng rating và số lượng ratings cho mỗi OccupationID
# (OccupationID, Rating) -> (OccupationID, (Rating, 1))
occupation_aggregated = occupation_rating.map(lambda x: (x[0], (x[1], 1))) \
                                          .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: (OccupationID, (TotalRatings, AverageRating))
occupation_avg = occupation_aggregated.map(lambda x: (x[0], (int(x[1][1]), x[1][0] / x[1][1])))

# Join với occupation để lấy OccupationName
occupation_with_avg = occupation_parsed.join(occupation_avg)

# Chuyển đổi thành format output: (OccupationName, TotalRatings, AverageRating)
result = occupation_with_avg.map(lambda x: (x[1][0], x[1][1][0], x[1][1][1]))

# Sắp xếp theo AverageRating giảm dần
result_sorted = result.sortBy(lambda x: x[2], ascending=False)

# In kết quả
for occupation in result_sorted.collect():
    name, total_ratings, avg_rating = occupation
    print(f"{name} - AverageRating: {avg_rating:.2f} (TotalRatings: {total_ratings})")

# Dừng SparkContext
sc.stop()
