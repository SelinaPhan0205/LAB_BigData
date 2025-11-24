from pyspark import SparkContext, SparkConf

# Khởi tạo SparkContext
conf = SparkConf().setAppName("MovieAverageRating")
sc = SparkContext(conf=conf)

# Đọc dữ liệu movies
movies_rdd = sc.textFile("movies.txt")
# Parse movies: (MovieID, Title)
movies_parsed = movies_rdd.map(lambda line: line.split(',')) \
                           .map(lambda fields: (fields[0], fields[1]))

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (MovieID, Rating)
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (fields[1], float(fields[2])))

# Tính tổng rating và số lượng ratings cho mỗi MovieID
# (MovieID, Rating) -> (MovieID, (Rating, 1)) -> (MovieID, (SumRatings, Count))
ratings_aggregated = ratings_parsed.map(lambda x: (x[0], (x[1], 1))) \
                                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: (MovieID, (AverageRating, TotalRatings))
ratings_with_avg = ratings_aggregated.map(lambda x: (x[0], (x[1][0] / x[1][1], x[1][1])))

# Join với movies để lấy Title: (MovieID, (Title, (AverageRating, TotalRatings)))
movies_with_ratings = movies_parsed.join(ratings_with_avg)

# Chuyển đổi thành format output: (Title, AverageRating, TotalRatings)
result = movies_with_ratings.map(lambda x: (x[1][0], x[1][1][0], int(x[1][1][1])))

# Sắp xếp theo AverageRating giảm dần (descending)
result_sorted = result.sortBy(lambda x: x[1], ascending=False)

# In kết quả cho tất cả các phim
for movie in result_sorted.collect():
    title, avg_rating, total_ratings = movie
    print(f"{title} AverageRating: {avg_rating:.2f} (TotalRatings: {total_ratings})")

print("\n" + "="*80 + "\n")

# Lọc các phim có ít nhất 5 lượt đánh giá
movies_min_5_ratings = result.filter(lambda x: x[2] >= 5)

# Tìm phim có điểm trung bình cao nhất
if movies_min_5_ratings.count() > 0:
    highest_rated = movies_min_5_ratings.sortBy(lambda x: x[1], ascending=False).first()
    title, avg_rating, total_ratings = highest_rated
    print(f"{title} is the highest rated movie with an average rating of {avg_rating:.2f} among movies with at least 5 ratings.")
else:
    print("No movies found with at least 5 ratings.")

# Dừng SparkContext
sc.stop()
