from pyspark import SparkContext, SparkConf

# Khởi tạo SparkContext
conf = SparkConf().setAppName("GenreAverageRating")
sc = SparkContext(conf=conf)

# Đọc dữ liệu movies
movies_rdd = sc.textFile("movies.txt")
# Parse movies: (MovieID, Genres)
movies_parsed = movies_rdd.map(lambda line: line.split(',')) \
                           .map(lambda fields: (fields[0], fields[2]))

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (MovieID, Rating)
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (fields[1], float(fields[2])))

# Join movies với ratings để có (MovieID, (Genres, Rating))
movies_ratings_joined = movies_parsed.join(ratings_parsed)

# Tách genres và tạo (Genre, Rating) cho mỗi genre
# (MovieID, (Genres, Rating)) -> [(Genre1, Rating), (Genre2, Rating), ...]
genre_ratings = movies_ratings_joined.flatMap(
    lambda x: [(genre, x[1][1]) for genre in x[1][0].split('|')]
)

# Tính tổng rating và số lượng ratings cho mỗi Genre
# (Genre, Rating) -> (Genre, (Rating, 1)) -> (Genre, (SumRatings, Count))
genre_aggregated = genre_ratings.map(lambda x: (x[0], (x[1], 1))) \
                                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: (Genre, (AverageRating, TotalRatings))
genre_with_avg = genre_aggregated.map(lambda x: (x[0], (x[1][0] / x[1][1], int(x[1][1]))))

# Sắp xếp theo AverageRating giảm dần
result_sorted = genre_with_avg.sortBy(lambda x: x[1][0], ascending=False)

# In kết quả
for genre in result_sorted.collect():
    genre_name, (avg_rating, total_ratings) = genre
    print(f"{genre_name} - AverageRating: {avg_rating:.2f} (TotalRatings: {total_ratings})")

# Dừng SparkContext
sc.stop()
