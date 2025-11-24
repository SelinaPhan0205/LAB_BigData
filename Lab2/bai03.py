from pyspark import SparkContext, SparkConf

# Khởi tạo SparkContext
conf = SparkConf().setAppName("GenderAverageRating")
sc = SparkContext(conf=conf)

# Đọc dữ liệu movies
movies_rdd = sc.textFile("movies.txt")
# Parse movies: (MovieID, Title)
movies_parsed = movies_rdd.map(lambda line: line.split(',')) \
                           .map(lambda fields: (fields[0], fields[1]))

# Đọc dữ liệu users
users_rdd = sc.textFile("users.txt")
# Parse users: (UserID, Gender)
users_parsed = users_rdd.map(lambda line: line.split(',')) \
                         .map(lambda fields: (fields[0], fields[1]))

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (UserID, (MovieID, Rating))
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (fields[0], (fields[1], float(fields[2]))))

# Join ratings với users để có (UserID, ((MovieID, Rating), Gender))
ratings_with_gender = ratings_parsed.join(users_parsed)

# Chuyển đổi thành (MovieID, (Gender, Rating))
movie_gender_rating = ratings_with_gender.map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))

# Tính tổng và số lượng ratings cho mỗi (MovieID, Gender)
# (MovieID, (Gender, Rating)) -> ((MovieID, Gender), (Rating, 1))
movie_gender_aggregated = movie_gender_rating.map(lambda x: ((x[0], x[1][0]), (x[1][1], 1))) \
                                              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: ((MovieID, Gender), AverageRating)
movie_gender_avg = movie_gender_aggregated.map(lambda x: ((x[0][0], x[0][1]), x[1][0] / x[1][1]))

# Chuyển đổi thành (MovieID, (Gender, AverageRating))
movie_gender_avg_transformed = movie_gender_avg.map(lambda x: (x[0][0], (x[0][1], x[1])))

# Group by MovieID để có (MovieID, [(Gender1, Avg1), (Gender2, Avg2)])
movie_grouped = movie_gender_avg_transformed.groupByKey().mapValues(list)

# Join với movies để lấy Title
movies_with_gender_avg = movies_parsed.join(movie_grouped)

# Chuyển đổi và format output
def format_output(record):
    movie_id, (title, gender_avgs) = record
    # Tạo dictionary để lưu average theo gender
    gender_dict = {gender: avg for gender, avg in gender_avgs}
    
    male_avg = gender_dict.get('M', None)
    female_avg = gender_dict.get('F', None)
    
    # Format output
    if male_avg is not None and female_avg is not None:
        return (title, f"Male_Avg: {male_avg:.2f}, Female_Avg: {female_avg:.2f}")
    elif male_avg is not None:
        return (title, f"Male_Avg: {male_avg:.2f}, Female_Avg: NA")
    elif female_avg is not None:
        return (title, f"Male_Avg: NA, Female_Avg: {female_avg:.2f}")
    else:
        return (title, "No data")

result = movies_with_gender_avg.map(format_output)

# Sắp xếp theo title
result_sorted = result.sortBy(lambda x: x[0])

# In kết quả
for movie in result_sorted.collect():
    title, stats = movie
    print(f"{title} - {stats}")

# Dừng SparkContext
sc.stop()
