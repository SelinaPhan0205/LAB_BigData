from pyspark import SparkContext, SparkConf

# Khởi tạo SparkContext
conf = SparkConf().setAppName("AgeGroupAverageRating")
sc = SparkContext(conf=conf)

# Hàm phân nhóm tuổi
def get_age_group(age):
    age = int(age)
    if age <= 18:
        return "0-18"
    elif age <= 35:
        return "18-35"
    elif age <= 50:
        return "35-50"
    else:
        return "50+"

# Đọc dữ liệu movies
movies_rdd = sc.textFile("movies.txt")
# Parse movies: (MovieID, Title)
movies_parsed = movies_rdd.map(lambda line: line.split(',')) \
                           .map(lambda fields: (fields[0], fields[1]))

# Đọc dữ liệu users
users_rdd = sc.textFile("users.txt")
# Parse users: (UserID, AgeGroup)
users_parsed = users_rdd.map(lambda line: line.split(',')) \
                         .map(lambda fields: (fields[0], get_age_group(fields[2])))

# Đọc và kết hợp cả 2 file ratings
ratings_1_rdd = sc.textFile("ratings_1.txt")
ratings_2_rdd = sc.textFile("ratings_2.txt")

# Kết hợp 2 RDD ratings
ratings_combined = ratings_1_rdd.union(ratings_2_rdd)

# Parse ratings: (UserID, (MovieID, Rating))
ratings_parsed = ratings_combined.map(lambda line: line.split(',')) \
                                  .map(lambda fields: (fields[0], (fields[1], float(fields[2]))))

# Join ratings với users để có (UserID, ((MovieID, Rating), AgeGroup))
ratings_with_age = ratings_parsed.join(users_parsed)

# Chuyển đổi thành (MovieID, (AgeGroup, Rating))
movie_age_rating = ratings_with_age.map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))

# Tính tổng và số lượng ratings cho mỗi (MovieID, AgeGroup)
# (MovieID, (AgeGroup, Rating)) -> ((MovieID, AgeGroup), (Rating, 1))
movie_age_aggregated = movie_age_rating.map(lambda x: ((x[0], x[1][0]), (x[1][1], 1))) \
                                        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình: ((MovieID, AgeGroup), AverageRating)
movie_age_avg = movie_age_aggregated.map(lambda x: ((x[0][0], x[0][1]), x[1][0] / x[1][1]))

# Chuyển đổi thành (MovieID, (AgeGroup, AverageRating))
movie_age_avg_transformed = movie_age_avg.map(lambda x: (x[0][0], (x[0][1], x[1])))

# Group by MovieID để có (MovieID, [(AgeGroup1, Avg1), (AgeGroup2, Avg2)])
movie_grouped = movie_age_avg_transformed.groupByKey().mapValues(list)

# Join với movies để lấy Title
movies_with_age_avg = movies_parsed.join(movie_grouped)

# Chuyển đổi và format output
def format_output(record):
    movie_id, (title, age_avgs) = record
    # Tạo dictionary để lưu average theo age group
    age_dict = {age_group: avg for age_group, avg in age_avgs}
    
    # Định nghĩa thứ tự các nhóm tuổi
    age_groups = ["0-18", "18-35", "35-50", "50+"]
    
    # Tạo list các giá trị average theo thứ tự
    avg_list = []
    for age_group in age_groups:
        if age_group in age_dict:
            avg_list.append(f"{age_group}: {age_dict[age_group]:.2f}")
        else:
            avg_list.append(f"{age_group}: NA")
    
    return (title, "[" + ", ".join(avg_list) + "]")

result = movies_with_age_avg.map(format_output)

# Sắp xếp theo title
result_sorted = result.sortBy(lambda x: x[0])

# In kết quả
for movie in result_sorted.collect():
    title, stats = movie
    print(f"{title} - {stats}")

# Dừng SparkContext
sc.stop()
