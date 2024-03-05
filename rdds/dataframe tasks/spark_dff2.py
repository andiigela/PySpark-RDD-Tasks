from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max, sum
spark = SparkSession.builder.appName("Quiz Dff2").getOrCreate()
df = spark.read.csv("/FileStore/tables/StudentData.csv",header=True,inferSchema=True);

# Task 23: Display the total numbers of students enrolled in each course
print(df.groupBy(df.course).agg(count("*").alias("course_students")).show());

# Task 24: Display the total number of male and female students enrolled in each course
print(df.groupBy(df.course,df.gender).agg(count("*")).show());

# Task 25: Display the total marks achieved by each gender in each course
print(df.groupBy(df.gender,df.course).sum("marks").show());

# Task 26: Display the minimum, maximum and average marks achieved in each course by each age group
print(df.groupBy(df.course,df.age).agg(min("marks").alias("min_marks"),max("marks").alias("max_marks"),avg("marks").alias("avg_marks")).show())