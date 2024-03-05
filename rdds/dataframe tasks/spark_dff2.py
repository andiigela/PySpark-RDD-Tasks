from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max, sum
df = spark.read.csv("/FileStore/tables/StudentData.csv",header=True,inferSchema=True);

# Task 23: Display the total numbers of students enrolled in each course
print(df.groupBy(df.course).agg(count("*").alias("course_students")).show());