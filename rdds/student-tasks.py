# ----------------------------------------- Student Data -----------------------------------------
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Student Data")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("/FileStore/tables/StudentData.csv")
headers = rdd.first();

# Task 6: Show the number of students in the file
rdd2 = rdd.filter(lambda x: x != headers)
rdd2.count()