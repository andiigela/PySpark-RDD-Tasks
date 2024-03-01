# ----------------------------------------- Student Data -----------------------------------------
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Student Data")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("/FileStore/tables/StudentData.csv")
headers = rdd.first();

# Task 6: Show the number of students in the file
rdd2 = rdd.filter(lambda x: x != headers)
print(rdd2.count())

# Task 7: Show the total marks achieved by Female and Male students
rdd3 = rdd.filter(lambda x: x != headers).map(lambda x: (x.split(",")[1],int(x.split(",")[5]))).reduceByKey(lambda x,y: x+y)
print(rdd3.collect())