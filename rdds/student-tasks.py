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

# Task 8: Show the total number of students that have passed and failed. 50+ marks are required to pass the course
rdd4 = rdd.filter(lambda x: x != headers);
passed = rdd4.filter(lambda x: int(x.split(",")[5]) > 50).count();
failed = rdd4.filter(lambda x: int(x.split(",")[5]) <= 50).count();
print(f"Passed {passed}")
print(f"Failed {failed}")

# Task 9: Show the total number of students enrolled per course
rdd5 = rdd.filter(lambda x: x != headers).map(lambda x: (x.split(",")[3],1)).reduceByKey(lambda x,y: x+y)
print(rdd5.collect())

# Task 10: Show the total marks that students have achieved per course
rdd6 = rdd.filter(lambda x: x != headers).map(lambda x: (x.split(",")[3], int(x.split(",")[5]))).reduceByKey(lambda x,y: x+y)
print(rdd6.collect())

# Task 11: Show the average marks that students have achieved per course
rdd7 = rdd.filter(lambda x: x != headers).map(lambda x: (x.split(",")[3],(int(x.split(",")[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],(x[1][0]/x[1][1])))
print(rdd7.collect())

# Task 12: Show the minimum and maximum marks achieved per course
rdd8 = rdd.filter(lambda x: x != headers).map(lambda x: (x.split(",")[3] ,int(x.split(",")[5])))
minimum_mark = rdd8.reduceByKey(lambda x,y: x if x < y else y)
maximum_mark = rdd8.reduceByKey(lambda x,y: x if x > y else y)
print(minimum_mark.collect())
print("---")
print(maximum_mark.collect())