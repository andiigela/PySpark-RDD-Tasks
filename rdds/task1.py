from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Spark Tasks")
sc = SparkContext.getOrCreate(conf=conf);

rdd = sc.textFile(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\Second Test.txt")

# Task 1: Count the number of words in the file.
rdd2 = rdd.flatMap(lambda x: x.split());
print(rdd2.count())