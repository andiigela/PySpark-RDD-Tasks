from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Spark Tasks")
sc = SparkContext.getOrCreate(conf=conf);

rdd = sc.textFile(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\Second Test.txt")
print(rdd.collect())
