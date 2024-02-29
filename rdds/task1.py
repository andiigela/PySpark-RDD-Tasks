from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Spark Tasks")
sc = SparkContext.getOrCreate(conf=conf);

rdd = sc.textFile(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\Second Test.txt")

# Task 1: Count the number of words in the file.
rdd2 = rdd.flatMap(lambda x: x.split());
print(rdd2.count())

# Task 2: Find distinct words in the file.
rdd3 = rdd.flatMap(lambda x: x.split()).distinct()
print(rdd3.collect())

# Task 3: Find the longest word in the file
rdd3 = rdd.flatMap(lambda x: x.split()).map(lambda x: [x,len(x)])
rdd4 = rdd3.map(lambda x: x[1]).max()
rdd5 = rdd3.filter(lambda x: x[1] == rdd4).map(lambda x: [x[0]]).flatMap(lambda x: x)
print(rdd5.collect())
