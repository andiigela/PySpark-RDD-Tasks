from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
spark = SparkSession.builder.appName("First Spark").getOrCreate();

df = spark.read.csv(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\StudentData.csv",header=True,inferSchema=True);

# Task 14: Create a new column in the DF for total marks and let the total marks be 120
df = df.withColumn("total marks", lit(120))

# Task 15: Create a new column average to calc the average marks of the student (marks/total marks)*100
df = df2.withColumn("average",(col("marks") / col("total marks"))*100)

