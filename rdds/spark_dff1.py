from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
spark = SparkSession.builder.appName("First Spark").getOrCreate();

df = spark.read.csv(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\StudentData.csv",header=True,inferSchema=True);

# Task 14: Create a new column in the DF for total marks and let the total marks be 120
df = df.withColumn("total marks", lit(120))

# Task 15: Create a new column average to calc the average marks of the student (marks/total marks)*100
df = df.withColumn("average",(col("marks") / col("total marks"))*100)
print(df.show())

# Task 16: Filter out all students who have achieved more than 80% marks in OOP course and save it in new DF.
df2 = df.filter((df.average > 80.0) & (df.course == "OOP") )
print(df2.show())

# Task 17: Filter out all students who have achieved more than 60% marks in Cloud course and save it in new DF.
df3 = df.filter((df.average > 60.0) & (df.course == "Cloud"))
print(df3.show())

# Task 18: Print the names and marks of all students from the above DFs
print(df.select(df.name,df.marks).show())