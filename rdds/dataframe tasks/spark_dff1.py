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

# Task 19: Write a code to display all the unique rows for age, number and course column
print(df.dropDuplicates(["age","gender","course"]).count()) # First Solution
print(df.select("name","gender","course").distinct().count()) # Second Solution

# ---------------------------------------------------------------------------------
anotherdf = spark.read.csv(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\OfficeData.csv",header=True,inferSchema=True);
# Task 20: Create a DF sorted on bonus in ascending order and show it
f_df = anotherdf.orderBy(anotherdf.bonus.asc()) # First Solution
# f_df = anotherdf.sort(anotherdf.bonus.asc()) # Second Solution
print(f_df.show())

# Task 21: Create a DF sorted on age and salary in descending and ascending order respectively and show it.
f_df2 = anotherdf.sort(anotherdf.age.desc(), anotherdf.salary.asc());
print(f_df2.show());

# Task 22: Create a DF sorted on age, bonus and salary in descending, descending and ascending order respectively and show it.
f_df3 = anotherdf.sort(anotherdf.age.desc(),anotherdf.bonus.desc(),anotherdf.salary.asc());
print(f_df3.show())