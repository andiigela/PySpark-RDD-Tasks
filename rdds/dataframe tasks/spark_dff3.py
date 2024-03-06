from pyspark.sql import SparkSession
from pyspark.sql.functions import count,sum,avg,min, max,col

spark = SparkSession.builder.appName("Fifth Quiz").getOrCreate();
df = spark.read.csv(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\OfficeDataProject.csv",header=True,inferSchema=True);

# Task 27: Print the total numbers of employees in the company
print(df.dropDuplicates(["employee_id"]).count()) # or df.count();

# Task 28: Print the total numbers of departments in the company
print(df.select("department").distinct().count()) # or df.dropDuplicates(["department"]).count()

# Task 29: Print the departments name of the company
print(df.select("department").distinct().show());

# Task 30: Print the total number of employees in each department
print(df.groupBy(["department"]).agg(count("*")).show());

# Task 31: Print the total number of employees in each state
print(df.groupBy(["state"]).agg(count("*")).show());

# Task 32: Print the total number of employees in each state in each department
print(df.groupBy(["state","department"]).agg(count("*")).show());