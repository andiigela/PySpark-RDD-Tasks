from pyspark.sql import SparkSession
from pyspark.sql.functions import count,sum,avg,min, max,col

spark = SparkSession.builder.appName("Fifth Quiz").getOrCreate();
df = spark.read.csv(r"C:\Users\andig\PycharmProjects\PySpark-RDD-Tasks\rdds\input_files\OfficeDataProject.csv",header=True,inferSchema=True);

#Task 27: Print the total numbers of employees in the company
print(df.dropDuplicates(["employee_id"]).count()) # or df.count();

#Task 28: Print the total numbers of departments in the company
df.select("department").distinct().count() # or df.dropDuplicates(["department"]).count()
