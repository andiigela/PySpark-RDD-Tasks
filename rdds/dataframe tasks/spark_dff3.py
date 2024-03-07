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

# Task 33: Print the minimum and maximum salaries in each department and sort salaries in ascending order
print(df.groupBy(["department"]).agg(min("salary").alias("min_salary"),max("salary").alias("max_salary"))
      .sort(col("min_salary"),col("max_salary")).show());

# Task 34: Print the names of employees working in NY state under Finance department whose bonuses are greater
# than the average bonuses of employees in NY state.
value = df.groupBy([df.state]).agg(avg("bonus")).filter(col("state") == "NY").first();
#print(value[1])
print(df.filter((df.state == "NY") & (df.department == "Finance") & (df.bonus > value[1])).show())

# Task 35: Raise the salaries $500 of all employees whose age is greater than 45
def raise_salary(age,salary):
    if age > 45:
        return salary + 500;
    return salary;
raise_salary_udf = udf(lambda x,y: raise_salary(x,y),IntegerType())
df.withColumn("salary", raise_salary_udf(col("age"),col("salary"))).show()