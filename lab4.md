
# LAB 04 – APACHE SPARK DATAFRAMES AND SPARK SQL

## 1. Open WSL Ubuntu

From Windows CMD:

```cmd
wsl
```

## 2. Activate PySpark Virtual Environment

```bash
source ~/pyspark-env/bin/activate
```

## 3. Verify Environment

```bash
java -version
python --version
pip show pyspark
pyspark --version
```

## 4. Launch PySpark

```bash
pyspark
```

## 5. Import Required Libraries

```python
from pyspark.sql import functions as F
```

## 6. Practical Task 1 – Employee Data

### Create Employee DataFrame

```python
data = [
    ("Amit", "IT", 50000, 2),
    ("Neha", "HR", 45000, 3),
    ("Ravi", "IT", 70000, 5),
    ("Pooja", "Sales", 55000, 4),
    ("Kiran", "IT", 60000, 3),
    ("Sneha", "HR", 48000, 2)
]

columns = ["Name", "Department", "Salary", "Experience"]

df = spark.createDataFrame(data, columns)
```

### Display Complete DataFrame

```python
df.show()
```

### Display Schema

```python
df.printSchema()
```

### Display Name and Department

```python
df.select("Name", "Department").show()
```

### Filter Employees with Salary Greater Than 50000

```python
df.filter(df.Salary > 50000).show()
```

### Filter Employees with Experience Greater Than 2 Years

```python
df.filter(df.Experience > 2).show()
```

### Sort Employees by Salary in Descending Order

```python
df.orderBy(df.Salary.desc()).show()
```

### Count Employees in Each Department

```python
df.groupBy("Department").count().show()
```

### Calculate Average Salary by Department

```python
df.groupBy("Department").avg("Salary").show()
```

### Find Maximum Salary in Each Department

```python
df.groupBy("Department").max("Salary").show()
```

## 7. Practical Task 2 – DataFrame Chaining

### Find Names and Salaries of IT Employees Earning More Than 55000

```python
df.filter(
    (df.Department == "IT") & (df.Salary > 55000)
).select("Name", "Salary").show()
```

## 8. Practical Task 3 – Spark SQL

### Create Temporary SQL View

```python
df.createOrReplaceTempView("employees")
```

### Display All Employees

```python
spark.sql("SELECT * FROM employees").show()
```

### Display Name and Salary

```python
spark.sql("SELECT Name, Salary FROM employees").show()
```

### Find Employees with Salary Greater Than 50000

```python
spark.sql(
    "SELECT * FROM employees WHERE Salary > 50000"
).show()
```

### Find Average Salary by Department

```python
spark.sql("""
SELECT Department, AVG(Salary) AS AverageSalary
FROM employees
GROUP BY Department
""").show()
```

### Find Maximum Salary by Department

```python
spark.sql("""
SELECT Department, MAX(Salary) AS MaximumSalary
FROM employees
GROUP BY Department
""").show()
```

### Display Employees in Descending Order of Salary

```python
spark.sql("""
SELECT * FROM employees
ORDER BY Salary DESC
""").show()
```

### Find IT Employees with Salary Greater Than 55000

```python
spark.sql("""
SELECT Name, Department, Salary
FROM employees
WHERE Department = 'IT' AND Salary > 55000
""").show()
```

## 9. Practical Task 4 – Joins

### Create Employee DataFrame

```python
employee_data = [
    (1, "Amit", 10),
    (2, "Neha", 20),
    (3, "Ravi", 10),
    (4, "Pooja", 30)
]

employee_columns = ["EmpID", "Name", "DeptID"]

employees = spark.createDataFrame(
    employee_data,
    employee_columns
)
```

### Create Department DataFrame

```python
department_data = [
    (10, "IT"),
    (20, "HR"),
    (30, "Sales")
]

department_columns = ["DeptID", "Department"]

departments = spark.createDataFrame(
    department_data,
    department_columns
)
```

### Perform Inner Join

```python
joined = employees.join(
    departments,
    employees.DeptID == departments.DeptID,
    "inner"
)

joined.show()
```

### Display Employee ID, Name and Department

```python
joined.select(
    employees.EmpID,
    employees.Name,
    departments.Department
).show()
```

### Perform Left Join

```python
left_joined = employees.join(
    departments,
    employees.DeptID == departments.DeptID,
    "left"
)

left_joined.show()
```

### Create Temporary SQL Views

```python
employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")
```

### Perform Inner Join Using Spark SQL

```python
spark.sql("""
SELECT e.EmpID, e.Name, d.Department
FROM employees e
JOIN departments d
ON e.DeptID = d.DeptID
""").show()
```

## 10. Practical Task 5 – Combined Analysis

### Filter Employees with Experience Greater Than or Equal to 2 Years

### Group by Department and Calculate Employee Count and Average Salary

```python
result = (
    df.filter(df.Experience >= 2)
    .groupBy("Department")
    .agg(
        F.count("Name").alias("EmployeeCount"),
        F.avg("Salary").alias("AverageSalary")
    )
)

result.show()
```

### Perform the Same Analysis Using Spark SQL

```python
df.createOrReplaceTempView("employees")

spark.sql("""
SELECT
    Department,
    COUNT(*) AS EmployeeCount,
    AVG(Salary) AS AverageSalary
FROM employees
WHERE Experience >= 2
GROUP BY Department
""").show()
```

## 11. Useful DataFrame Commands

```python
df.show()
df.show(10)
df.printSchema()
df.columns
df.count()
df.describe().show()
df.select("Name").show()
df.distinct().show()
df.orderBy("Salary").show()
df.orderBy(F.desc("Salary")).show()
df.dropDuplicates().show()
```

## 12. Stop Spark

Exit PySpark:

```python
exit()
```

Deactivate virtual environment:

```bash
deactivate
```
