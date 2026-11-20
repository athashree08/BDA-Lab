
# LAB 03 – APACHE SPARK RDD OPERATIONS

## 1. Open WSL Ubuntu

From Windows CMD:

```cmd
wsl
```

## 2. Navigate to Working Directory

```bash
cd /mnt/c/Users/Athashree
pwd
```

## 3. Verify Java and Python

```bash
java -version
python3 --version
pip --version
```

## 4. Create and Activate Python Virtual Environment

```bash
python3 -m venv ~/pyspark-env
source ~/pyspark-env/bin/activate
```

Verify:

```bash
which python
python --version
```

## 5. Install PySpark

```bash
pip install pyspark==4.2.0
```

Verify installation:

```bash
pip show pyspark
pyspark --version
```

## 6. Launch PySpark

```bash
pyspark
```

Verify SparkContext:

```python
sc
sc.master
```

## 7. Practical Task 1 – Marks Processing

```python
marks = [35, 42, 67, 81, 29, 74, 56, 91]

marks_rdd = sc.parallelize(marks)

print(marks_rdd.count())

passed = marks_rdd.filter(lambda x: x >= 50)
print(passed.collect())

grace_marks = marks_rdd.map(lambda x: x + 5)
print(grace_marks.collect())
```

## 8. Practical Task 2 – Salary Processing

```python
salaries = [25000, 45000, 50000, 30000, 75000]

salary_rdd = sc.parallelize(salaries)

total_salary = salary_rdd.reduce(lambda a, b: a + b)
print(total_salary)

high_salaries = salary_rdd.filter(lambda x: x > 40000)
print(high_salaries.collect())

updated_salaries = salary_rdd.map(lambda x: x * 1.10)
print(updated_salaries.collect())
```

## 9. Practical Task 3 – Word Count

```python
lines = [
    "big data is powerful",
    "spark processes big data",
    "big data analytics"
]

rdd = sc.parallelize(lines)

words = rdd.flatMap(lambda line: line.split())

counts = words.map(lambda word: (word, 1))

word_counts = counts.reduceByKey(lambda a, b: a + b)

print(word_counts.collect())
```

## 10. Practical Task 4 – Persistence

```python
even_numbers = sc.parallelize(range(1, 100))

even_numbers_rdd = even_numbers.filter(lambda x: x % 2 == 0)

even_numbers_rdd.cache()

print(even_numbers_rdd.count())

print(even_numbers_rdd.sum())
```

## 11. Partition Verification

```python
marks_rdd.getNumPartitions()

salary_rdd.getNumPartitions()

rdd.getNumPartitions()
```

Create an RDD with 4 partitions:

```python
marks_rdd = sc.parallelize(marks, 4)

marks_rdd.getNumPartitions()
```

## 12. Exit PySpark

```python
exit()
```

## 13. Run PySpark Again

Activate the virtual environment:

```bash
source ~/pyspark-env/bin/activate
```

Launch PySpark:

```bash
pyspark
```

## 14. Verify Environment Configuration

```bash
java -version
python --version
pip show pyspark
pyspark --version
which java
which python
which pyspark
```

## 15. Deactivate Virtual Environment

```bash
deactivate
```

